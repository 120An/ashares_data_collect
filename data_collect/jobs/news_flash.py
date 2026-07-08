"""7×24 财经快讯双活采集任务（新闻系统架构 §4.2）。

数据源**双活**（akshare）：``stock_info_global_cls``（财联社电报，滚动窗约数百条）
+ ``stock_info_global_em``（东财全球财经快讯，滚动窗约 200 条）。两源均为**滚动
窗口接口——无日期参数、错过无法从源回补**，归档层对快讯就是唯一存档（架构 §4.4），
因此本 job 的健壮性机制多于同族 news_cctv：

1. **源级隔离**：单源采集异常只计失败源继续（双活的意义），双源全挂才 raise
   触发框架 retry；
2. **溢出检测**（每源独立）：本轮窗口整体全部为新 `_id` 且已归档集合非空 →
   上轮与本轮窗口零重叠，跳轮/爆量期间中间的快讯已**永久丢失** → 钉钉告警
   （对策：加密调度频率）。冷启动（涉及各日归档全空）必然整窗全新，豁免不告警；
3. **断流持续监测**（spec §6"双活之一连续 N 轮 0 条"）：本地状态文件跨进程累计
   各源连续 0 条**窗口数据**轮数（判定用窗口条数而非归档新增——健康源持续返回同一
   滚动窗会归档 0 新，据此判定会误报断流；任务每轮跑在新子进程，模块级状态每轮
   复位，须落盘），达 `_SILENCE_ROUNDS` 告警一次、恢复后发恢复通知并复位；
4. **verify = 归档重放对账**：快讯源头无法回补，`run_verify` 不触源，只把
   `[today-N, today]` 自然日窗口的归档重放入库（create-only 幂等），兜集群宕机
   期间"已归档未入库"的缺口。

信封形状/raw_* 剥除/`_id` 决定论/降级模式全部沿用 news_cctv 的统一契约。
"""

from __future__ import annotations

import datetime
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from data_collect.config import get_news_config
from data_collect.utils import news_archive
from data_collect.utils import news_normalize as nn
from data_collect.utils import notify
from data_collect.utils import opensearch_utils as osu
from data_collect.utils.news_common import cell as _cell
from data_collect.utils.news_common import flush_spool_safe as _flush_spool_safe
from data_collect.utils.news_common import fmt_truncated as _fmt_items
from data_collect.utils.news_common import strip_raw as _strip_raw
from data_collect.utils.news_common import today as _today
from data_collect.utils.news_common import verify_dates as _verify_dates
from data_collect.utils.notify import send_dingtalk

logger = logging.getLogger(__name__)

# 项目根：data_collect 包上一级（.../data_collect/jobs/news_flash.py → parents[2]）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 双活源（也是归档目录名与信封 source 值）；顺序即采集/摘要顺序
_SOURCES = ("cls", "em")

# 断流判定：某源连续 N 轮 0 条新数据 → 告警（*/15 调度下 8 轮 ≈ 2 小时）
_SILENCE_ROUNDS = 8

# 断流监测状态文件名（落 spool 根目录，与 news_archive 降级 spool 同址）
_STATE_FILE_NAME = ".flash_source_state.json"

# 剥离键契约（raw_title/raw_content/time_estimated）已收敛至 news_common.RAW_ONLY_KEYS

# 每源必需列（源改版丢列时显式报错计失败源，见 _collect_source）。
# 列映射集中于此与 _row_to_envelope 的按源分派——将来源改版只改这一处：
# - cls: 标题/内容/发布日期(datetime.date 对象)/发布时间(datetime.time 对象)，无 url；
# - em:  标题/摘要/发布时间(字符串)/链接（content 取摘要，url 取链接）。
_REQUIRED_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "cls": ("标题", "内容", "发布日期", "发布时间"),
    "em": ("标题", "摘要", "发布时间", "链接"),
}


def _fetch_cls() -> pd.DataFrame:
    """拉取财联社电报滚动窗（symbol=全部）。

    lazy import：akshare 导入慢且依赖重；测试 monkeypatch 本函数即可隔离网络。
    """
    import akshare as ak

    return ak.stock_info_global_cls(symbol="全部")


def _fetch_em() -> pd.DataFrame:
    """拉取东财全球财经快讯滚动窗（约 200 条）。lazy import 同上。"""
    import akshare as ak

    return ak.stock_info_global_em()


def _alert(message: str) -> bool:
    """guarded 钉钉：发送失败仅记日志，绝不阻塞采集主流程；返回是否发送成功。

    委托 notify.guarded_send，注入本模块级 send_dingtalk（保留其 monkeypatch 点）；
    bool 返回被断流状态机用于判定是否置 alerted，务必保留。
    """
    return notify.guarded_send(message, send=send_dingtalk)


def _load_name_dict() -> Dict[str, str]:
    """加载股票简称词典；DB 不可用降级空词典 + 钉钉告警（不阻塞采集，同 news_cctv）。

    告警必须发：create-only 文档不可变，本轮入库文档的简称打标为空即**永久缺失**。
    """
    try:
        return nn.load_name_dict()
    except Exception as exc:
        logger.warning(f"简称词典加载失败，本轮股票打标降级为仅正则代码: {exc!r}")
        _alert(
            f"快讯简称词典加载失败（{exc!r}），本轮 stocks 打标降级为仅正则代码；"
            f"create-only 下本轮文档简称标注永久缺失，请尽快排查 DB"
        )
        return {}


def _load_archived_ids(source: str, date8: str) -> set:
    """读某源某日归档 `_id` 集（append 去重用）；断写损坏降级空集不阻塞采集。

    降级语义同 news_cctv：create-only 入库使去重缺失无害，归档最多多出可被 `_id`
    幂等吸收的重复行——采集连续性优先（快讯窗口不可回补，停摆代价更高）。
    """
    try:
        return news_archive.load_ids(source, date8)
    except (EOFError, OSError) as exc:
        logger.warning(
            f"快讯 {source}/{date8} 归档读取失败，放弃该日归档去重继续采集: {exc!r}")
        _alert(
            f"快讯 {source}/{date8} 归档文件读取失败（疑断写损坏：{exc!r}），"
            f"已降级继续采集；修复方式见 news_archive 模块说明"
        )
        return set()


def _cls_pub_time(row) -> str | None:
    """组合 cls 的 发布日期(date 对象) + 发布时间(time 对象) 为 "YYYY-MM-DD HH:MM:SS"。

    akshare 该接口返回原生 date/time 对象，必须传原生对象 datetime.combine——
    str() 整列拼串会把 NaT/缺失变成 "NaT"/"nan" 脏串。对象异形（源改版成字符串等）
    回退 normalize_time 尽力解析；仍失败返回 None（调用方回退 fetch_time）。
    """
    d, t = row.get("发布日期"), row.get("发布时间")
    if isinstance(d, datetime.date) and isinstance(t, datetime.time):
        if pd.isna(d):  # pd.NaT 是 date 子类但不可 combine/strftime，先拦
            return None
        return datetime.datetime.combine(d, t).strftime("%Y-%m-%d %H:%M:%S")
    combined = f"{_cell(d)} {_cell(t)}".strip()
    return nn.normalize_time(combined, source="cls") if combined else None


def _row_to_envelope(source: str, row, fetch_time: str,
                     name_dict: Dict[str, str]) -> dict:
    """单行 → 统一信封（架构 §5.2；列映射按源分派，见 _REQUIRED_COLUMNS 注释）。

    - `_id` 用**原始** title/content（clean_text 之前）计算：em 含 url 走二级
      sha1(url)；cls 无 url/native_id 走三级 sha1(source|pub_time|title)，title 空
      （电报常见）时 make_id 以 content[:128] 替位——清洗规则日后调整不产生新
      `_id`（同 news_cctv 决定论约定）；
    - pub_time 解析失败：`_id` 以 None→空串参与计算保持决定论（若用回退的
      fetch_time 参与，同一条目每轮得到新 `_id`，滚动窗残留期内反复重复入库）；
      信封 pub_time 回退 fetch_time 并标 `time_estimated: True`（只进归档，入库剥除）；
    - `raw_title`/`raw_content` 保留源原文只进归档，`title`/`content` 存 clean 后
      版本（重放可直接入库）——各 news job 统一契约。
    """
    if source == "cls":
        raw_title = _cell(row.get("标题")).strip()
        raw_content = _cell(row.get("内容")).strip()
        pub_time = _cls_pub_time(row)
        url = ""
    else:  # em
        raw_title = _cell(row.get("标题")).strip()
        raw_content = _cell(row.get("摘要")).strip()
        pub_time = nn.normalize_time(row.get("发布时间"), source=source)
        url = _cell(row.get("链接")).strip()

    title = nn.clean_text(raw_title)
    envelope = {
        "_id": nn.make_id({"source": source, "url": url, "pub_time": pub_time,
                           "title": raw_title, "content": raw_content}),
        "title": title,
        "content": nn.clean_text(raw_content),
        "raw_title": raw_title,
        "raw_content": raw_content,
        "pub_time": pub_time or fetch_time,
        "fetch_time": fetch_time,
        "source": source,
        "channel": "flash",
        "stocks": nn.tag_stocks(title, name_dict),
        "vec_status": "pending",
    }
    if url:
        envelope["url"] = url          # 仅 em；cls 无 url 不写空键（同联播惯例）
    if pub_time is None:
        envelope["time_estimated"] = True
    return envelope


def _collect_source(source: str, fetch_time: str,
                    name_dict: Dict[str, str]) -> List[dict]:
    """拉取单源滚动窗并信封化（批内按 `_id` 去重保首见）；异常由调用方按源隔离。

    缺列防御：源改版丢列时显式抛错计失败源——`.get` 逐行取会静默产出空信封，
    全窗坍缩到同一个 `_id`，比失败更糟。窗口条数记日志（回填架构 §4.2 实测值）。
    """
    df = _fetch_cls() if source == "cls" else _fetch_em()
    if df is None or len(df) == 0:
        logger.info(f"快讯源 {source} 本轮窗口 0 条")
        return []
    missing = [c for c in _REQUIRED_COLUMNS[source] if c not in df.columns]
    if missing:
        raise ValueError(
            f"快讯源 {source} 返回缺列 {missing}（疑源改版），实际列: {list(df.columns)}")
    logger.info(f"快讯源 {source} 本轮窗口 {len(df)} 条")

    envelopes: List[dict] = []
    seen: set = set()
    for _, row in df.iterrows():
        env = _row_to_envelope(source, row, fetch_time, name_dict)
        if env["_id"] in seen:
            continue                   # 滚动窗内偶发重复行，保首见
        seen.add(env["_id"])
        envelopes.append(env)
    return envelopes


def _archive_source(source: str, envelopes: List[dict]) -> Tuple[int, bool]:
    """按 pub_time 日期分组归档新信封（滚动窗可跨午夜），返回 (新增条数, 是否疑似溢出)。

    溢出检测：本轮窗口非空且全部 `_id` 均未见于涉及各日已归档集合——说明上轮与
    本轮窗口零重叠，跳轮/爆量期间中间的快讯已永久丢失（滚动窗不可回补）→ 告警。
    冷启动豁免：涉及各日归档全空（首次运行/新源）必然整窗全新，仅记日志。
    """
    if not envelopes:
        return 0, False

    by_day: Dict[str, List[dict]] = {}
    for env in envelopes:
        day8 = env["pub_time"][:10].replace("-", "")
        by_day.setdefault(day8, []).append(env)

    archived_seen: set = set()
    new_count = 0
    for day8 in sorted(by_day):
        seen = _load_archived_ids(source, day8)
        archived_seen |= seen
        fresh = [env for env in by_day[day8] if env["_id"] not in seen]
        if fresh:
            news_archive.append(source, day8, fresh)
            new_count += len(fresh)

    overflowed = False
    if new_count == len(envelopes):
        if archived_seen:
            overflowed = True
            _alert(
                f"快讯源 {source} 疑似窗口溢出：本轮 {len(envelopes)} 条与已归档零重叠"
                f"（整窗全新），跳轮/爆量期间中间快讯可能已永久丢失；"
                f"如频发请加密采集频率（*/15 → */10）"
            )
        else:
            logger.info(f"快讯源 {source} 整窗全新但历史归档为空（冷启动），不判溢出")
    return new_count, overflowed


# ---------- 断流持续监测（状态文件跨进程持久） ----------

def _state_file() -> Path:
    """断流监测状态文件路径：spool 根目录下（config `news.spool_dir`，
    缺省 <项目根>/news_spool——与 news_archive 的降级 spool 同址）。"""
    spool_dir = get_news_config().get("spool_dir")
    root = Path(spool_dir) if spool_dir else _PROJECT_ROOT / "news_spool"
    return root / _STATE_FILE_NAME


def _read_state() -> Dict[str, dict]:
    """读状态文件；缺失/损坏一律重置为空（监测状态可再生，不值得阻塞采集）。"""
    path = _state_file()
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
            logger.warning(f"断流状态文件形状异常，重置: {path}")
    except (OSError, ValueError) as exc:   # JSONDecodeError 是 ValueError 子类
        logger.warning(f"断流状态文件读取失败，重置: {path}: {exc!r}")
    return {}


def _write_state(state: Dict[str, dict]) -> None:
    """写状态文件；失败仅记日志（本轮状态丢失，下轮从头累计，无害）。"""
    path = _state_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    except OSError:
        logger.warning(f"断流状态文件写入失败（本轮状态丢失）: {path}", exc_info=True)


def _update_silence_state(window_counts: Dict[str, int],
                          failed_sources: List[str]) -> None:
    """单源断流持续监测（spec §6 监控清单"双活之一连续 N 轮 0 条"）。

    **断流判定用窗口条数（本轮该源返回＆信封化的条数，归档去重之前），而非归档新增
    条数**：健康源持续返回同一滚动窗（一段时间无新快讯）会归档 0 新，若据此判定会
    误报断流；bulk 重放/窗口重叠导致的去重同样把归档新增打成 0。只要源返回 >0 条
    窗口条目即视为存活（无论去重后是否新增）。溢出检测另用"归档新增 vs 窗口"比较，
    不受此处影响（见 _archive_source）。

    规则：某源本轮窗口 0 条或采集异常 → consecutive_zero+1；达 _SILENCE_ROUNDS
    且未告警 → 告警一次并置 alerted（**发送成功才置位**，失败下轮重试——断流告警
    不容丢）；恢复（窗口有条目）→ 已告警则发恢复通知（丢失仅属美观问题），
    无条件复位。状态须落盘：任务每轮跑在新子进程，模块级标志每轮复位。
    """
    state = _read_state()
    for source in _SOURCES:
        entry = state.get(source)
        if not isinstance(entry, dict):
            entry = {}   # 单源条目形状异常同样重置（"损坏即重置"契约的条目级兜底）
        # 失败源窗口条数计 0（不会出现在 window_counts）→ 断流候选
        zero_round = source in failed_sources or window_counts.get(source, 0) == 0
        if zero_round:
            entry["consecutive_zero"] = int(entry.get("consecutive_zero", 0)) + 1
            rounds = entry["consecutive_zero"]
            if rounds >= _SILENCE_ROUNDS and not entry.get("alerted"):
                if _alert(
                    f"快讯源 {source} 已连续 {rounds} 轮无新数据，疑似断流"
                    f"（*/15 调度下约 {rounds * 15 / 60:.1f} 小时）；"
                    f"另一源双活仍在采集，请排查该源接口"
                ):
                    entry["alerted"] = True
        else:
            if entry.get("alerted"):
                _alert(f"快讯源 {source} 断流恢复（本轮窗口 {window_counts.get(source, 0)} 条）")
            entry["consecutive_zero"] = 0
            entry["alerted"] = False
        state[source] = entry
    _write_state(state)


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每轮任务：双活拉取滚动窗 → 信封化 → 跨日归档去重 → create-only 合并入库。

    `run_date` 仅为框架签名兼容——滚动窗接口无日期参数，永远采"当前窗口"；
    归档/入库的日期归属由每条的 pub_time 决定（窗口可跨午夜）。
    单源异常隔离（计失败源继续），双源全挂才 raise 触发框架 retry。
    """
    _flush_spool_safe()
    name_dict = _load_name_dict()
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 逐源采集+信封化（源级隔离：单源异常不抛，记失败源继续）
    envelopes_by_source: Dict[str, List[dict]] = {}
    failed_sources: List[str] = []
    errors: List[str] = []
    for source in _SOURCES:
        try:
            envelopes_by_source[source] = _collect_source(source, fetch_time, name_dict)
        except Exception as exc:
            failed_sources.append(source)
            errors.append(f"{source}: {exc!r}")
            logger.warning(f"快讯源 {source} 采集失败（源级隔离，继续其余源）: {exc!r}")
    if len(failed_sources) == len(_SOURCES):
        raise RuntimeError(f"快讯双源全部采集失败: {'; '.join(errors)}")

    # 跨日分组归档 + 溢出检测（每源独立）
    new_counts: Dict[str, int] = {}
    overflow_sources: List[str] = []
    for source, envelopes in envelopes_by_source.items():
        new_counts[source], overflowed = _archive_source(source, envelopes)
        if overflowed:
            overflow_sources.append(source)

    # 入库写全量（与归档去重无关，幂等由 create-only 409→dup 保证），剥 raw_* 键
    all_envelopes = [env for source in _SOURCES
                     for env in envelopes_by_source.get(source, [])]
    ok = dup = 0
    if all_envelopes:
        ok, dup = osu.bulk_create(osu.get_client(),
                                  [_strip_raw(env) for env in all_envelopes])

    # 断流持续监测（放 bulk 之后：入库失败本轮 raise 走框架 retry，不重复计轮数）。
    # 用窗口条数（本轮各源返回条数，归档去重之前）判定断流：健康源持续返回同一
    # 滚动窗会归档 0 新，若据归档新增判定会误报断流（详见 _update_silence_state）。
    # 失败源不在 envelopes_by_source 中，.get 得 0（且状态机另按 failed_sources 兜底）。
    window_counts = {source: len(envs) for source, envs in envelopes_by_source.items()}
    _update_silence_state(window_counts, failed_sources)

    parts = []
    for source in _SOURCES:
        if source in failed_sources:
            parts.append(f"{source} 失败")
        else:
            parts.append(
                f"{source} {len(envelopes_by_source[source])}条(新{new_counts[source]})")
    summary = f"快讯: {' '.join(parts)}, 入库新增 {ok} 重复 {dup}"
    if failed_sources:
        summary += f", 失败源[{','.join(failed_sources)}]"
    if overflow_sources:
        summary += f", 溢出[{','.join(overflow_sources)}]"
    return summary


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    """查漏补缺：**归档重放对账**（快讯滚动窗无源头回补，verify 不触源）。

    重要——verify 自然日语义，**忽略框架传入的 start_date/end_date**：框架
    `_resolve_invocation` 按交易日推算窗口（为行情任务设计），快讯 7×24 滚动，
    交易日窗口会系统性漏掉周末（同 news_cctv 约定）。实际窗口改读 config
    `news.verify_days_back`（默认 3）按自然日回溯 `[today-N, today]`——**含今天**
    （当天归档可能含集群宕机期间已归档未入库的条目，与联播"当天归 run 管"不同）。

    逐 (source, day) 重放归档 → 剥 raw_*/time_estimated → create-only 入库（已入库
    走 409 计 dup，天然幂等）；单 (source, day) 异常隔离计失败继续，failed>0 末尾
    raise RuntimeError(摘要) 保留框架失败通知语义。不做溢出/断流判定（那是 run 的
    实时监测职责，重放旧窗口无此语义）。
    """
    dates = _verify_dates(get_news_config().get("verify_days_back", 3),
                          _today(), include_today=True)

    client = osu.get_client()
    replayed = ok_total = dup_total = 0
    failed: List[str] = []   # "source/day"
    for source in _SOURCES:
        for day in dates:
            try:
                envelopes = list(news_archive.replay(source, (day, day)))
                if not envelopes:
                    continue
                ok, dup = osu.bulk_create(
                    client, [_strip_raw(env) for env in envelopes])
                replayed += len(envelopes)
                ok_total += ok
                dup_total += dup
                logger.info(f"快讯verify 重放 {source}/{day}: {len(envelopes)} 条, "
                            f"新入库 {ok} 重复 {dup}")
            except Exception as exc:
                failed.append(f"{source}/{day}")
                logger.warning(f"快讯verify 重放 {source}/{day} 失败（继续其余）: {exc!r}")

    summary = (f"快讯verify [{dates[0]}~{dates[-1]}]: 重放 {replayed} 条, "
               f"新入库 {ok_total}, 重复 {dup_total}, 失败 {len(failed)}")
    if failed:
        summary += f" ({_fmt_items(failed)})"
        raise RuntimeError(summary)   # 窗口已处理完，仍上抛保留框架失败通知语义
    return summary
