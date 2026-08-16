"""新闻原始归档层：jsonl.gz 追加写入 / 当日去重 / 回放（架构 §5.4）。

归档是备份/重放保障，非查询路径。信封（envelope）为 dict，本层只要求含 `_id`
（幂等键），其余字段原样 JSON 序列化（ensure_ascii=False）落盘：
`{archive_base}/raw/{source}/YYYY/MM/DD.jsonl.gz`。

- **追加不改写**：每次 append 把整批信封压成一个完整 gzip member，二进制单次
  write 追加到日文件尾部（gzip 规范支持多 member 串接，`gzip.open(path, "rt")`
  读取时自动跨 member 拼接）——绝不"读全量→解压→重写"，单次 write 也把断写
  窗口压到最小；
- **append 不去重**：去重责任在调用方（job 启动时 `load_ids` 建内存集过滤）+
  重放侧按 `_id` 幂等（OpenSearch create-only，见 opensearch_utils）；
- **spool 降级**（防归档反成采集单点，快讯窗口不可回补）：NAS 写失败 → 同相对
  路径落本地 spool 目录 + 钉钉告警（spool 根下 `.nas_alert_ts` 标记文件节流，
  TTL 内不重发——任务每轮跑在新子进程，模块级标志会复位，须落盘才跨进程有效），
  入库照常；每轮 job 开头 `flush_spool()` 按 `_id` 幂等搬回 NAS，全部搬回（spool
  清空）后删标记并发一条恢复通知；NAS 与 spool 双双失败才抛。

残余风险（断写）：追加为"单次 write 一个完整 member"，仍非原子——极端断写会留下
截断的尾部 member，当日文件读取（load_ids/replay）将抛 EOFError/BadGzipFile；
次日换新文件自愈；人工修复可按 gzip magic（0x1f 0x8b）重切文件，spool 数据可兜底。

并发说明：pipeline 内任务串行执行（子进程 max_workers=1）；若多个 pipeline 恰好
并发 flush_spool，最坏产生可被幂等吸收的 NAS 重复行（重放侧按 `_id` create-only
吸收），后到者 unlink 抛 FileNotFoundError 记 warning——均属良性。
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Set, Tuple
from urllib.parse import quote

from data_collect.config import get_news_config
from data_collect.utils.notify import send_dingtalk

logger = logging.getLogger(__name__)

# 项目根：data_collect 包上一级（.../data_collect/utils/news_archive.py → parents[2]）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# NAS 降级告警节流：spool 根下标记文件，mtime 距今小于 TTL 则不重发钉钉。
# 标记必须落盘：任务每轮在新子进程（Windows spawn）中重新 import 本模块，
# 模块级标志每轮复位——NAS 宕一天将随 */15 调度刷 ~96 条钉钉。
_ALERT_MARKER_NAME = ".nas_alert_ts"
_ALERT_TTL_SECONDS = 4 * 3600


class ArchiveStorageType(str, Enum):
    NAS = "nas"
    SPOOL = "spool"


_ARCHIVE_RECEIPT_PROOF = object()


@dataclass(frozen=True, slots=True)
class ArchiveReceipt:
    """Evidence returned only after a concrete archive append succeeds."""

    news_id: str
    storage_type: ArchiveStorageType
    archive_uri: str
    archived_at: datetime
    success: bool = True
    status: str = "archived"
    _proof: object = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._proof is not _ARCHIVE_RECEIPT_PROOF:
            raise ValueError("ArchiveReceipt 只能由真实归档写入结果创建")

    @property
    def is_verified(self) -> bool:
        return self._proof is _ARCHIVE_RECEIPT_PROOF


def _is_posix() -> bool:
    """当前是否 POSIX 系统（独立函数便于单测打桩，勿直接 patch os.name——那会
    污染全局 os 使 pathlib 选错 Path 实现）。"""
    return os.name == "posix"


def _assert_not_windows_drive(raw: str) -> None:
    """POSIX 上拒绝 Windows 盘符路径（如 ``Z:\\...``）——**响亮失败**。

    背景（2026-07-08 部署审查）：Linux 上 ``Path("Z:\\A股冷数据\\news")`` 是**单层
    相对**路径（反斜杠是合法文件名字符、``Z:`` 不是盘符），mkdir 会静默成功，把
    归档写进 CWD 下的垃圾目录——而归档是快讯/政策/监管的**唯一存档**（滚动窗源头
    不可回补），静默走错路 = 数据看着采到了、实则备份全丢在错地方。
    """
    if re.match(r"^[A-Za-z]:[\\/]", raw):
        raise RuntimeError(
            f"news 归档根 {raw!r} 是 Windows 盘符路径，但当前是 Linux/Unix——"
            f"归档会静默写进 CWD 垃圾目录（唯一存档丢失）。请配置 "
            f"news.archive_base_posix 为 POSIX 挂载点（如 /mnt/media/A股冷数据/news）")


def _archive_root() -> Path:
    """NAS 归档根目录（其下 raw/ 子目录）。**平台感知**：POSIX 用
    `news.archive_base_posix`（Linux 挂载点），Windows 用 `news.archive_base`
    （盘符路径）——一份 config 两台机器通用，无需按机器改路径（2026-07-09）。
    """
    cfg = get_news_config()
    if _is_posix():
        raw = cfg.get("archive_base_posix")
        if not raw:
            raise RuntimeError(
                "当前是 Linux/Unix，但 news.archive_base_posix 未配置——请填 SMB/NAS "
                "的 POSIX 挂载点（如 /mnt/media/A股冷数据/news），见部署文档")
        _assert_not_windows_drive(str(raw))
        return Path(raw)
    return Path(cfg["archive_base"])


def _spool_root() -> Path:
    """本地 spool 根目录：config `news.spool_dir`，缺省 <项目根>/news_spool。"""
    spool_dir = get_news_config().get("spool_dir")
    return Path(spool_dir) if spool_dir else _PROJECT_ROOT / "news_spool"


def _rel_path(source: str, date: str) -> Path:
    """相对路径 raw/{source}/YYYY/MM/DD.jsonl.gz（NAS 与 spool 共用同一结构）。"""
    if not source or source in {".", ".."} or "/" in source or "\\" in source:
        raise ValueError(f"非法 source（应为单层目录名）: {source!r}")
    # isascii 拦截全角数字（"２０２６"等 isdigit 为 True，但会生成不可解析路径）
    if len(date) != 8 or not date.isascii() or not date.isdigit():
        raise ValueError(f"非法日期（应为 YYYYMMDD）: {date!r}")
    return Path("raw") / source / date[:4] / date[4:6] / f"{date[6:8]}.jsonl.gz"


def archive_path(source: str, date: str) -> Path:
    """归档文件绝对路径：{archive_base}/raw/{source}/YYYY/MM/DD.jsonl.gz。"""
    return _archive_root() / _rel_path(source, date)


def _encode_member(envelopes: List[Dict[str, Any]]) -> bytes:
    """把整批信封序列化为 JSONL 并压成一个完整 gzip member（含头尾）。"""
    payload = "".join(json.dumps(env, ensure_ascii=False) + "\n" for env in envelopes)
    return gzip.compress(payload.encode("utf-8"))


def _append_member(path: Path, blob: bytes) -> None:
    """mkdir -p 后把完整 gzip member 以二进制追加单次 write 到文件尾。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "ab") as f:
        f.write(blob)


def _archive_uri(path: Path, news_id: Any) -> str:
    """Build a URI from the path that was actually written, never configuration."""

    return f"{path.resolve().as_uri()}#news_id={quote(str(news_id), safe='')}"


def _receipts_for_written_path(
    envelopes: List[Dict[str, Any]], path: Path, storage_type: ArchiveStorageType
) -> Tuple[ArchiveReceipt, ...]:
    archived_at = datetime.now(timezone.utc)
    return tuple(
        ArchiveReceipt(
            news_id=env["_id"],
            storage_type=storage_type,
            archive_uri=_archive_uri(path, env["_id"]),
            archived_at=archived_at,
            _proof=_ARCHIVE_RECEIPT_PROOF,
        )
        for env in envelopes
    )


def _iter_envelopes(path: Path) -> Iterator[Dict[str, Any]]:
    """逐行读取一个 jsonl.gz（自动跨 gzip member 拼接），yield 信封 dict。"""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _alert_marker() -> Path:
    """告警节流标记文件路径（放 spool 根，不与 raw/ 数据混放）。"""
    return _spool_root() / _ALERT_MARKER_NAME


def _warn_nas_degraded(nas_path: Path, spool_path: Path, exc: Exception) -> None:
    """NAS 降级告警：标记文件 mtime 距今 < TTL 只记日志不发钉钉（跨进程节流）；
    钉钉/标记写失败均不影响归档流程。"""
    logger.warning(f"NAS 归档写失败（{nas_path}），已降级本地 spool（{spool_path}）: {exc}")
    marker = _alert_marker()
    try:
        if marker.exists() and time.time() - marker.stat().st_mtime < _ALERT_TTL_SECONDS:
            return  # TTL 内已告警过，不重发
    except OSError:
        pass  # 标记不可读按未告警处理
    try:
        send_dingtalk(
            f"新闻归档 NAS 写失败，已降级写本地 spool（{_ALERT_TTL_SECONDS // 3600} 小时内"
            f"不重发提醒，NAS 恢复后 flush_spool 自动搬回）：{exc}"
        )
    except Exception:
        logger.warning("spool 降级钉钉告警发送失败（不影响归档流程）", exc_info=True)
    try:
        # 此刻 spool 根必已存在（spool 写入成功后才会走到这里）
        marker.write_text(datetime.now().isoformat(), encoding="utf-8")
    except OSError:
        logger.warning(f"告警节流标记写入失败: {marker}", exc_info=True)


def append_with_receipt(
    source: str, date: str, envelopes: Iterable[Dict[str, Any]]
) -> Tuple[ArchiveReceipt, ...]:
    """Append envelopes and return evidence for the location actually written.

    Receipts are created only after the concrete NAS or spool append succeeds.
    Complete failure raises and returns no theoretical path or fabricated URI.
    The archived envelope bytes are produced before receipt construction and are
    never enriched with receipt or compatibility fields.
    """

    envelopes = list(envelopes)
    if not envelopes:
        return ()
    for env in envelopes:
        if not isinstance(env, dict) or "_id" not in env:
            desc = f"keys={sorted(env)}" if isinstance(env, dict) else repr(env)[:200]
            raise ValueError(f"信封缺少 _id（幂等键，见架构 §5.2）: {desc}")

    rel = _rel_path(source, date)
    blob = _encode_member(envelopes)
    nas_path = _archive_root() / rel
    try:
        _append_member(nas_path, blob)
        return _receipts_for_written_path(envelopes, nas_path, ArchiveStorageType.NAS)
    except OSError as nas_exc:
        spool_path = _spool_root() / rel
        try:
            _append_member(spool_path, blob)
        except OSError as spool_exc:
            raise RuntimeError(
                f"归档 NAS 与 spool 双双失败: NAS={nas_exc}; spool={spool_exc}"
            ) from spool_exc
        _warn_nas_degraded(nas_path, spool_path, nas_exc)
        return _receipts_for_written_path(
            envelopes, spool_path, ArchiveStorageType.SPOOL
        )


def append(source: str, date: str, envelopes: Iterable[Dict[str, Any]]) -> int:
    """追加写归档：NAS 优先，OSError 降级本地 spool，双双失败才抛 RuntimeError。

    每次调用写一个独立 gzip member；不去重（重复 `_id` 原样落盘，调用方应先用
    load_ids 过滤）。返回新写条数（落 spool 时返回值照常）。

    这是保留给现有 job 的兼容接口；需要真实位置证据的新调用方应使用
    :func:`append_with_receipt`。
    """
    return len(append_with_receipt(source, date, envelopes))


def load_ids(source: str, date: str) -> Set[str]:
    """读 NAS 当日归档回全部 `_id` 集合（当日内存去重用；文件不存在返回空集）。

    只读 NAS 归档，不含 spool 积压——job 开头先 flush_spool 即自然并入。
    异常契约：断写/损坏文件会抛 EOFError（断在数据区）或 BadGzipFile/OSError
    （断在头部）；调用方（job）应自行决定降级策略（如放弃当日去重继续采集）。
    """
    path = archive_path(source, date)
    if not path.exists():
        return set()
    return {env["_id"] for env in _iter_envelopes(path)}


def replay(source: str, date_range: Tuple[str, str]) -> Iterator[Dict[str, Any]]:
    """按自然日逐日读取归档，逐条 yield 信封（重建/回放用）。

    date_range=(start, end) 均 YYYYMMDD、含两端；不存在的日期文件静默跳过。
    异常契约：断写/损坏文件会抛 EOFError（断在数据区）或 BadGzipFile/OSError
    （断在头部）；调用方应自行决定跳过该日或人工修复（模块 docstring）后重放。
    """
    start, end = date_range
    cur = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    while cur <= end_dt:
        path = archive_path(source, cur.strftime("%Y%m%d"))
        if path.exists():
            yield from _iter_envelopes(path)
        cur += timedelta(days=1)


def _cleanup_empty_dirs(start: Path, stop: Path) -> None:
    """自 start 逐级向上删除空目录，止于 stop（不含）；遇非空即停。"""
    cur = start
    while cur != stop and stop in cur.parents:
        try:
            cur.rmdir()
        except OSError:
            return
        cur = cur.parent


def _clear_alert_if_recovered(spool_raw: Path, moved: int) -> None:
    """spool 已无积压且存在降级标记 → 删标记 + 发一次恢复通知（失败仅记日志）。"""
    marker = _alert_marker()
    if not marker.exists():
        return
    if spool_raw.is_dir() and any(spool_raw.rglob("*.jsonl.gz")):
        return  # 仍有积压（NAS 未恢复或存在损坏文件），保持降级状态
    try:
        marker.unlink()
    except OSError:
        logger.warning(f"告警节流标记删除失败: {marker}", exc_info=True)
        return  # 标记还在，下轮再试（避免重复恢复通知）
    try:
        send_dingtalk(f"新闻归档 NAS 已恢复，spool 积压已搬回 {moved} 条")
    except Exception:
        logger.warning("NAS 恢复通知发送失败（不影响归档流程）", exc_info=True)


def flush_spool() -> int:
    """把 spool 积压归档按 `_id` 幂等搬回 NAS（每轮采集 job 开头调用）。

    - 逐文件：读出信封 → 与 NAS 当日已有 `_id` 比对 → 只追加缺失的（文件内
      重复 `_id` 也只搬一条）；
    - 整文件搬回成功才删 spool 文件（并尽力清理空目录）；
    - NAS 仍不可用/单文件异常（含损坏 gzip）：保留该文件、记日志、继续其余文件，
      不抛（下轮再试；损坏文件需人工介入，见模块 docstring 修复方式）；
    - 全部搬回（spool 清空）且处于降级状态：删 `.nas_alert_ts` 标记并发恢复通知；
    - 跨 pipeline 并发 flush：最坏产生可被幂等吸收的 NAS 重复行，后到者 unlink
      抛 FileNotFoundError 记 warning、该文件计数不加——属良性；
    - 返回实际搬回 NAS 的条数。
    """
    spool_raw = _spool_root() / "raw"
    moved = 0
    if spool_raw.is_dir():
        for spool_file in sorted(spool_raw.rglob("*.jsonl.gz")):
            parts = spool_file.relative_to(spool_raw).parts  # (source, YYYY, MM, DD.jsonl.gz)
            if len(parts) != 4:
                logger.warning(f"spool 目录结构异常，跳过: {spool_file}")
                continue
            source, date = parts[0], parts[1] + parts[2] + parts[3].split(".", 1)[0]
            try:
                seen = load_ids(source, date)
                missing = []
                for env in _iter_envelopes(spool_file):
                    if env["_id"] in seen:
                        continue
                    seen.add(env["_id"])
                    missing.append(env)
                if missing:
                    _append_member(archive_path(source, date), _encode_member(missing))
                spool_file.unlink()
                moved += len(missing)
                logger.info(f"spool 搬回 NAS: {source}/{date} {len(missing)} 条")
            except Exception as exc:
                logger.warning(f"spool 搬回失败（保留待下轮）: {spool_file}: {exc}")
                continue
            _cleanup_empty_dirs(spool_file.parent, stop=spool_raw)
    _clear_alert_if_recovered(spool_raw, moved)
    return moved
