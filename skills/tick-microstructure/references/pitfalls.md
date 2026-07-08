# 坑位参考

使用 A 股 L1 快照 tick 时容易踩的 9 个坑，含原因、规避方法和本包的处理方式。

---

## 1. 量额笔数是累计值，必须先 diff

**为什么：** QMT tick 中 `volume / amount / transaction_num` 是当日**累计值**（从开盘起始），
不是每个 3 秒切片的增量。直接用累计值做资金流/方向分类会得到全天汇总，而非逐切片值。

**怎么避：** 调用分析函数前，先用 `to_increments(df)` 对这三列取 `.diff()` 得到增量
`dVol / dAmt / dCnt`，再过滤 `dAmt > 0` 取有效切片。

**本包处理：** `to_increments()` 在所有涉及增量的函数（`money_flow / avg_trade_amount / vwap / amihud_illiquidity` 等）内部自动调用。首行 diff 结果为 NaN，被 `dAmt>0` 过滤掉。

---

## 2. 3 秒聚合稀释大单

**为什么：** L1 快照每 3 秒一条，单个切片可能包含数百笔成交。高流动性股票一个 3 秒切片内，
大单与小单会混在 `dAmt` 里一起汇总。"笔均"（`dAmt/dCnt`）是混合均值，无法还原单笔最大成交。

**怎么避：**
- 不要把"切片笔均超过阈值"等同于"真实存在大单"——只是统计近似。
- `main_force_flow` 用"单切片 `dAmt >= big_threshold`"作大单脉冲判断，比笔均稳，但仍是近似。
- 真实逐笔大单识别需要 L2 逐笔成交数据。

**本包处理：** `main_force_flow(big_threshold=50e4)` 文档注明"大单脉冲近似"。`trade_size_buckets` 基于笔均分桶，局限在方法文档（methods.md §2.2）中已说明。

---

## 3. 方向分类用滞后盘口，禁用当前盘口

**为什么：** 若用当前切片的 `bid1 / ask1` 做 Lee-Ready 分类，该切片的价格本身已影响盘口，
产生 **look-ahead 泄漏**——回测/策略信号会高估胜率。

**具体机制：** 当 `last_price_t >= ask1_t` 时，这次主动买扫单已使 ask1 向上移动；
用 `ask1_t` 判断只是事后确认，而非预测性信号。真正的"pre-trade"盘口是上一快照 `ask1_{t-1}`。

**怎么避：** 使用 `prev_bid1 = bid1.shift(1)` 和 `prev_ask1 = ask1.shift(1)` 做分类依据。

**本包处理：** `_signed_increments()` 内部执行 `inc["prev_bid1"] = inc["bid_price_1"].shift(1)` 并用于 Lee-Ready 和 tick 方向判断；`effective_spread` 也使用 `mid_prev`（上一快照中价）而非当前盘口中价。

---

## 4. 竞价段 last_price 可能为 0，dVol = 0

**为什么：** 集合竞价阶段（09:15-09:25）是撮合报价阶段，`last_price` 可能为 0（尚未成交）
或仍为前日收盘价；`volume` 不变（无成交量增量）导致 `dVol = 0`。

若不过滤竞价段就直接计算：
- 竞价段 `last_price=0` 拉低 TWAP。
- `dVol=0` 使 `dAmt/dVol` 除零（VWAP 漂移）。
- Lee-Ready 用 0 价格与盘口比较得到错误方向。

**怎么避：** 先用 `split_sessions(df)` 切分会话，只对 `"continuous"` 段做 VWAP/TWAP/资金流计算。

**本包处理：** `continuous(df)` 过滤 `dVol>0` 且时间 ∈ [09:30, 14:57)；`_signed_increments` 过滤 `dAmt>0`；`opening_auction / closing_auction` 内部调用 `split_sessions` 后只取对应会话段。

---

## 5. 时间戳为 UTC 毫秒，需 +8h 转北京时间

**为什么：** QMT/xtquant 的 `time` 字段是 **UTC 毫秒整数**，
`pd.to_datetime(ms, unit="ms")` 得到 UTC 时间，比北京时间早 8 小时。
若不转换，会话切分（如 09:30 判断）会失效，时间比较出现"01:30"等异常。

**怎么避：** 转换时加 `+pd.Timedelta(hours=8)`，得到北京时间（tz-naive，语义上 UTC+8）。

**本包处理：** `from_xtquant` 内：
```python
df["datetime"] = pd.to_datetime(df["time"], unit="ms") + pd.Timedelta(hours=8)
```
`from_packed_parquet` 读取的 parquet `datetime` 列在写入时已转好，无需再处理。
`from_dataframe` **不做转换**，调用方须自行保证 `datetime` 列是北京时间。

---

## 6. 涨跌停单边盘口失真

**为什么：** 股票涨停时 `ask_price_1 = 0`（或 NaN）、`ask_vol_1 = 0`；
跌停时买方挂单清空。此时：
- OBI = `bidVol / (bidVol + 0) = 1.0`（涨停），不反映真实盘口力量。
- `quoted_spread = ask1 - bid1`：若 `ask1=0` → 负值，明显错误。
- `microprice` 分母 `bidVol + askVol = bidVol`，微价等于 bid1，失去意义。

**怎么避：** 按 `stock_status` 列过滤涨跌停时段；或在结果中将对应切片置 NaN。

**本包处理：** `order_book_imbalance` 分母为 0 时返回 NaN（通过 `.replace(0, pd.NA)`）。
价差和微价未特判——建议上层用 `stock_status` 过滤后再调用。

---

## 7. `.empty` 无数据日标记

**为什么：** 本项目 `a_share_tick.py` 在无数据日（非交易日或 QMT 未返回数据）写入 `.empty` 空标记文件，
避免批量 verify 反复重试。直接 `from_packed_parquet` 读取此文件会得到空 DataFrame 或报错。

**怎么避：** 批量循环时先检查扩展名：
```python
from pathlib import Path
for f in sorted(parquet_dir.glob("*.parquet")):
    if f.suffix != ".parquet":   # 跳过 .empty 等标记文件
        continue
    df = ta.from_packed_parquet(f, stock_code=code)
```

**本包处理：** 包本身不处理此文件约定（属于项目层逻辑）；调用方负责跳过。
删除 `.empty` 文件可强制对应日重新采集。

---

## 8. Amihud / Kyle λ 单日噪声大，需多日聚合

**为什么：** Amihud 非流动性和 Kyle λ 的日内估计各只有约 240 个 1 分钟数据点，
样本量不足导致单日估计方差极大——同一股票两天的 Amihud 值可能相差 3-5 倍。
原始 Amihud(2002) 公式本身就是**跨日平均**（通常 20-60 天）；Kyle λ 文献中也是多日估计。

**怎么避：** 将单日值存储后，用滚动均值（如 20 日）后再解读/用于选股。

**本包处理：** 函数返回单日标量值，聚合逻辑交由上层（见 recipes.md §4 流动性因子入库示例）。
方法文档（methods.md §4.2/4.3）已注明"建议多日聚合"。

---

## 9. volume 单位：手 vs 股（错误会偏差 100 倍）

**为什么：** QMT/xtquant 原始数据 `volume` 是**手（1 手 = 100 股）**，而 `amount` 是**元（按股计价）**。
若直接用原始手单位的 volume：
- `VWAP = dAmt / dVol` 量纲变成"元/手 = 100×(元/股)"，比实际高 **100 倍**。
- `participation_rate = 自身量(股) / 市场量(手)` 偏差 **100 倍**。
- `kyle_lambda` 的 `signed_vol` 单位是手，回归斜率量纲错误。

**怎么避：**
- 使用 `from_packed_parquet` 或 `from_xtquant` — 已自动执行 `volume × 100`（`lot_size=100`）。
- 使用 `from_dataframe` 时，**调用方须自行保证 volume 已是股**，必要时先 `df["volume"] *= 100`。

**本包处理：**
```python
# from_xtquant / from_packed_parquet 内部均有：
df["volume"] = df["volume"] * lot_size   # lot_size=100，手->股，保证 amount/volume=价格
```
`bid_vol / ask_vol` 盘口挂量**保留手，不换算**（仅用于 OBI/微价/委比等比值计算，不涉及绝对金额）。
