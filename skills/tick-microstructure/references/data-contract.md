# 数据契约参考

本文档定义 `tick_analysis` 包消费的标准列格式、校验规则，以及三个适配器的使用方法与单位转换细节。

---

## 1. 列契约（Canonical Schema）

所有函数输入均为**单只股票**、按 `datetime` 升序排列的 DataFrame。

### 1.1 必需列

| 列名 | 含义 | 类型 | 单位 | 备注 |
|---|---|---|---|---|
| `datetime` | 快照时间 | datetime64[ns] | **北京时间**（tz-naive 或 UTC+8） | 必须严格升序 |
| `last_price` | 最新成交价 | float | 元 | |
| `volume` | **累计**成交量 | int/float | **股（shares）** | 适配器已将手→股；通用源须自保证 |
| `amount` | **累计**成交额 | float | **元** | |
| `transaction_num` | **累计**成交笔数 | int/float | — | |

注意：`volume / amount / transaction_num` 均为**当日累计值**，内部通过 `to_increments()` 差分取增量再计算。

### 1.2 盘口列（至少提供一档）

| 列名 | 含义 | 类型 | 单位 |
|---|---|---|---|
| `bid_price_1..5` | 买一至买五报价 | float | 元 |
| `ask_price_1..5` | 卖一至卖五报价 | float | 元 |
| `bid_vol_1..5` | 买一至买五挂量 | float | **手（源原生，不换算）** |
| `ask_vol_1..5` | 卖一至卖五挂量 | float | **手（源原生，不换算）** |

盘口量仅用于 OBI / 微价 / 委比等**比值**计算，不涉及绝对金额，故保留源原生单位（手），不换算为股。

### 1.3 可选列

| 列名 | 含义 | 类型 |
|---|---|---|
| `open` | 当日开盘价 | float |
| `high` | 当日最高价 | float |
| `low` | 当日最低价 | float |
| `last_close` | 前收盘价 | float |
| `stock_code` | 股票代码（如 `002602.SZ`） | str |
| `stock_status` | 交易状态（如涨跌停标记） | str/int |

`last_close` 在 `opening_auction()` 中用于计算竞价涨跌幅；缺失时对应字段返回 NaN。

### 1.4 volume 单位说明（关键）

| 数据源 | volume 原生单位 | 适配器是否换算 |
|---|---|---|
| QMT/xtquant 原始 | **手（lots）** | `from_xtquant` 自动 `× lot_size(=100)` → 股 |
| 本项目打包 parquet | **手（lots）** | `from_packed_parquet` 自动 `× lot_size(=100)` → 股 |
| 通用 DataFrame | **调用方决定** | `from_dataframe` **不做任何缩放** |

契约不变量：`amount ≈ VWAP × volume`（两边单位一致时成立，元 / 股 = 元/股）。

若 volume 仍是手未换算，VWAP（`dAmt / dVol`）和参与率（自身量 / 市场量）会偏差 **100 倍**，请务必注意。

---

## 2. 适配器

### 2.1 `from_packed_parquet` — 本项目打包 parquet

```python
import tick_analysis as ta

# 从文件路径读取（pyarrow 谓词下推，按 stock_code 过滤）
df = ta.from_packed_parquet(
    "Z:/A股冷数据/tick_by_day/2026/06/26.parquet",
    stock_code="002602.SZ",    # 可选，None 则读全部股票
    lot_size=100               # 手->股换算倍数，默认 100
)

# 也可传已读入的 DataFrame（跳过 parquet 读取步骤）
df = ta.from_packed_parquet(existing_df, stock_code="002602.SZ")
```

**行为：**
- 需要 `pyarrow`（仅此函数）；缺失时抛清晰 `RuntimeError`，不影响其他函数。
- `volume` 列自动乘以 `lot_size`（手→股）。
- 调用 `validate_tick_frame()` 校验必需列和盘口列。

**适合：** 本项目 `a_share_tick.py` 采集的按日打包 parquet（列已天然契约对齐）。

> **警告 — `stock_code=None` 读取全市场数据**：传 `None` 时会读取当日全部股票，**仅供手工检视或离线抽样**；不要将其输出直接传入分析函数（`to_increments`、`main_force_flow` 等），这些函数**要求单只股票**，跨股 diff 会把不同股票的累计量混算，结果无意义。批量分析请逐股循环传 `stock_code`。

---

### 2.2 `from_xtquant` — xtquant 原始 DataFrame

```python
# xtdata.get_market_data_ex(field_list=[], stock_list=["002602.SZ"], period="tick")
# 返回 {"002602.SZ": DataFrame}，DataFrame 含 xtquant 原始列

raw_df = xt_data["002602.SZ"]
df = ta.from_xtquant(
    raw_df,
    stock_code="002602.SZ",   # 可选，注入 stock_code 列
    lot_size=100               # 手->股换算倍数，默认 100
)
```

**行为：**
- 展开列表列 `askPrice / bidPrice / askVol / bidVol` → `ask_price_1..5 / ask_vol_1..5` 等。
- `time`（UTC 毫秒整数）→ `datetime`（`+8h` 北京时间）。
- 标量列重命名（见下方映射表）。
- `volume` 列自动乘以 `lot_size`（手→股）。

---

### 2.3 `from_dataframe` — 通用列名映射

```python
df = ta.from_dataframe(
    your_df,
    mapping={
        "ts": "datetime",           # 你的时间列 -> 契约列名（须已是北京时间）
        "close": "last_price",
        "vol": "volume",            # 注意：不做单位缩放！须自行保证已是股
        "turnover": "amount",
        "trade_num": "transaction_num",
        "b1": "bid_price_1", "a1": "ask_price_1",
        "bv1": "bid_vol_1", "av1": "ask_vol_1",
        # ...
    }
)
```

**行为：** 仅做列重命名，随后调用 `validate_tick_frame()`。**不做任何单位换算，不做时区转换。**

**适合：** 接入 Wind、通达信、其他第三方 tick 数据；调用方须自行完成 volume 单位换算和时区转换。

> **注意 — auction_pct 依赖 last_close**：接入第三方源时，务必在 `mapping` 中映射 `last_close`（前收盘价）；若缺失，`opening_auction()` 返回的 `auction_pct` 全为 NaN，竞价异动筛选会**静默落空**（无报错）。

---

### 2.4 `validate_tick_frame` — 独立校验

```python
df = ta.validate_tick_frame(df, levels=5, require_orderbook=True)
```

- 检查必需列是否存在；缺失时抛 `ValueError`（错误信息列出缺失列名）。
- `require_orderbook=True`（默认）：检查至少一档 `bid_price_1 / ask_price_1 / bid_vol_1 / ask_vol_1`。
- 按 `datetime` 升序排序并 `reset_index`，返回拷贝。

---

### 2.5 `fills` 契约（执行分析函数专用）

`slippage_vs_vwap / participation_rate / implementation_shortfall` 需要调用方传入 `fills` DataFrame，契约如下：

| 列名 | 含义 | 类型 | 单位 |
|---|---|---|---|
| `price` | 成交价 | float | 元 |
| `volume` | 成交量 | int/float | **股（shares，须与契约 volume 同单位）** |
| `side` | 方向 | str | `"buy"` 或 `"sell"` |

> **注意**：`fills.volume` 须与 tick 契约 `volume` 同单位（**股**），否则 `participation_rate`（自身量/市场量）结果偏差 100 倍。当前实现按 `fills["side"].iloc[0]` 将整组 fills 视为同一方向；混合方向订单需调用方拆分后分别传入。

---

## 3. xtquant 原始字段 <-> 契约列映射表

| xtquant 原始列 | 契约列 | 转换规则 |
|---|---|---|
| `time` | `datetime` | `pd.to_datetime(ms, unit="ms") + pd.Timedelta(hours=8)` |
| `lastPrice` | `last_price` | 直接重命名 |
| `volume` | `volume` | `× lot_size(100)` **手→股** |
| `amount` | `amount` | 直接重命名（元，不变） |
| `transactionNum` | `transaction_num` | 直接重命名 |
| `lastClose` | `last_close` | 直接重命名 |
| `stockStatus` | `stock_status` | 直接重命名 |
| `open` | `open` | 直接重命名 |
| `high` | `high` | 直接重命名 |
| `low` | `low` | 直接重命名 |
| `askPrice`（list 列） | `ask_price_1..5` | `df[raw_col].apply(lambda x: x[i])` for i=0..4 |
| `bidPrice`（list 列） | `bid_price_1..5` | 同上 |
| `askVol`（list 列） | `ask_vol_1..5` | 同上（保留**手**，不换算） |
| `bidVol`（list 列） | `bid_vol_1..5` | 同上（保留**手**，不换算） |

**核心逻辑：** xtquant 的 `volume` 是**手**，`amount` 是**元**；适配器只换算 `volume`，`amount` 不变，
从而保证 `amount / volume ≈ VWAP（元/股）` 成立。

---

## 4. 常见错误与排查

| 症状 | 可能原因 | 排查方法 |
|---|---|---|
| `ValueError: tick frame 缺必需列: ['volume']` | 列名不一致 | 用 `from_dataframe(df, mapping={...})` |
| VWAP 比实际价格大约 100 倍 | volume 单位是手而非股 | 确认用 `from_xtquant` 或手动 `df["volume"] *= 100` |
| OBI 全为 NaN | 盘口列缺失或命名错误 | 检查 `bid_vol_1 / ask_vol_1` 是否存在 |
| `RuntimeError: from_packed_parquet 需要 pyarrow` | pyarrow 未安装 | `pip install pyarrow` |
| `kyle_lambda` 返回 NaN | 聚合后少于 3 个点 | 换更粗的 `freq`（如 `"5min"`）或提供更长数据段 |
| `roll_spread` 返回 NaN | cov >= 0（趋势行情正常） | 非错误，单日强趋势下 Roll 公式不适用 |
