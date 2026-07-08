# 方法参考：7 类分析公式与实现

---

## 1. 资金流向 / 主动买卖

### 1.1 Lee-Ready 方向分类

**方法名：** Lee-Ready quote rule (Lee & Ready 1991)

**公式：**
```
若 price_t >= ask1_prev  → dir = +1（主动买）
若 price_t <= bid1_prev  → dir = -1（主动卖）
若 price_t > mid_prev    → dir = +1（tick test 近似）
否则                     → dir = -1
mid_prev = (bid1_prev + ask1_prev) / 2
```
`_prev` 均为上一快照的盘口（滞后一个 3 秒切片，避免 look-ahead）。

**对应函数：** `classify_trades(df, method="lee_ready")` → `pd.Series` 值为 ±1

**解读：** 正值表示该切片以卖方报价成交（主动买），负值表示以买方报价成交（主动卖）。

**局限：** 3 秒快照可含多笔混合方向成交；L1 无法分拆。高速撤单导致盘口在 3 秒内变化时，滞后盘口可能已失效。在 3 秒聚合 last_price bar 上，整个 bar 的 ΔAmt 被赋予单一 ±1 方向，方向来自 bar **收盘 tick**，当 bar 内方向混合时净流向会偏向收盘 tick 的方向，可能与价格趋势相反。**本包默认已切换为 BVC**（见 1.2 节）。

---

### 1.2 BVC 块成交分类（**默认方法**）

**方法名：** Bulk Volume Classification (Easley, López de Prado & O'Hara 2012)

**公式：**
```
Δp_t = last_price_t - last_price_{t-1}
σ_t  = EWMA 标准差(Δp, span=40)（因果，仅用 t 时刻及之前的信息）
frac_t = Φ(Δp_t / σ_t)      # Φ = 标准正态 CDF；σ_t 未达 min_periods=20 时 frac_t=0.5
buy_amt_t  = dAmt_t × frac_t
sell_amt_t = dAmt_t × (1 - frac_t)
```

**对应函数：** `classify_trades(df, method="bvc")` → `pd.Series` 命名 `buy_frac`，值 ∈[0,1]
（注意：**非** ±1，与 lee_ready/tick 返回类型不同）

`money_flow(df, method="bvc")` 使用相同 BVC 逻辑，接口与 lee_ready 统一。**BVC 是本包默认方法。**

**解读：** 价格明显上涨时买入比例趋向 1；价格明显下跌时趋向 0。适合 3 秒聚合块的整体方向估计。

**局限：** σ 采用因果 EWMA（span≈40），避免使用全天 std 造成的前视泄漏（做因子/回测时关键）。但 3 秒 clock-bar BVC 比 volume-bar BVC 退化：ΔP 零膨胀（大量切片价格不变）使 σ 偏小，frac 分布向 0/1 两端集中，在低波动期尤为明显。

> **BVC vs Lee-Ready 权衡**：BVC 由 ΔP 推方向，资金流天然与价格同号——不会出现"逆价大幅净流入"（优点），但独立信息量低，近似于价格行为的重述；Lee-Ready 借助盘口（更独立），但聚合 bar 上有单向偏。两者皆为 L1 近似，无 L2 逐笔数据无法验证真实方向。

---

### 1.3 Tick Rule

**方法名：** 价格涨跌符号法

**公式：**
```
dir_t = sign(last_price_t - last_price_{t-1})
若 dir_t = 0 则 forward fill 沿用前一非零方向；首行缺值填 +1
```

**对应函数：** `classify_trades(df, method="tick")` → `pd.Series` 值为 ±1

**解读：** 最简单的方向近似，不依赖盘口；在盘口缺失或失真时可用作备选。

**局限：** 震荡行情误判率高；不如 lee_ready 准确。

---

### 1.4 全单资金净额

**公式：**
```
buy = Σ dAmt_t × I[dir_t > 0]      （lee_ready/tick）
sell = Σ dAmt_t × I[dir_t < 0]
net = buy - sell
net_ratio = net / (buy + sell)
```
BVC 下：`buy = Σ dAmt_t × frac_t`，`sell = Σ dAmt_t × (1-frac_t)`。

**对应函数：** `money_flow(df, method="lee_ready") -> dict`

键：`buy / sell / net / net_ratio`，单位元。

---

### 1.5 主力净额

**公式：**
```
大单掩码 = (dAmt_t >= big_threshold)     # 默认 50 万元
big_buy  = Σ buy_amt_t  × I[掩码]
big_sell = Σ sell_amt_t × I[掩码]
net = big_buy - big_sell
cum_net_t = Σ_{s≤t} signed_s × I[掩码_s]
open_net  = Σ net_s × I[掩码 & t ≤ 10:00]
tail_net  = Σ net_s × I[掩码 & t ≥ 14:30]
```

**对应函数：** `main_force_flow(df, big_threshold=50e4, method="lee_ready") -> dict`

键：`big_buy / big_sell / net / cum_net(Series) / open_net / tail_net / big_count`

**解读：** 净额方向反映主动买卖力度的相对强弱；`open_net/tail_net` 反映开/尾盘主力动向。注意：绝对金额、具体席位不可由 L1 推断；分类偏差见 Lee-Ready 局限（1.1 节）。

**局限：** 阈值静态，不随价格/流动性自适应；多笔小单之和超过阈值时会被误判为大单（L1 无法排除）。

> **caveat — open_net 含集合竞价量**：`open_net` 窗口为 `t ≤ 10:00`，**包含** 09:25-09:30 集合竞价撮合量（连续段首行 volume 为累计值）。要仅统计纯连续竞价段净额，需先 `split_sessions(df)["continuous"]` 再调用。

---

## 2. 笔均 / 大中小单

### 2.1 笔均成交额

**公式：** `avg_trade_t = dAmt_t / dCnt_t`（仅 dCnt > 0 的切片）

**对应函数：** `avg_trade_amount(df) -> pd.Series`

**解读：** 笔均放大表明更多大单参与；笔均收缩表明散单密集。

**局限：** 3 秒内大单与小单混合时，笔均是混合均值，不代表单笔最大成交额。

---

### 2.2 大中小单拆分

**公式：**
```
avg_t = dAmt_t / dCnt_t
bucket_t = cut(avg_t, bins=[0, 4e4, 20e4, 100e4, ∞))
汇总 dAmt 按 bucket
```
默认标签：小单（<4万）/ 中单（4-20万）/ 大单（20-100万）/ 特大单（>100万）；
`thresholds` 参数可调。

**对应函数：** `trade_size_buckets(df, thresholds=(4e4, 20e4, 100e4), labels=(...)) -> pd.Series`

**解读：** 特大单占比高表明机构主导；小单密集可能是散户或程序化交易。

**局限：** 分类基于笔均，非单笔；3 秒聚合限制精度。

> **caveat — 阈值为固定名义元，跨股不可比**：默认阈值（4万/20万/100万元）是**固定名义金额**，低价票（¥3）与高价票（¥300）同股数落入不同桶；换手率不同的股票也无法对比。建议：跨股比较时按价格 × 日均量（ADV）缩放阈值，或仅作**同股同日**内部比较。

---

## 3. 盘口压力

### 3.1 订单簿不平衡 (OBI)

**公式：**
```
OBI_t = (ΣbidVol - ΣaskVol) / (ΣbidVol + ΣaskVol)   ∈ [-1, 1]
```
`levels` 参数控制取几档（1~5）；分母为 0 时返回 NaN。

**对应函数：** `order_book_imbalance(df, levels=1) -> pd.Series`

**解读：** OBI > 0 表示买盘挂单更厚（短期价格压力偏上）；OBI < 0 反之。

**局限：** 涨跌停单边盘口全为 0 → NaN（需过滤）。L1 快照无法捕捉盘口的高频撤单变化。

---

### 3.2 微价 (Microprice)

**方法名：** Stoikov (2018) Microprice

**公式：**
```
microprice_t = (bid1 × askVol1 + ask1 × bidVol1) / (bidVol1 + askVol1)
```
用对侧挂量加权：买量厚 → 微价偏向卖一侧（价格上压）；卖量厚 → 微价偏向买一侧。

**对应函数：** `microprice(df) -> pd.Series`

**解读：** 微价相对中价的偏移量反映短期价格预期方向；可替代简单中价用于方向判断。

---

### 3.3 委差 / 委比

**公式：**
```
weicha_t = ΣbidVol - ΣaskVol                                （委差，手）
weibi_t  = (ΣbidVol - ΣaskVol) / (ΣbidVol + ΣaskVol)      （委比 ∈ [-1,1]）
```

**对应函数：** `weiba_weicha(df, levels=5) -> pd.DataFrame` 含列 `weicha / weibi`

**解读：** 委比 > 0 表示挂买单远多于挂卖单；注意挂单可能含托盘/对倒，不等于真实买意。

---

### 3.4 盘口深度

**公式：** `depth_t = Σ_{i=1}^{levels} (bid_price_i × bid_vol_i + ask_price_i × ask_vol_i)`

**对应函数：** `book_depth(df, levels=5, side="both") -> pd.Series`

`side` 可取 `"bid" / "ask" / "both"`。返回单位为"手×元"（相对量纲，仅用于同股票同日比较）。

---

### 3.5 报价价差

**公式：**
```
spread_t    = ask1_t - bid1_t                       （绝对值，元）
spread_bp_t = spread_t / mid_t × 10000             （相对中价，基点）
```

**对应函数：** `quoted_spread(df, in_bps=False) -> pd.Series`

**解读：** 价差越窄流动性越好；竞价段或无成交时可能出现异常大价差，需过滤。

---

## 4. 流动性

### 4.1 有效价差

**方法名：** Effective Spread（Stoll 2000 综述）

**公式：**
```
eff_spread_t = 2 × |price_t - mid_t| / mid_t
```
`mid_t = (bid1_t + ask1_t) / 2`（**成交所在快照的同期中价**）；单边盘口（bid/ask≤0）返回 NaN。

**对应函数：** `effective_spread(df, in_bps=True) -> pd.Series`

默认返回基点（×10000）。

**解读：** 有效价差是成本度量，衡量已发生成交相对中价的偏离，用同期中价是标准做法。look-ahead 仅与方向分类相关，与成本度量无关，故不用滞后中价（用滞后会把切片间价格漂移注入估计，在趋势行情高估成本）。均值有意义，逐切片噪声大。

**局限：** 3 秒聚合下单快照内多笔成交混合，有效价差是聚合近似；建议看均值或百分位。

---

### 4.2 Kyle λ（价格冲击）

**方法名：** Kyle (1985) Lambda，日内回归近似

**公式：**
```
按 freq（默认 1min）聚合增量帧：
  Δprice_k  = last_price 区间末 - 区间初
  SV_k      = Σ signed_vol_t（该分钟内）
OLS 回归：Δprice_k = λ × SV_k + ε
λ = 斜率（元/股）
```

**对应函数：** `kyle_lambda(df, freq="1min", method="lee_ready") -> float`

少于 3 个聚合点时返回 NaN。

**解读：** λ 越大，单位签名量带来的价格变动越大（冲击成本越高，流动性越差）。

**局限：** 单日样本点约 240（1min）；方差大。建议多日取均值后再解读。

---

### 4.3 Amihud 非流动性

**方法名：** Amihud (2002) Illiquidity（日内近似）

**公式：**
```
按 freq（默认 1min）聚合：
  |ret_k|   = |last_price_k / last_price_{k-1} - 1|
  amt_k_亿  = Σ dAmt_t / 1e8
ILLIQ = mean(|ret_k| / amt_k_亿)   （inf/NaN 行已剔除）
```

**对应函数：** `amihud_illiquidity(df, freq="1min") -> float`

**解读：** 值越大表示单位资金量引发的价格波动越大（流动性越差）。

**局限：** 这是日内近似，非原始 Amihud(2002) 跨日公式；单日噪声大，需多日聚合。

---

### 4.4 Roll 隐含价差

**方法名：** Roll (1984) Implied Spread

**公式：**
```
仅取 dAmt>0 的切片：
  Δp_t = last_price_t - last_price_{t-1}
  cov  = Cov(Δp_t, Δp_{t-1})
Roll Spread = 2 × sqrt(-cov)    （若 cov >= 0 返回 NaN）
```

**对应函数：** `roll_spread(df) -> float`

**解读：** 负自协方差源自价差造成的价格反转；Roll 估计隐含交易成本（元）。

**局限：** 趋势行情下 cov 可能为正，导致返回 NaN（非错误，正确行为）；需足够多的切片（>=3）。

> **caveat — 日内 3 秒序列 Roll 多数不可用**：日内 3 秒快照受信息驱动漂移与离散化报价影响，Cov(Δp_t, Δp_{t-1}) 常为正值而非负值，导致函数返回 NaN。**多数活跃交易日 Roll 价差有偏或完全不可用**，仅作粗略横截面参考（如对同一股票不同日比较趋势，而非与其他指标混用）。

---

## 5. 竞价

### 5.1 开盘竞价

**时间段：** 09:15–09:25（`split_sessions` 的 `open_auction` 会话）。

**实现逻辑：**
- `open_price`：连续竞价段首行 `last_price`（09:30 首条 tick）
- `prev_close`：当日首行 `last_close`（若列存在；否则 NaN）
- `auction_pct`：`(open_price / prev_close - 1) × 100`
- `auction_volume`：连续段首行 `volume`（股，含竞价撮合量）
- `n_snapshots`：竞价段快照数

**对应函数：** `opening_auction(df) -> dict`；若连续段为空返回 `{}`。

**解读：** `auction_pct` > 0 表示高开；`auction_volume` 大表示竞价撮合活跃。

**局限：** `auction_volume` 是累计值而非纯竞价增量；9:15-9:25 指示价路径未提取（可通过 `split_sessions["open_auction"]["last_price"]` 获得序列）。

> **caveat — open_price 与 auction_volume 取自连续段首行**：`open_price` 和 `auction_volume` 均取自**连续段首行**（09:30 后第一条快照），而非 09:25 竞价撮合快照本身。对于活跃股，连续段首条快照已可能包含 09:30 后若干毫秒的成交；`auction_volume` 为当日累计值，非竞价纯增量。两者作近似使用，精度取决于股票的流动性和 QMT 推送延迟。

---

### 5.2 收盘竞价

**时间段：** 14:57–15:00（`split_sessions` 的 `close_auction` 会话）。

**实现逻辑：**
- `close_price`：收盘竞价段末行 `last_price`
- `n_snapshots`：快照数

**对应函数：** `closing_auction(df) -> dict`；若收盘段为空返回 `{}`。

---

### 5.3 竞价特征合并

**对应函数：** `auction_features(df) -> dict`

合并 `opening_auction` + `closing_auction` 所有键，供批量扫描一次性取全部竞价特征。

---

## 6. 执行分析 (TCA)

### 6.1 VWAP

**公式：** `VWAP = Σ dAmt_t / Σ dVol_t`（仅 dVol>0 的切片；`start/end` 区间可选）

**对应函数：** `vwap(df, start=None, end=None) -> float`

`start/end` 接受 `Timestamp` 或可被 `pd.Timestamp()` 解析的字符串。

---

### 6.2 TWAP

**公式：** `TWAP = mean(last_price_t)`（连续段 dVol>0 切片；`start/end` 区间可选）

**对应函数：** `twap(df, start=None, end=None) -> float`

---

### 6.3 相对 VWAP 的滑点

**公式：**
```
avg_fill = Σ(fill.price × fill.volume) / Σ fill.volume
sign = +1（fills.side 首行 == "buy"）或 -1（"sell"）
slippage_bp = sign × (avg_fill - VWAP) / VWAP × 10000
```
买单正值表示成交高于 VWAP（不利），负值表示有利。

**对应函数：** `slippage_vs_vwap(fills, df) -> float`

`fills`：DataFrame 含列 `price / volume / side`（"buy"/"sell"）。

---

### 6.4 参与率

**公式：** `participation_rate = Σ fills.volume / Σ dVol_market`

**对应函数：** `participation_rate(fills, df, start=None, end=None) -> float`

---

### 6.5 实现差额 (IS)

**公式：**
```
avg_fill = Σ(price × volume) / Σ volume
IS_bps = (avg_fill - arrival_price) / arrival_price × 10000
```
买方向 IS > 0 表示成本高于到达价。卖方向需调用方自行取负。

**对应函数：** `implementation_shortfall(arrival_price, fills) -> dict`

键：`avg_fill / arrival_price / is_bps`。

---

## 附录：仅文档（未实现代码）

### A. VPIN（订单流毒性）

**方法名：** Volume-synchronized Probability of Informed Trading (Easley et al. 2012)

**公式概要：**
```
将成交量分成等量桶（桶大小 = 日均量 / n）
每桶估算：buy_vol_k = Σ dVol × frac_BVC
          sell_vol_k = Σ dVol × (1-frac_BVC)
VPIN = mean(|buy_vol_k - sell_vol_k| / bucket_size)   ∈ [0,1]
```

**争议警示：** Andersen & Bondarenko (2014) 指出 VPIN 在高波动期会系统性虚高，作为危机预警指标的可靠性存疑。L1 上 BVC 本身已有近似误差，VPIN 会进一步放大误差。**建议仅作探索性指标，不可用于强信号决策。**

**未实现原因：** L1 精度下价值存疑；`trade_size_buckets` 已涵盖大部分大单分析需求。

---

### B. 已实现波动率 / 跳跃检测 / 日内季节性

**已实现波动率（RV）：**
```
RV = Σ_t (ln(last_price_t) - ln(last_price_{t-1}))²
```
3 秒采样已足够日内 RV；但存在微观结构噪声偏差（可用 Zhang et al. 2005 两次采样法修正）。
**未实现。**

**双幂变差（BPV）：**
```
BPV = (π/2) × Σ |ret_t| × |ret_{t-1}|
```
`RV - BPV > 0` 估计跳跃贡献（Barndorff-Nielsen & Shephard 2004）。**未实现。**

**日内季节性：**
```
seasonal_h = mean_{day}(|ret_{h}|) / global_mean(|ret|)   （按时间 bin 归一化）
```
A 股典型：开盘（9:30-10:00）与收盘前（14:30-15:00）波动最大；午盘（11:30-13:00）最小。
可用 `continuous(df)` 后按时间 bin groupby 计算。**未实现（骨架留给上层）。**
