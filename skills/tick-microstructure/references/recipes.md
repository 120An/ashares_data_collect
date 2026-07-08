# 可复制片段（Recipes）

以下代码片段可直接复制使用，均基于 `tick_analysis` 公共 API。
前提：`tick_analysis/` 目录在 Python 路径上（或整个 skill 目录已拷贝到项目中）。

---

## 1. 批量多日主力净额

循环读取多个交易日的打包 parquet，聚合主力净额为按日 DataFrame。

```python
from pathlib import Path
import pandas as pd
import tick_analysis as ta

PARQUET_DIR = Path("Z:/A股冷数据/tick_by_day")
STOCK = "002602.SZ"

records = []
for year_dir in sorted(PARQUET_DIR.iterdir()):
    for month_dir in sorted(year_dir.iterdir()):
        for f in sorted(month_dir.glob("*.parquet")):
            # 跳过 .empty 无数据日标记（见 pitfalls.md #7）
            if f.suffix != ".parquet":
                continue
            try:
                df = ta.from_packed_parquet(f, stock_code=STOCK)
            except Exception:
                continue
            if df.empty:
                continue
            mf = ta.main_force_flow(df, big_threshold=50e4, method="lee_ready")
            records.append({
                "date": f"{year_dir.name}-{month_dir.name}-{f.stem}",
                "net": mf["net"],
                "big_buy": mf["big_buy"],
                "big_sell": mf["big_sell"],
                "open_net": mf["open_net"],
                "tail_net": mf["tail_net"],
            })

daily_main = pd.DataFrame(records).set_index("date")
print(daily_main.tail())
```

---

## 2. 画日内累计主力净额

`main_force_flow()` 返回的 `"cum_net"` 是 `pd.Series`（index 为增量帧行号），
可与 `datetime` 列对齐后直接 `.plot()`。

```python
import tick_analysis as ta
import matplotlib.pyplot as plt

df = ta.from_packed_parquet(
    "Z:/A股冷数据/tick_by_day/2026/06/26.parquet",
    stock_code="002602.SZ"
)

mf = ta.main_force_flow(df, big_threshold=50e4, method="lee_ready")
cum_net = mf["cum_net"]   # pd.Series，index 为 df 的整数行号（增量帧的位置索引子集）

# 对齐 datetime（cum_net.index 是 df 的行号子集，必须用 .iloc 而非 .loc）
# 直接 cum_net.plot() 会以行号作 x 轴，不是时间轴！
times = df["datetime"].iloc[cum_net.index]

fig, ax = plt.subplots(figsize=(12, 4))
ax.plot(times.values, cum_net.values / 1e8, label="主力累计净额(亿元)")
ax.axhline(0, color="gray", lw=0.8, ls="--")
ax.set_title("002602.SZ 日内累计主力净额 2026-06-26")
ax.set_ylabel("亿元")
ax.legend()
plt.tight_layout()
plt.show()
```

---

## 3. 竞价异动扫描（多股票）

对当日多只股票批量提取竞价特征，筛选高开或竞价异动标的。

```python
from pathlib import Path
import pandas as pd
import tick_analysis as ta

PARQUET_PATH = Path("Z:/A股冷数据/tick_by_day/2026/06/26.parquet")
STOCKS = ["002602.SZ", "000001.SZ", "600036.SH", "300750.SZ"]

rows = []
for code in STOCKS:
    try:
        df = ta.from_packed_parquet(PARQUET_PATH, stock_code=code)
    except Exception:
        continue
    if df.empty:
        continue
    feat = ta.auction_features(df)
    feat["stock_code"] = code
    rows.append(feat)

auction_df = pd.DataFrame(rows).set_index("stock_code")

# 筛选：高开超 2% 且竞价快照数正常（>=5 条表明有数据）
# 注意：筛选键为 open_n_snapshots（非旧版 n_snapshots）；
# auction_pct 依赖 last_close——若数据源缺此列，auction_pct 全 NaN，筛选静默落空。
high_open = auction_df[
    (auction_df.get("auction_pct", pd.Series(dtype=float)) > 2.0) &
    (auction_df.get("open_n_snapshots", pd.Series(dtype=int)) >= 5)
]
print(high_open[["open_price", "prev_close", "auction_pct", "auction_volume"]])
```

---

## 4. 流动性因子入库雏形

对多只股票单日分别计算 Amihud / Kyle λ / 报价价差 / Roll，汇成 DataFrame 供写库或 CSV。

```python
from pathlib import Path
import pandas as pd
import tick_analysis as ta

PARQUET_PATH = Path("Z:/A股冷数据/tick_by_day/2026/06/26.parquet")
TRADE_DATE = "2026-06-26"
STOCKS = ["002602.SZ", "000001.SZ", "600036.SH"]

rows = []
for code in STOCKS:
    try:
        df = ta.from_packed_parquet(PARQUET_PATH, stock_code=code)
    except Exception:
        continue
    if df.empty:
        continue
    rows.append({
        "trade_date": TRADE_DATE,
        "stock_code": code,
        "amihud":       ta.amihud_illiquidity(df, freq="1min"),
        "kyle_lambda":  ta.kyle_lambda(df, freq="1min", method="lee_ready"),
        "spread_bp":    float(ta.quoted_spread(df, in_bps=True).mean()),
        "roll_spread":  ta.roll_spread(df),
    })

factor_df = pd.DataFrame(rows)
print(factor_df)

# 写 CSV（替换为 DB 写入逻辑即可入库）
factor_df.to_csv(f"liquidity_factors_{TRADE_DATE}.csv", index=False)

# 注意：Amihud/Kyle λ 单日噪声大，使用前建议 20 日滚动均值
# factor_df.sort_values("trade_date") \
#          .groupby("stock_code")["amihud"] \
#          .transform(lambda s: s.rolling(20).mean())
```
