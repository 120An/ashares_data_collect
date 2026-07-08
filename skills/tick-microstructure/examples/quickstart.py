"""tick-microstructure 快速上手：读一只票 → 算全套指标 → 打印。

运行：
    python examples/quickstart.py
"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import tick_analysis as ta

PARQUET = r"Z:\A股冷数据\tick_by_day\2026\06\26.parquet"
CODE = "002602.SZ"


def main():
    df = ta.from_packed_parquet(PARQUET, stock_code=CODE)
    print(f"{CODE} 当日 {len(df)} 条 tick")

    r = ta.analyze(df, big_threshold=50e4)
    mf, big = r["money_flow"], r["main_force"]
    print(f"全单主动净额: {mf['net'] / 1e8:+.2f} 亿  (买{mf['buy']/1e8:.2f}/卖{mf['sell']/1e8:.2f})")
    print(f"主力净额    : {big['net'] / 1e8:+.2f} 亿  尾盘 {big['tail_net']/1e8:+.2f} 亿")
    print(f"VWAP        : {r['vwap']:.3f}")
    print(f"盘口OBI均值 : {r['orderbook']['obi_mean']:+.3f}  价差 {r['orderbook']['spread_bp_mean']:.1f}bp")
    print(f"Amihud      : {r['liquidity']['amihud']:.4f}")
    print(f"竞价        : {r['auction']}")


if __name__ == "__main__":
    main()
