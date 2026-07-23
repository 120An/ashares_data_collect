"""数据源注册表 CLI（spec §8）：validate / list / test <id>。

用法（conda py10）:
    python manage_sources.py validate            # schema 校验（提交前必跑）
    python manage_sources.py list [--job X] [--channel Y] [--disabled]
    python manage_sources.py test <id>           # 单源冒烟：真拉一次，不归档不入库
"""

from __future__ import annotations

import argparse
import sys

from data_collect.utils import source_registry as sr


def cmd_validate(_args) -> int:
    errors = sr.validate_registry()
    if errors:
        print(f"校验失败（{len(errors)} 处）:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print(f"校验通过：{len(sr.load_all())} 个源")
    return 0


def cmd_list(args) -> int:
    sources = sr.load_all()
    if args.job:
        sources = [s for s in sources if s.job == args.job]
    if args.channel:
        sources = [s for s in sources if s.channel == args.channel]
    if args.disabled:
        sources = [s for s in sources if not s.enabled]
    print(f"{'id':<22}{'adapter':<10}{'channel':<14}{'job':<18}{'on':<4}note")
    for s in sources:
        print(f"{s.id:<22}{s.adapter:<10}{s.channel:<14}{s.job:<18}"
              f"{'Y' if s.enabled else 'N':<4}{s.note}")
    print(f"共 {len(sources)} 个源")
    return 0


def cmd_test(args) -> int:
    """单源冒烟：统一走适配器层 smoke_test（headers/proxy/timeout 同生产）。"""
    from data_collect.utils import source_adapters as sa

    s = sr.get_source(args.id)
    try:
        print(f"[{s.id}] {sa.smoke_test(s)}")
    except (NotImplementedError, RuntimeError) as exc:
        print(f"[{s.id}] 冒烟失败: {exc}")
        return 2
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="数据源注册表管理")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("validate", help="schema 校验")
    p_list = sub.add_parser("list", help="源总览")
    p_list.add_argument("--job")
    p_list.add_argument("--channel")
    p_list.add_argument("--disabled", action="store_true", help="只看停用源")
    p_test = sub.add_parser("test", help="单源冒烟")
    p_test.add_argument("id")
    args = parser.parse_args(argv)
    return {"validate": cmd_validate, "list": cmd_list, "test": cmd_test}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
