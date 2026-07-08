"""通知模块（钉钉等）。"""

from __future__ import annotations

import logging

import requests

from data_collect.config import get_dingtalk_config
from data_collect.utils.retry import retry_notify

logger = logging.getLogger(__name__)


def ensure_ding_prefix(message: str) -> str:
    """确保消息包含关键短语。"""
    config = get_dingtalk_config()
    prefix = config.get("message_prefix", "白白胖胖说")
    if prefix in message:
        return message
    return f"{prefix}：{message}"


@retry_notify
def _post_dingtalk(url: str, payload: dict) -> None:
    """实际发送请求（带重试）。"""
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()


def send_dingtalk(message: str) -> None:
    """发送钉钉文本消息。重试耗尽后只记日志，不影响主流程。"""
    config = get_dingtalk_config()
    token = config["webhook_token"]
    url = f"https://oapi.dingtalk.com/robot/send?access_token={token}"
    payload = {
        "msgtype": "text",
        "text": {"content": ensure_ding_prefix(message)},
    }
    try:
        _post_dingtalk(url, payload)
    except Exception as exc:
        logger.error(f"钉钉通知发送失败（重试耗尽）: {exc}")


def guarded_send(message: str, send=None) -> bool:
    """守护式发送：包裹 send_dingtalk，成功返回 True、异常返回 False（仅记日志）。

    send_dingtalk 自身已吞掉 POST 错误，本函数主要挡配置/前缀等异常，并提供
    bool 返回值（快讯断流告警状态机据此判定是否置 alerted）。`send` 可注入调用方
    模块级的 send_dingtalk（保留其 monkeypatch 点），缺省用本模块的 send_dingtalk。
    """
    if send is None:
        send = send_dingtalk
    try:
        send(message)
        return True
    except Exception:
        logger.warning(f"钉钉发送失败（不阻塞主流程）: {message[:60]}", exc_info=True)
        return False
