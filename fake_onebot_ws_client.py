from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import random
import signal
import time
from dataclasses import dataclass, field
from itertools import count
from typing import Any


DEFAULT_SELF_ID = 123456789
DEFAULT_MESSAGE = "hello from fake onebot load test"
DEFAULT_BAD_MESSAGE = "blocked text from fake onebot load test"
DEFAULT_TIMEOUT_SECONDS = 15.0
STATS_INTERVAL_SECONDS = 1.0
ONEBOT_OK_STATUS = "ok"
ONEBOT_FAILED_STATUS = "failed"


@dataclass(frozen=True, slots=True)
class LoadConfig:
    url: str
    token: str
    self_id: int
    rate: float
    duration_seconds: float
    burst: int
    groups: int
    users_per_group: int
    group_id: int | None
    user_id: int | None
    bad_ratio: float
    notice_ratio: float
    slow_action_ratio: float
    fail_action_ratio: float
    slow_action_delay_seconds: float
    action_timeout_ratio: float
    message: str
    bad_message: str


@dataclass(slots=True)
class RunStats:
    started_at: float = field(default_factory=time.monotonic)
    sent_events: int = 0
    sent_messages: int = 0
    sent_notices: int = 0
    sent_bad_messages: int = 0
    received_actions: int = 0
    responded_actions: int = 0
    failed_actions: int = 0
    slow_actions: int = 0
    dropped_actions: int = 0
    unknown_actions: int = 0
    send_errors: int = 0
    receive_errors: int = 0

    def elapsed_seconds(self) -> float:
        return max(time.monotonic() - self.started_at, 0.001)

    def summary_line(self) -> str:
        elapsed = self.elapsed_seconds()
        return (
            f"elapsed={elapsed:.1f}s "
            f"events={self.sent_events} "
            f"messages={self.sent_messages} "
            f"notices={self.sent_notices} "
            f"bad_messages={self.sent_bad_messages} "
            f"actions={self.received_actions} "
            f"responded={self.responded_actions} "
            f"failed_actions={self.failed_actions} "
            f"slow_actions={self.slow_actions} "
            f"dropped_actions={self.dropped_actions} "
            f"unknown_actions={self.unknown_actions} "
            f"send_errors={self.send_errors} "
            f"receive_errors={self.receive_errors} "
            f"event_rate={self.sent_events / elapsed:.1f}/s"
        )


class PayloadFactory:
    def __init__(self, config: LoadConfig) -> None:
        self._config = config
        self._message_ids = count(100000)
        self._notice_ids = count(900000)

    def next_event(self) -> dict[str, Any]:
        if random.random() < self._config.notice_ratio:
            return self.next_notice()
        return self.next_group_message()

    def next_group_message(self) -> dict[str, Any]:
        group_id = self._group_id()
        user_id = self._user_id()
        message_id = next(self._message_ids)
        is_bad = random.random() < self._config.bad_ratio
        text = self._config.bad_message if is_bad else self._config.message
        return {
            "time": int(time.time()),
            "self_id": self._config.self_id,
            "post_type": "message",
            "message_type": "group",
            "sub_type": "normal",
            "message_id": message_id,
            "group_id": group_id,
            "user_id": user_id,
            "raw_message": text,
            "message": [{"type": "text", "data": {"text": text}}],
            "sender": {
                "user_id": user_id,
                "nickname": f"load-user-{user_id}",
                "card": f"load-card-{user_id}",
                "role": "member",
            },
        }

    def next_notice(self) -> dict[str, Any]:
        group_id = self._group_id()
        user_id = self._user_id()
        operator_id = self._user_id()
        notice_id = next(self._notice_ids)
        if notice_id % 2 == 0:
            return {
                "time": int(time.time()),
                "self_id": self._config.self_id,
                "post_type": "notice",
                "notice_type": "group_recall",
                "group_id": group_id,
                "user_id": user_id,
                "operator_id": operator_id,
                "message_id": max(notice_id - 1, 1),
            }
        return {
            "time": int(time.time()),
            "self_id": self._config.self_id,
            "post_type": "notice",
            "notice_type": "notify",
            "sub_type": "group_name",
            "group_id": group_id,
            "user_id": user_id,
            "operator_id": operator_id,
            "group_name": f"load-group-{group_id}-{notice_id}",
        }

    def _group_id(self) -> int:
        if self._config.group_id is not None:
            return self._config.group_id
        return 100000 + random.randrange(max(self._config.groups, 1))

    def _user_id(self) -> int:
        if self._config.user_id is not None:
            return self._config.user_id
        return 200000 + random.randrange(max(self._config.users_per_group, 1))


class ActionResponder:
    def __init__(self, config: LoadConfig, stats: RunStats) -> None:
        self._config = config
        self._stats = stats
        self._message_ids = count(700000)

    async def respond(self, ws: Any, request: dict[str, Any]) -> None:
        action = str(request.get("action") or "")
        params = request.get("params")
        if not isinstance(params, dict):
            params = {}
        echo = request.get("echo")
        self._stats.received_actions += 1

        if random.random() < self._config.action_timeout_ratio:
            self._stats.dropped_actions += 1
            return

        if random.random() < self._config.slow_action_ratio:
            self._stats.slow_actions += 1
            await asyncio.sleep(self._config.slow_action_delay_seconds)

        should_fail = random.random() < self._config.fail_action_ratio
        response = self._build_response(action, params, echo, failed=should_fail)
        if should_fail:
            self._stats.failed_actions += 1
        if response.get("retcode") == 1404:
            self._stats.unknown_actions += 1

        await ws.send_str(json.dumps(response, separators=(",", ":")))
        self._stats.responded_actions += 1

    def _build_response(
        self,
        action: str,
        params: dict[str, Any],
        echo: Any,
        *,
        failed: bool,
    ) -> dict[str, Any]:
        if failed:
            return _onebot_failed(echo, retcode=100, message="simulated failure")

        handlers = {
            "get_login_info": self._get_login_info,
            "send_group_msg": self._message_action,
            "send_group_forward_msg": self._message_action,
            "delete_msg": self._empty_action,
            "set_group_ban": self._empty_action,
            "upload_group_file": self._empty_action,
            "get_group_info": self._get_group_info,
            "get_group_info_ex": self._get_group_info,
            "get_group_member_list": self._get_group_member_list,
            "set_group_name": self._empty_action,
            "set_group_portrait": self._empty_action,
        }
        handler = handlers.get(action)
        if handler is None:
            return _onebot_failed(echo, retcode=1404, message=f"unknown action: {action}")
        return _onebot_ok(echo, handler(params))

    def _get_login_info(self, params: dict[str, Any]) -> dict[str, Any]:
        del params
        return {
            "user_id": self._config.self_id,
            "nickname": "fake-onebot-load-bot",
        }

    def _message_action(self, params: dict[str, Any]) -> dict[str, Any]:
        del params
        return {"message_id": next(self._message_ids)}

    def _empty_action(self, params: dict[str, Any]) -> dict[str, Any]:
        del params
        return {}

    def _get_group_info(self, params: dict[str, Any]) -> dict[str, Any]:
        group_id = _as_int(params.get("group_id"), default=100000)
        return {
            "group_id": group_id,
            "group_name": f"load-group-{group_id}",
            "member_count": self._config.users_per_group,
            "max_member_count": max(self._config.users_per_group, 500),
        }

    def _get_group_member_list(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        group_id = _as_int(params.get("group_id"), default=100000)
        return [
            {
                "group_id": group_id,
                "user_id": 200000 + index,
                "nickname": f"load-user-{index}",
                "card": f"load-card-{index}",
                "role": "member",
            }
            for index in range(max(self._config.users_per_group, 1))
        ]


async def run(config: LoadConfig) -> int:
    try:
        import aiohttp
    except ImportError:
        print(
            "Missing dependency: aiohttp. "
            "Install with: python -m pip install -r tools/loadtest/requirements.txt"
        )
        return 2

    stats = RunStats()
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    headers = {}
    if config.token:
        headers["Authorization"] = f"Bearer {config.token}"
    headers["X-Self-ID"] = str(config.self_id)
    headers["X-Client-Role"] = "Universal"

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=DEFAULT_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async with session.ws_connect(config.url, heartbeat=30) as ws:
            factory = PayloadFactory(config)
            responder = ActionResponder(config, stats)
            tasks = [
                asyncio.create_task(_send_events(ws, factory, config, stats, stop_event)),
                asyncio.create_task(_receive_actions(ws, responder, stats, stop_event)),
                asyncio.create_task(_print_stats(stats, stop_event)),
            ]
            try:
                await _wait_for_completion(config, stop_event)
            finally:
                stop_event.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                print("final " + stats.summary_line())
    return 0


async def _send_events(
    ws: Any,
    factory: PayloadFactory,
    config: LoadConfig,
    stats: RunStats,
    stop_event: asyncio.Event,
) -> None:
    if config.burst > 0:
        for _ in range(config.burst):
            if stop_event.is_set():
                return
            await _send_one_event(ws, factory, config, stats)

    if config.rate <= 0:
        return

    interval = 1.0 / config.rate
    next_send_at = time.monotonic()
    while not stop_event.is_set():
        now = time.monotonic()
        if now < next_send_at:
            await asyncio.sleep(next_send_at - now)
        await _send_one_event(ws, factory, config, stats)
        next_send_at += interval


async def _send_one_event(
    ws: Any,
    factory: PayloadFactory,
    config: LoadConfig,
    stats: RunStats,
) -> None:
    payload = factory.next_event()
    try:
        await ws.send_str(json.dumps(payload, separators=(",", ":")))
        stats.sent_events += 1
        if payload.get("post_type") == "notice":
            stats.sent_notices += 1
        else:
            stats.sent_messages += 1
            if str(payload.get("raw_message")) == config.bad_message:
                stats.sent_bad_messages += 1
    except Exception:
        stats.send_errors += 1
        raise


async def _receive_actions(
    ws: Any,
    responder: ActionResponder,
    stats: RunStats,
    stop_event: asyncio.Event,
) -> None:
    async for message in ws:
        if stop_event.is_set():
            return
        message_type = getattr(getattr(message, "type", None), "name", "")
        if message_type == "TEXT":
            try:
                payload = json.loads(message.data)
            except json.JSONDecodeError:
                stats.receive_errors += 1
                continue
            if isinstance(payload, dict) and "action" in payload:
                await responder.respond(ws, payload)
        elif message_type in {"CLOSED", "ERROR"}:
            stop_event.set()
            return


async def _print_stats(stats: RunStats, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(STATS_INTERVAL_SECONDS)
        print(stats.summary_line())


async def _wait_for_completion(
    config: LoadConfig,
    stop_event: asyncio.Event,
) -> None:
    if config.duration_seconds <= 0:
        await stop_event.wait()
        return
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop_event.wait(), timeout=config.duration_seconds)
    stop_event.set()


def _onebot_ok(echo: Any, data: Any) -> dict[str, Any]:
    return {
        "status": ONEBOT_OK_STATUS,
        "retcode": 0,
        "data": data,
        "echo": echo,
    }


def _onebot_failed(echo: Any, *, retcode: int, message: str) -> dict[str, Any]:
    return {
        "status": ONEBOT_FAILED_STATUS,
        "retcode": retcode,
        "msg": message,
        "wording": message,
        "data": None,
        "echo": echo,
    }


def _as_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _ratio(value: str) -> float:
    parsed = float(value)
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("ratio must be between 0 and 1")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        signal_value = getattr(signal, signame, None)
        if signal_value is None:
            continue
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(signal_value, stop_event.set)


def parse_args() -> LoadConfig:
    parser = argparse.ArgumentParser(
        description="Fake OneBot reverse WebSocket client for AstrBot load tests.",
    )
    parser.add_argument("--url", required=True, help="AstrBot OneBot WS server URL.")
    parser.add_argument("--token", default="", help="Bearer token, if AstrBot requires one.")
    parser.add_argument("--self-id", type=_positive_int, default=DEFAULT_SELF_ID)
    parser.add_argument("--rate", type=_non_negative_float, default=10.0)
    parser.add_argument("--duration", type=_non_negative_float, default=60.0)
    parser.add_argument("--burst", type=_non_negative_int, default=0)
    parser.add_argument("--groups", type=_positive_int, default=3)
    parser.add_argument("--users", type=_positive_int, default=50)
    parser.add_argument("--group-id", type=_positive_int, default=None)
    parser.add_argument("--user-id", type=_positive_int, default=None)
    parser.add_argument("--bad-ratio", type=_ratio, default=0.03)
    parser.add_argument("--notice-ratio", type=_ratio, default=0.01)
    parser.add_argument("--slow-action-ratio", type=_ratio, default=0.0)
    parser.add_argument("--fail-action-ratio", type=_ratio, default=0.0)
    parser.add_argument("--action-timeout-ratio", type=_ratio, default=0.0)
    parser.add_argument("--slow-action-delay", type=_non_negative_float, default=2.0)
    parser.add_argument("--message", default=DEFAULT_MESSAGE)
    parser.add_argument("--bad-message", default=DEFAULT_BAD_MESSAGE)
    args = parser.parse_args()
    return LoadConfig(
        url=args.url,
        token=args.token,
        self_id=args.self_id,
        rate=args.rate,
        duration_seconds=args.duration,
        burst=args.burst,
        groups=args.groups,
        users_per_group=args.users,
        group_id=args.group_id,
        user_id=args.user_id,
        bad_ratio=args.bad_ratio,
        notice_ratio=args.notice_ratio,
        slow_action_ratio=args.slow_action_ratio,
        fail_action_ratio=args.fail_action_ratio,
        action_timeout_ratio=args.action_timeout_ratio,
        slow_action_delay_seconds=args.slow_action_delay,
        message=args.message,
        bad_message=args.bad_message,
    )


def main() -> int:
    return asyncio.run(run(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
