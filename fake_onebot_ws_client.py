from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import random
import signal
import time
import webbrowser
from dataclasses import dataclass, field
from itertools import count
from pathlib import Path
from typing import Any


DEFAULT_SELF_ID = 123456789
DEFAULT_MESSAGE = "hello from onebot qq input load test"
DEFAULT_BAD_MESSAGE = "blocked text from onebot qq input load test"
DEFAULT_TIMEOUT_SECONDS = 15.0
STATS_INTERVAL_SECONDS = 1.0
WEBUI_DEFAULT_HOST = "127.0.0.1"
WEBUI_DEFAULT_PORT = 8765
WEBUI_HTML_PATH = Path(__file__).with_name("webui.html")
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
    concurrency: int
    send_interval_seconds: float
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

    def snapshot(self) -> dict[str, int | float]:
        elapsed = self.elapsed_seconds()
        return {
            "elapsed_seconds": elapsed,
            "sent_events": self.sent_events,
            "sent_messages": self.sent_messages,
            "sent_notices": self.sent_notices,
            "sent_bad_messages": self.sent_bad_messages,
            "received_actions": self.received_actions,
            "responded_actions": self.responded_actions,
            "failed_actions": self.failed_actions,
            "slow_actions": self.slow_actions,
            "dropped_actions": self.dropped_actions,
            "unknown_actions": self.unknown_actions,
            "send_errors": self.send_errors,
            "receive_errors": self.receive_errors,
            "event_rate": self.sent_events / elapsed,
        }


@dataclass(slots=True)
class WebRunState:
    task: asyncio.Task[int] | None = None
    stats: RunStats | None = None
    stop_event: asyncio.Event | None = None
    config: LoadConfig | None = None
    error: str = ""
    exit_code: int | None = None
    stopped_by_user: bool = False

    def running(self) -> bool:
        return self.task is not None and not self.task.done()


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
            "nickname": "onebot-qq-input-load-bot",
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


async def run(
    config: LoadConfig,
    *,
    stats: RunStats | None = None,
    stop_event: asyncio.Event | None = None,
    print_stats: bool = True,
    install_signals: bool = True,
) -> int:
    try:
        import aiohttp
    except ImportError:
        print(
            "Missing dependency: aiohttp. "
            "Install with: python -m pip install -r tools/loadtest/requirements.txt"
        )
        return 2

    stats = stats or RunStats()
    stop_event = stop_event or asyncio.Event()
    if install_signals:
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
            ]
            if print_stats:
                tasks.append(asyncio.create_task(_print_stats(stats, stop_event)))
            try:
                await _wait_for_completion(config, stop_event)
            finally:
                stop_event.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                if print_stats:
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
        await _send_burst_events(ws, factory, config, stats, stop_event)

    if config.rate <= 0:
        if config.duration_seconds <= 0:
            stop_event.set()
        return

    interval = 1.0 / config.rate
    next_send_at = time.monotonic()
    while not stop_event.is_set():
        now = time.monotonic()
        if now < next_send_at:
            await asyncio.sleep(next_send_at - now)
        await _send_one_event(ws, factory, config, stats)
        next_send_at += interval


async def _send_burst_events(
    ws: Any,
    factory: PayloadFactory,
    config: LoadConfig,
    stats: RunStats,
    stop_event: asyncio.Event,
) -> None:
    worker_count = min(max(config.concurrency, 1), max(config.burst, 1))
    base_count, remainder = divmod(config.burst, worker_count)
    tasks = [
        asyncio.create_task(
            _send_burst_worker(
                ws,
                factory,
                config,
                stats,
                stop_event,
                base_count + (1 if index < remainder else 0),
            )
        )
        for index in range(worker_count)
    ]
    await asyncio.gather(*tasks)


async def _send_burst_worker(
    ws: Any,
    factory: PayloadFactory,
    config: LoadConfig,
    stats: RunStats,
    stop_event: asyncio.Event,
    event_count: int,
) -> None:
    for index in range(event_count):
        if stop_event.is_set():
            return
        await _send_one_event(ws, factory, config, stats)
        if config.send_interval_seconds > 0 and index < event_count - 1:
            await asyncio.sleep(config.send_interval_seconds)


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


async def run_webui(host: str, port: int, *, open_browser: bool = True) -> int:
    try:
        from aiohttp import web
    except ImportError:
        print(
            "Missing dependency: aiohttp. "
            "Install with: python -m pip install -r tools/loadtest/requirements.txt"
        )
        return 2

    state = WebRunState()
    app = web.Application()
    app["state"] = state
    app.router.add_get("/", _handle_webui_index)
    app.router.add_post("/api/start", _handle_webui_start)
    app.router.add_post("/api/stop", _handle_webui_stop)
    app.router.add_get("/api/status", _handle_webui_status)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    try:
        await site.start()
    except OSError as exc:
        await runner.cleanup()
        if exc.errno in {98, 10048}:
            print(
                f"WebUI port {host}:{port} is already in use. "
                f"Use --webui-port {port + 1} or stop the existing WebUI process."
            )
            return 2
        raise
    browser_host = "localhost" if host in {"127.0.0.1", "0.0.0.0"} else host
    browser_url = f"http://{browser_host}:{port}/"
    print(f"WebUI running in the foreground at {browser_url}")
    print("Press Ctrl+C in this terminal to stop it.")
    if open_browser:
        webbrowser.open(browser_url)

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    try:
        await stop_event.wait()
    finally:
        if state.running() and state.stop_event is not None:
            state.stop_event.set()
            if state.task is not None:
                state.stopped_by_user = True
                state.task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await state.task
        await runner.cleanup()
    return 0


async def _handle_webui_index(request: Any) -> Any:
    from aiohttp import web

    del request
    if not WEBUI_HTML_PATH.exists():
        return web.Response(
            status=500,
            text=f"Missing WebUI file: {WEBUI_HTML_PATH}",
            content_type="text/plain",
        )
    return web.FileResponse(WEBUI_HTML_PATH)


async def _handle_webui_start(request: Any) -> Any:
    from aiohttp import web

    state: WebRunState = request.app["state"]
    if state.running():
        return web.json_response(_webui_status_payload(state), status=409)

    try:
        payload = await request.json()
    except ValueError:
        return web.json_response({"error": "请求 JSON 格式无效"}, status=400)

    try:
        config = _config_from_web_payload(payload)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    stats = RunStats()
    stop_event = asyncio.Event()
    state.task = asyncio.create_task(
        run(
            config,
            stats=stats,
            stop_event=stop_event,
            print_stats=False,
            install_signals=False,
        )
    )
    state.stats = stats
    state.stop_event = stop_event
    state.config = config
    state.error = ""
    state.exit_code = None
    state.stopped_by_user = False
    state.task.add_done_callback(lambda task: _finish_webui_task(state, task))
    return web.json_response(_webui_status_payload(state))


async def _handle_webui_stop(request: Any) -> Any:
    from aiohttp import web

    state: WebRunState = request.app["state"]
    if state.running() and state.stop_event is not None:
        state.stopped_by_user = True
        state.stop_event.set()
        if state.task is not None:
            state.task.cancel()
        await asyncio.sleep(0)
    return web.json_response(_webui_status_payload(state))


async def _handle_webui_status(request: Any) -> Any:
    from aiohttp import web

    state: WebRunState = request.app["state"]
    return web.json_response(_webui_status_payload(state))


def _finish_webui_task(state: WebRunState, task: asyncio.Task[int]) -> None:
    try:
        state.exit_code = task.result()
    except asyncio.CancelledError:
        if state.stopped_by_user:
            state.exit_code = 0
            state.error = ""
        elif not state.error:
            state.exit_code = None
            state.error = "任务已取消"
    except Exception as exc:
        state.exit_code = 1
        state.error = str(exc)


def _webui_status_payload(state: WebRunState) -> dict[str, Any]:
    config = state.config
    stats = state.stats
    return {
        "running": state.running(),
        "error": state.error,
        "exit_code": state.exit_code,
        "config": _public_config(config) if config is not None else None,
        "stats": stats.snapshot() if stats is not None else None,
    }


def _public_config(config: LoadConfig) -> dict[str, Any]:
    return {
        "url": config.url,
        "token_configured": bool(config.token),
        "self_id": config.self_id,
        "rate": config.rate,
        "duration": config.duration_seconds,
        "count": config.burst,
        "concurrency": config.concurrency,
        "interval": config.send_interval_seconds,
        "groups": config.groups,
        "users": config.users_per_group,
        "group_id": config.group_id,
        "user_id": config.user_id,
        "bad_ratio": config.bad_ratio,
        "notice_ratio": config.notice_ratio,
        "slow_action_ratio": config.slow_action_ratio,
        "fail_action_ratio": config.fail_action_ratio,
        "action_timeout_ratio": config.action_timeout_ratio,
        "slow_action_delay": config.slow_action_delay_seconds,
        "message": config.message,
        "bad_message": config.bad_message,
    }


def _config_from_web_payload(payload: Any) -> LoadConfig:
    if not isinstance(payload, dict):
        raise ValueError("请求体必须是 JSON 对象")

    url = _payload_str(payload, "url", "").strip()
    if not url:
        raise ValueError("OneBot WS URL 不能为空")

    return LoadConfig(
        url=url,
        token=_payload_str(payload, "token", ""),
        self_id=_payload_int(payload, "self_id", default=DEFAULT_SELF_ID, minimum=1),
        rate=_payload_float(payload, "rate", default=0.0, minimum=0.0),
        duration_seconds=_payload_float(payload, "duration", default=0.0, minimum=0.0),
        burst=_payload_int(payload, "count", default=0, minimum=0),
        concurrency=_payload_int(payload, "concurrency", default=1, minimum=1),
        send_interval_seconds=_payload_float(payload, "interval", default=0.0, minimum=0.0),
        groups=_payload_int(payload, "groups", default=3, minimum=1),
        users_per_group=_payload_int(payload, "users", default=50, minimum=1),
        group_id=_payload_optional_int(payload, "group_id", minimum=1),
        user_id=_payload_optional_int(payload, "user_id", minimum=1),
        bad_ratio=_payload_float(payload, "bad_ratio", default=0.03, minimum=0.0, maximum=1.0),
        notice_ratio=_payload_float(payload, "notice_ratio", default=0.01, minimum=0.0, maximum=1.0),
        slow_action_ratio=_payload_float(
            payload,
            "slow_action_ratio",
            default=0.0,
            minimum=0.0,
            maximum=1.0,
        ),
        fail_action_ratio=_payload_float(
            payload,
            "fail_action_ratio",
            default=0.0,
            minimum=0.0,
            maximum=1.0,
        ),
        action_timeout_ratio=_payload_float(
            payload,
            "action_timeout_ratio",
            default=0.0,
            minimum=0.0,
            maximum=1.0,
        ),
        slow_action_delay_seconds=_payload_float(
            payload,
            "slow_action_delay",
            default=2.0,
            minimum=0.0,
        ),
        message=_payload_str(payload, "message", DEFAULT_MESSAGE),
        bad_message=_payload_str(payload, "bad_message", DEFAULT_BAD_MESSAGE),
    )


def _payload_str(payload: dict[str, Any], name: str, default: str) -> str:
    value = payload.get(name, default)
    if value is None:
        return default
    return str(value)


def _payload_int(
    payload: dict[str, Any],
    name: str,
    *,
    default: int,
    minimum: int,
) -> int:
    value = payload.get(name, default)
    if value in (None, ""):
        value = default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是整数") from exc
    if parsed < minimum:
        raise ValueError(f"{name} 不能小于 {minimum}")
    return parsed


def _payload_optional_int(
    payload: dict[str, Any],
    name: str,
    *,
    minimum: int,
) -> int | None:
    value = payload.get(name)
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是整数") from exc
    if parsed < minimum:
        raise ValueError(f"{name} 不能小于 {minimum}")
    return parsed


def _payload_float(
    payload: dict[str, Any],
    name: str,
    *,
    default: float,
    minimum: float,
    maximum: float | None = None,
) -> float:
    value = payload.get(name, default)
    if value in (None, ""):
        value = default
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是数字") from exc
    if parsed < minimum:
        raise ValueError(f"{name} 不能小于 {minimum:g}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} 不能大于 {maximum:g}")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OneBot reverse WebSocket QQ input load-test client for AstrBot.",
    )
    parser.add_argument("--webui", action="store_true", help="Run the local WebUI.")
    parser.add_argument("--webui-host", default=WEBUI_DEFAULT_HOST)
    parser.add_argument("--webui-port", type=_positive_int, default=WEBUI_DEFAULT_PORT)
    parser.add_argument("--no-open", action="store_true", help="Do not open the browser for WebUI.")
    parser.add_argument("--url", default="", help="AstrBot OneBot WS server URL.")
    parser.add_argument("--token", default="", help="Bearer token, if AstrBot requires one.")
    parser.add_argument("--self-id", type=_positive_int, default=DEFAULT_SELF_ID)
    parser.add_argument("--rate", type=_non_negative_float, default=10.0)
    parser.add_argument("--duration", type=_non_negative_float, default=60.0)
    parser.add_argument("--burst", "--count", dest="burst", type=_non_negative_int, default=0)
    parser.add_argument("--concurrency", type=_positive_int, default=1)
    parser.add_argument("--interval", type=_non_negative_float, default=0.0)
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
    return parser.parse_args()


def _config_from_args(args: argparse.Namespace) -> LoadConfig:
    return LoadConfig(
        url=args.url,
        token=args.token,
        self_id=args.self_id,
        rate=args.rate,
        duration_seconds=args.duration,
        burst=args.burst,
        concurrency=args.concurrency,
        send_interval_seconds=args.interval,
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
    args = parse_args()
    if args.webui:
        return asyncio.run(
            run_webui(
                args.webui_host,
                args.webui_port,
                open_browser=not args.no_open,
            )
        )
    if not args.url:
        raise SystemExit("--url is required unless --webui is used")
    return asyncio.run(run(_config_from_args(args)))


if __name__ == "__main__":
    raise SystemExit(main())
