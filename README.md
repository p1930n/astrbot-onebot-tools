# OneBot QQ Input Load Client

This tool is a OneBot reverse WebSocket client for AstrBot load tests. It
simulates QQ group message and notice input, and responds to OneBot action
requests from AstrBot.

The client connects as a OneBot reverse WebSocket universal client with
`X-Client-Role: Universal` and `X-Self-ID` headers.

It is a development load-test tool. It is not part of either plugin runtime and
should not be bundled into plugin release packages.

## Install

```powershell
python -m pip install -r requirements.txt
```

## Run WebUI

Start the local WebUI in the foreground:

```powershell
python fake_onebot_ws_client.py --webui
```

The command opens the WebUI in your browser and keeps running in the terminal.
Press `Ctrl+C` in that terminal to stop it.

Use a custom WebUI port when needed:

```powershell
python fake_onebot_ws_client.py --webui --webui-port 8765
```

If the port is already in use, stop the existing terminal process or choose a
different port:

```powershell
python fake_onebot_ws_client.py --webui --webui-port 8766
```

If you do not want to open a browser automatically:

```powershell
python fake_onebot_ws_client.py --webui --no-open
```

The WebUI exposes these controls:

- connection: OneBot WS URL, Token, self_id
- message source: QQ user_id, QQ group_id
- payloads: message content, bad message content
- burst controls: count, interval, concurrency
- traffic mix and action behavior: rate, duration, groups, users, bad_ratio,
  notice_ratio, slow_action_ratio, fail_action_ratio, action_timeout_ratio,
  slow_action_delay

The right side shows live status, metrics, recent logs, and the current
sanitized runtime configuration. Logs keep the latest 200 entries and do not
echo the bearer token.

## Run CLI

Replace the URL with the OneBot WebSocket server URL exposed by AstrBot.

```powershell
python fake_onebot_ws_client.py `
  --url ws://127.0.0.1:6198/ws `
  --rate 10 `
  --duration 120 `
  --groups 3 `
  --users 50 `
  --bad-ratio 0.03 `
  --notice-ratio 0.01
```

To target an existing AstrBot group policy, pin the source group id:

```powershell
python fake_onebot_ws_client.py `
  --url ws://127.0.0.1:6198/ws `
  --group-id 123456789 `
  --bad-ratio 0.3 `
  --bad-message "text that should match your configured rule"
```

If AstrBot requires a token:

```powershell
python fake_onebot_ws_client.py `
  --url ws://127.0.0.1:6198/ws `
  --token YOUR_TOKEN
```

For burst-style sends, `--count` is an alias of `--burst`, `--interval` adds a
delay between sends, and `--concurrency` controls burst concurrency.

## Useful Scenarios

Baseline:

```powershell
python fake_onebot_ws_client.py --url ws://127.0.0.1:6198/ws --rate 10 --duration 120
```

Short peak:

```powershell
python fake_onebot_ws_client.py --url ws://127.0.0.1:6198/ws --rate 100 --duration 60
```

Burst:

```powershell
python fake_onebot_ws_client.py --url ws://127.0.0.1:6198/ws --count 1000 --interval 0.01 --concurrency 50 --rate 0 --duration 5
```

Slow and failed actions:

```powershell
python fake_onebot_ws_client.py `
  --url ws://127.0.0.1:6198/ws `
  --rate 50 `
  --duration 60 `
  --slow-action-ratio 0.05 `
  --fail-action-ratio 0.02 `
  --action-timeout-ratio 0.01
```

## AstrBot Checks

During a run, inspect the plugin metrics from AstrBot:

```text
.cf metrics
/glog metrics
```

Watch for action timeouts, failed dispatches, SQLite delays, and whether event
counts match the sent payload volume.

## Supported Action Responses

The client responds to these action names:

- `get_login_info`
- `send_group_msg`
- `send_group_forward_msg`
- `delete_msg`
- `set_group_ban`
- `upload_group_file`
- `get_group_info`
- `get_group_info_ex`
- `get_group_member_list`
- `set_group_name`
- `set_group_portrait`

Unknown actions return a failed OneBot-style response with `retcode=1404`.
