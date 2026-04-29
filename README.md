# Fake OneBot WebSocket Load Client

This tool simulates a NapCat / aiocqhttp reverse WebSocket client that connects
to AstrBot, sends OneBot v11 group message and notice payloads, and responds to
OneBot action requests from AstrBot.

The client connects as a OneBot reverse WebSocket universal client with
`X-Client-Role: Universal` and `X-Self-ID` headers.

It is a development load-test tool. It is not part of either plugin runtime and
should not be bundled into plugin release packages.

## Install

```powershell
python -m pip install -r requirements.txt
```

## Run

Replace the URL with the OneBot WebSocket server URL exposed by AstrBot.

```powershell
python fake_onebot_ws_client.py `
  --url ws://127.0.0.1:6199/ws `
  --rate 10 `
  --duration 120 `
  --groups 3 `
  --users 50 `
  --bad-ratio 0.03 `
  --notice-ratio 0.01
```

If AstrBot requires a token:

```powershell
python fake_onebot_ws_client.py `
  --url ws://127.0.0.1:6199/ws `
  --token YOUR_TOKEN
```

## Useful Scenarios

Baseline:

```powershell
python fake_onebot_ws_client.py --url ws://127.0.0.1:6199/ws --rate 10 --duration 120
```

Short peak:

```powershell
python fake_onebot_ws_client.py --url ws://127.0.0.1:6199/ws --rate 100 --duration 60
```

Burst:

```powershell
python fake_onebot_ws_client.py --url ws://127.0.0.1:6199/ws --burst 1000 --rate 0 --duration 5
```

Slow and failed actions:

```powershell
python fake_onebot_ws_client.py `
  --url ws://127.0.0.1:6199/ws `
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

The fake client responds to these action names:

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
