# Telegram End-Report Setup

This guide lets you receive final test/load reports directly in your Telegram app.

## 1) Create a Telegram Bot

1. Open Telegram and chat with `@BotFather`.
2. Send `/newbot` and follow the prompts.
3. Copy the generated bot token.

You will get something like:

`1234567890:AA...your_token...`

## 2) Get Your Chat ID

### Option A: Personal chat
1. Open a chat with your bot and send any message (for example: `hello`).
2. Visit:

`https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`

3. Find `chat.id` in the JSON response.

### Option B: Group chat
1. Add the bot to the group.
2. Send one message in the group.
3. Call `getUpdates` URL above and use the group's `chat.id`.

## 3) Set Environment Variables

### PowerShell (current session)

```powershell
$env:TELEGRAM_BOT_TOKEN="<YOUR_BOT_TOKEN>"
$env:TELEGRAM_CHAT_ID="<YOUR_CHAT_ID>"
```

### PowerShell (persist for your user)

```powershell
setx TELEGRAM_BOT_TOKEN "<YOUR_BOT_TOKEN>"
setx TELEGRAM_CHAT_ID "<YOUR_CHAT_ID>"
```

Open a new terminal after `setx`.

## 4) Send End Report from Scripts

## Dashboard automatic flow (no extra command)

After Telegram env vars are configured and backend is running:

1. Open `dashboard/index.html`.
2. Click `Submit Batch (10)`.
3. The dashboard now waits for those submitted tasks to reach terminal state.
4. Once all are done, it automatically sends a completion report to Telegram.

You will see UI status like `Processing X/Y tasks...` then `Report sent to Telegram`.

## Load test report to Telegram

```powershell
python load_test.py --tasks 120 --duration 120
```

If env vars are set, the final summary is sent automatically.

You can also pass explicit credentials:

```powershell
python load_test.py --tasks 120 --duration 120 --telegram-token "<TOKEN>" --telegram-chat-id "<CHAT_ID>"
```

## System test report to Telegram

```powershell
python test_system.py
```

`test_system.py` now sends the final summary automatically when `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are configured.

## 5) Validate Bot Delivery

If messages are not received:

1. Verify bot token is correct.
2. Ensure you sent at least one message to the bot/chat first.
3. Re-run:
   `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
4. Confirm `chat.id` matches your target chat.

## Notes for Realistic Failure Testing

The project now includes heavier/slower API task types to better emulate real-world failure behavior:

- `fetch_large_photos` (large JSON payload parsing)
- `fetch_slow_httpbin` (slow API likely to timeout with low timeout settings)

These increase retry/dead-letter activity and help validate worker resilience and recovery paths.
