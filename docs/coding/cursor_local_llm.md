# Using Cursor with Local LLM (Osaurus)

Use Cursor with a local LLM served by [Osaurus](https://osaurus.ai/) so you can code without internet (e.g. on a plane). Osaurus exposes an OpenAI-compatible API on **http://127.0.0.1:1337**.

## Prerequisites

1. **Osaurus** installed and running (e.g. `osaurus serve --port 1337` or start from the app).
2. At least one model loaded in Osaurus (e.g. a small code model).

## Cursor configuration

### Option A: Cursor Settings UI (recommended)

1. Open **Cursor Settings** (gear icon in the bottom-left).
2. Go to **Models**.
3. Under **OpenAI API Keys** (or **Add model** / custom provider):
   - **Override Base URL**: `http://127.0.0.1:1337/v1`
   - **API Key**: any placeholder (e.g. `not-needed` or `local`) — Osaurus often does not require a key.
4. Choose the model that matches what Osaurus is serving (e.g. the model name shown in Osaurus).
5. Restart Cursor if the model list or base URL does not update.

### Option B: Workspace settings

This repo’s `.vscode/settings.json` includes:

```json
"cursor.openai.baseUrl": "http://127.0.0.1:1337/v1"
```

If your Cursor version respects this key, the override is applied for this workspace. Otherwise use Option A.

## Before going offline

1. Start Osaurus and load your model.
2. In Cursor, set the base URL and model as above.
3. Send a test message in Cursor Chat to confirm the local model responds.

## Notes

- Same base URL (`http://127.0.0.1:1337/v1`) is used by this project’s AI migration tools via `SL_LLM_BASE_URL` (see [LLM-based translation](llm_based_translation.md)).
- Some Cursor features (e.g. certain tab completions) may behave differently with a local model.
