# discord-ai-bot

A Discord bot that answers prompts with a **fully local LLM** via
[Ollama](https://ollama.com). Nothing leaves the machine: no OpenAI/Anthropic API
calls, no per-token cost, and conversations stay private to the server it runs on.

Built and deployed on my Mac mini (which also runs the Ollama models).

## What it does

- `!ai <prompt>` in any channel the bot can read → the prompt goes to a local Ollama
  model, the answer comes back as a reply.
- **Per-message model switching**: `!ai @mistral explain VLANs` routes that one prompt
  to a different local model without any config change.
- **Long answers are chunked** to respect Discord's 2,000-character message limit,
  split on line boundaries so code blocks and paragraphs don't get mangled.
- Graceful failure: if Ollama is down, the user gets an error reply instead of silence.

## Stack

- **Node.js** with [discord.js](https://discord.js.org) v14 (Gateway intents: guilds,
  messages, message content)
- **Ollama** REST API (`/api/generate`) for inference — default model `llama3.2:3b`
- `dotenv` for configuration — the token is never in code

## Run it

```bash
cp .env.example .env       # add your Discord bot token
npm install
npm start
```

Requires [Ollama](https://ollama.com) running locally with at least one model pulled
(`ollama pull llama3.2:3b`).

## Design notes

This bot is deliberately small (~100 lines). The interesting decision is architectural:
using local inference makes a hosted bot free to run 24/7 and private by default —
the right trade-off for a home-server utility where response latency matters less
than cost and privacy.
