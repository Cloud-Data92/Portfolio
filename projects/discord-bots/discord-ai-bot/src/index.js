const { Client, GatewayIntentBits, Events } = require('discord.js');
require('dotenv').config();

const OLLAMA_URL = process.env.OLLAMA_URL || 'http://localhost:11434';
const DEFAULT_MODEL = process.env.OLLAMA_MODEL || 'llama3.2:3b';
const BOT_PREFIX = process.env.BOT_PREFIX || '!ai';

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

// Query Ollama API
async function queryOllama(prompt, model = DEFAULT_MODEL) {
  const response = await fetch(`${OLLAMA_URL}/api/generate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model, prompt, stream: false }),
  });

  if (!response.ok) {
    throw new Error(`Ollama error: ${response.status}`);
  }

  const data = await response.json();
  return data.response;
}

// Split long messages for Discord's 2000 char limit
function splitMessage(text, maxLength = 1900) {
  const chunks = [];
  while (text.length > 0) {
    if (text.length <= maxLength) {
      chunks.push(text);
      break;
    }
    let splitAt = text.lastIndexOf('\n', maxLength);
    if (splitAt === -1) splitAt = maxLength;
    chunks.push(text.substring(0, splitAt));
    text = text.substring(splitAt).trimStart();
  }
  return chunks;
}

client.once(Events.ClientReady, (c) => {
  console.log(`Bot ready as ${c.user.tag}`);
  console.log(`Using model: ${DEFAULT_MODEL}`);
  console.log(`Prefix: ${BOT_PREFIX}`);
});

client.on(Events.MessageCreate, async (message) => {
  if (message.author.bot) return;
  if (!message.content.startsWith(BOT_PREFIX)) return;

  const prompt = message.content.slice(BOT_PREFIX.length).trim();
  if (!prompt) {
    await message.reply('Give me a prompt after `!ai`. Example: `!ai explain recursion`');
    return;
  }

  // Check for model switch: !ai @mistral prompt
  let model = DEFAULT_MODEL;
  const modelMatch = prompt.match(/^@(\S+)\s+(.+)/s);
  let actualPrompt = prompt;
  if (modelMatch) {
    model = modelMatch[1];
    actualPrompt = modelMatch[2];
  }

  await message.channel.sendTyping();

  try {
    const response = await queryOllama(actualPrompt, model);
    const chunks = splitMessage(response);
    for (const chunk of chunks) {
      await message.reply(chunk);
    }
  } catch (error) {
    console.error('Ollama query failed:', error);
    await message.reply(`Error querying ${model}: ${error.message}`);
  }
});

const token = process.env.DISCORD_TOKEN;
if (!token) {
  console.error('DISCORD_TOKEN not set in .env file');
  console.error('1. Go to https://discord.com/developers/applications');
  console.error('2. Create a new application → Bot → Copy token');
  console.error('3. Add to .env: DISCORD_TOKEN=your_token_here');
  process.exit(1);
}

client.login(token);
