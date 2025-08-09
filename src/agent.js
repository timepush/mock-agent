import { readFileSync } from "fs";
import yaml from "yaml";

function parseInterval(str) {
  if (str.endsWith("ms")) return parseInt(str);
  if (str.endsWith("s")) return parseInt(str) * 1000;
  if (str.endsWith("m")) return parseInt(str) * 60 * 1000;
  if (str.endsWith("h")) return parseInt(str) * 60 * 60 * 1000;
  throw new Error("Invalid interval: " + str);
}

const config = yaml.parse(readFileSync("./config.yaml", "utf8"));

function randomValue() {
  return (Math.random() * 100).toFixed(2);
}

function getAlignedTimestamp(intervalMs) {
  const now = new Date();
  if (intervalMs === 60 * 60 * 1000) {
    now.setMinutes(0, 0, 0);
  } else if (intervalMs === 60 * 1000) {
    now.setSeconds(0, 0);
  }
  return now.toISOString();
}

function getNextAlignedDelay(intervalMs) {
  const now = new Date();
  if (intervalMs === 60 * 60 * 1000) {
    const next = new Date(now);
    next.setMinutes(0, 0, 0);
    next.setHours(now.getMinutes() === 0 && now.getSeconds() === 0 && now.getMilliseconds() === 0 ? now.getHours() : now.getHours() + 1);
    return next - now;
  }
  if (intervalMs === 60 * 1000) {
    const next = new Date(now);
    next.setSeconds(0, 0);
    next.setMinutes(now.getSeconds() === 0 && now.getMilliseconds() === 0 ? now.getMinutes() : now.getMinutes() + 1);
    return next - now;
  }
  return 0;
}

async function sendData(client, intervalMs) {
  const body = {
    timestamp: getAlignedTimestamp(intervalMs),
    value: Number(randomValue()),
    is_valid: true,
  };
  const url = client.api_url || config.api_url;
  await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${client.secret}`,
      "X-Client-ID": client.client_id,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  console.log(`[${client.client_id}] Sent: ${body.timestamp} - ${body.value}`);
}

// --- NDJSON STREAMING ---
async function startNdjsonStream(client, intervalMs) {
  const stream = new TransformStream();
  const writer = stream.writable.getWriter();

  // Start the request immediately
  const url = client.api_url || config.api_url;
  fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${client.secret}`,
      "X-Client-ID": client.client_id,
      "Content-Type": "application/x-ndjson",
    },
    body: stream.readable,
  }).catch((err) => console.error(`[${client.client_id}] Stream error:`, err));

  const writeRecord = async () => {
    const record = {
      timestamp: getAlignedTimestamp(intervalMs),
      value: Number(randomValue()),
      is_valid: true,
    };
    const line = JSON.stringify(record) + "\n";
    await writer.write(new TextEncoder().encode(line));
    console.log(`[${client.client_id}] Streamed: ${record.timestamp} - ${record.value}`);
  };

  // Align start
  const alignedDelay = getNextAlignedDelay(intervalMs);
  setTimeout(() => {
    writeRecord();
    setInterval(writeRecord, intervalMs);
  }, alignedDelay);
}

// --- Decide per client ---
for (const client of config.clients) {
  const intervalMs = parseInterval(client.interval);

  // Use NDJSON if configured (e.g., via extra YAML field `mode: ndjson`)
  if (client.mode === "ndjson") {
    startNdjsonStream(client, intervalMs);
    console.log(`Started NDJSON stream for ${client.client_id} every ${client.interval} `);
  } else {
    const alignedDelay = getNextAlignedDelay(intervalMs);
    setTimeout(() => {
      sendData(client, intervalMs);
      setInterval(() => sendData(client, intervalMs), intervalMs);
      console.log(`Started agent for ${client.client_id} every ${client.interval} `);
    }, alignedDelay);
  }
}
