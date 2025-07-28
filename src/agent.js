import { readFileSync } from "fs";
import yaml from "yaml";

function parseInterval(str) {
  // Supports s, m, h
  if (str.endsWith("ms")) return parseInt(str);
  if (str.endsWith("s")) return parseInt(str) * 1000;
  if (str.endsWith("m")) return parseInt(str) * 60 * 1000;
  if (str.endsWith("h")) return parseInt(str) * 60 * 60 * 1000;
  throw new Error("Invalid interval: " + str);
}

const config = yaml.parse(readFileSync("./config.yaml", "utf8"));

function randomValue() {
  // Generate a random float value for testing
  return (Math.random() * 100).toFixed(2);
}

function getAlignedTimestamp(intervalMs) {
  const now = new Date();
  if (intervalMs === 60 * 60 * 1000) {
    // 1 hour: xx:00:00
    now.setMinutes(0, 0, 0);
    return now.toISOString();
  }
  if (intervalMs === 60 * 1000) {
    // 1 minute: xx:xx:00
    now.setSeconds(0, 0);
    return now.toISOString();
  }
  return now.toISOString();
}

function sendData(client, intervalMs) {
  const body = {
    timestamp: getAlignedTimestamp(intervalMs),
    value: Number(randomValue()),
    is_valid: true,
  };
  fetch(config.api_url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${client.secret}`,
      "X-Client-ID": client.client_id,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  })
    .then((res) => res.text())
    .then((text) => {
      console.log(`[${client.client_id}] Sent: ${body.timestamp} - ${body.value}`);
    })
    .catch((err) => {
      console.error(`[${client.client_id}] Error:`, err);
    });
}

function getNextAlignedDelay(intervalMs) {
  const now = new Date();
  if (intervalMs === 60 * 60 * 1000) {
    // 1 hour
    // Next hour
    const next = new Date(now);
    next.setMinutes(0, 0, 0);
    next.setHours(now.getMinutes() === 0 && now.getSeconds() === 0 && now.getMilliseconds() === 0 ? now.getHours() : now.getHours() + 1);
    return next - now;
  }
  if (intervalMs === 60 * 1000) {
    // 1 minute
    // Next minute
    const next = new Date(now);
    next.setSeconds(0, 0);
    next.setMinutes(now.getSeconds() === 0 && now.getMilliseconds() === 0 ? now.getMinutes() : now.getMinutes() + 1);
    return next - now;
  }
  return 0;
}

for (const client of config.clients) {
  const intervalMs = parseInterval(client.interval);
  const alignedDelay = getNextAlignedDelay(intervalMs);
  setTimeout(() => {
    sendData(client, intervalMs);
    setInterval(() => sendData(client, intervalMs), intervalMs);
    console.log(`Started agent for ${client.client_id} every ${client.interval} (aligned)`);
  }, alignedDelay);
}
