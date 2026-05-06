const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const STATE_FILE = './bot-state.json';

const REDIS_URL   = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const REDIS_KEY   = 'solbot:state';

app.use(express.static(path.join(__dirname, '../public')));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  next();
});

// API: get bot state — try Redis first, fall back to file
app.get('/api/state', async (req, res) => {
  try {
    if (REDIS_URL && REDIS_TOKEN) {
      try {
        const response = await fetch(REDIS_URL + '/get/' + REDIS_KEY, {
          headers: { 'Authorization': 'Bearer ' + REDIS_TOKEN },
        });
        const data = await response.json();
        // Handle all possible Upstash response formats
        const raw = data.result || data.value || (typeof data === 'string' ? data : null);
        if (raw) {
          try {
            let parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
            // Handle double-encoded JSON (string inside string)
            if (typeof parsed === 'string') parsed = JSON.parse(parsed);
            return res.json(parsed);
          } catch(parseErr) {
            if (typeof raw === 'object') return res.json(raw);
          }
        }
      } catch (e) {
        console.error('Redis read failed:', e.message);
      }
    }
    if (fs.existsSync(STATE_FILE)) {
      return res.json(JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')));
    }
    res.json({ running: false, lastAction: 'Bot not started yet.' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`Dashboard server running on port ${PORT}`);
});
