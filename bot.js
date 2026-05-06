const { Connection, Keypair, VersionedTransaction, PublicKey } = require('@solana/web3.js');
const { getAssociatedTokenAddress, getAccount } = require('@solana/spl-token');
const fetch = require('node-fetch');
const bs58 = require('bs58');
const fs = require('fs');

const RPC_URL           = process.env.RPC_URL           || 'https://api.mainnet-beta.solana.com';
const PRIVATE_KEY       = process.env.PRIVATE_KEY;
const COINGECKO_API_KEY = process.env.COINGECKO_API_KEY || '';
const STATE_FILE        = './bot2-state.json';

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
  MAX_POSITIONS:    parseInt(process.env.MAX_POSITIONS    || '4'),
  TOTAL_BUDGET_SOL: parseFloat(process.env.TOTAL_BUDGET   || '1.0'),
  MIN_RESERVE_SOL:  parseFloat(process.env.MIN_RESERVE    || '0.05'),
  STOP_LOSS:        parseFloat(process.env.STOP_LOSS      || '0.08'),
  EXIT_Z_SCORE:     parseFloat(process.env.EXIT_Z_SCORE   || '0.5'),
  BUY_THRESHOLD:    parseFloat(process.env.BUY_THRESHOLD  || '0.005'),
  MAX_TRADE_SOL:    parseFloat(process.env.MAX_TRADE_SOL  || '0.25'),
  SCAN_INTERVAL_MS: parseInt(process.env.SCAN_INTERVAL_MS || '180000'),
};

// ── Redis ─────────────────────────────────────────────────────────────────────
const REDIS_URL   = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const REDIS_KEY   = 'solbot2:state';

// ── Token Registry ─────────────────────────────────────────────────────────────
const TOKENS = {
  SOL:      { mint: 'So11111111111111111111111111111111111111112', decimals: 9 },
  POPCAT:   { mint: '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr', decimals: 9 },
  FARTCOIN: { mint: '9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump', decimals: 6 },
  MEW:      { mint: 'MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5',  decimals: 5 },
  PENGU:    { mint: '2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv', decimals: 6 },
  BONK:     { mint: 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', decimals: 5 },
};

const COINGECKO_IDS = {
  SOL:      'solana',
  POPCAT:   'popcat',
  FARTCOIN: 'fartcoin',
  MEW:      'cat-in-a-dogs-world',
  PENGU:    'pudgy-penguins',
  BONK:     'bonk',
};

const WATCH_TOKENS = ['POPCAT', 'FARTCOIN', 'MEW', 'PENGU', 'BONK'];

// ── State ─────────────────────────────────────────────────────────────────────
let state = {
  running: false,
  wallet: null,
  startingSol: 1.0,
  currentSol: 1.0,
  // Multi-position: keyed by token name
  positions: {},
  // positions[TOKEN] = { amount, entryPrice, entryPriceUsd, solSpent, openedAt }
  trades: [],
  lastAction: 'Initializing...',
  lastCheck: null,
  errors: [],
  signals: {},
  priceHistory: {},
  volumeHistory: {},
  correlationMatrix: {},
  timeWindow: null,
  circuitOpen: false,
  circuitReason: null,
  consecutiveLosses: 0,
  totalPnl: 0,
};

// ── Persistence ───────────────────────────────────────────────────────────────
async function saveState() {
  try { fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2)); } catch(e) {}
  if (!REDIS_URL || !REDIS_TOKEN) return;
  try {
    const slim = Object.assign({}, state, { priceHistory: {}, volumeHistory: {} });
    await fetch(REDIS_URL + '/set/' + REDIS_KEY, {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + REDIS_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify(JSON.stringify(slim)),
    });
  } catch(e) { log('Redis save failed: ' + e.message); }
}

async function loadState() {
  if (REDIS_URL && REDIS_TOKEN) {
    try {
      const res = await fetch(REDIS_URL + '/get/' + REDIS_KEY, {
        headers: { 'Authorization': 'Bearer ' + REDIS_TOKEN },
      });
      const data = await res.json();
      const raw = data.result || data.value;
      if (raw) {
        let parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
        if (typeof parsed === 'string') parsed = JSON.parse(parsed);
        state = { ...state, ...parsed };
        log('State loaded from Redis. Positions: ' + Object.keys(state.positions || {}).join(', ') || 'none');
        return;
      }
    } catch(e) { log('Redis load failed: ' + e.message); }
  }
  try {
    if (fs.existsSync(STATE_FILE)) {
      state = { ...state, ...JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')) };
      log('State loaded from file');
    }
  } catch(e) { log('Fresh state'); }
}

function log(msg) {
  const ts = new Date().toISOString();
  console.log('[' + ts + '] ' + msg);
  state.lastAction = msg;
  state.lastCheck = ts;
}

function addError(msg) {
  state.errors.unshift({ time: new Date().toISOString(), msg });
  if (state.errors.length > 20) state.errors.pop();
}

// ── Circuit Breaker ───────────────────────────────────────────────────────────
function checkCircuit(sol) {
  if (state.circuitOpen) return true;
  const totalValue = getTotalPortfolioSol();
  const drawdown = (state.startingSol - totalValue) / state.startingSol;
  if (drawdown >= 0.5) {
    state.circuitOpen = true;
    state.circuitReason = 'Max drawdown ' + (drawdown*100).toFixed(1) + '%';
    state.running = false;
    log('CIRCUIT BREAKER: ' + state.circuitReason);
    saveState();
    return true;
  }
  if (state.consecutiveLosses >= 4) {
    state.circuitOpen = true;
    state.circuitReason = '4 consecutive losses';
    state.running = false;
    log('CIRCUIT BREAKER: ' + state.circuitReason);
    saveState();
    return true;
  }
  return false;
}

function getTotalPortfolioSol() {
  let total = state.currentSol;
  Object.entries(state.positions || {}).forEach(function([token, pos]) {
    const sig = state.signals && state.signals[token];
    if (sig && sig.price) total += sig.price * pos.amount;
  });
  return total;
}

// ── Solana Helpers ────────────────────────────────────────────────────────────
async function getSolBalance(connection, publicKey) {
  return (await connection.getBalance(publicKey)) / 1e9;
}

async function getTokenBalance(connection, walletPubkey, tokenMint, decimals) {
  try {
    const ata = await getAssociatedTokenAddress(new PublicKey(tokenMint), walletPubkey);
    const account = await getAccount(connection, ata);
    return Number(account.amount) / Math.pow(10, decimals);
  } catch(e) { return 0; }
}

async function getSwapQuote(inputMint, outputMint, amount, slippageBps) {
  slippageBps = slippageBps || 150;
  try {
    const url = 'https://lite-api.jup.ag/swap/v1/quote?inputMint=' + inputMint +
                '&outputMint=' + outputMint + '&amount=' + amount + '&slippageBps=' + slippageBps;
    const res = await fetch(url, { timeout: 15000 });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    return data;
  } catch(e) { log('Quote failed: ' + e.message); return null; }
}

async function executeSwap(connection, wallet, quote) {
  try {
    const res = await fetch('https://lite-api.jup.ag/swap/v1/swap', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        quoteResponse: quote,
        userPublicKey: wallet.publicKey.toString(),
        wrapAndUnwrapSol: true,
        dynamicComputeUnitLimit: true,
        prioritizationFeeLamports: 'auto',
      }),
      timeout: 15000,
    });
    const result = await res.json();
    if (result.error) throw new Error(result.error);
    const tx = VersionedTransaction.deserialize(Buffer.from(result.swapTransaction, 'base64'));
    tx.sign([wallet]);
    const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: true, maxRetries: 3 });
    await connection.confirmTransaction(sig, 'confirmed');
    return sig;
  } catch(e) { log('Swap failed: ' + e.message); addError(e.message); return null; }
}

// ── Price & Signal Helpers ────────────────────────────────────────────────────
function updatePriceHistory(token, price) {
  if (!state.priceHistory[token]) state.priceHistory[token] = [];
  state.priceHistory[token].push({ price, time: Date.now() });
  if (state.priceHistory[token].length > 40) state.priceHistory[token].shift();
}

function getZScore(token) {
  const h = state.priceHistory[token];
  if (!h || h.length < 10) return 0;
  const prices = h.map(x => x.price);
  const mean = prices.reduce((a,b) => a+b, 0) / prices.length;
  const std = Math.sqrt(prices.reduce((s,p) => s + Math.pow(p-mean,2), 0) / prices.length);
  return std === 0 ? 0 : (prices[prices.length-1] - mean) / std;
}

function getMomentum(token) {
  const h = state.priceHistory[token];
  if (!h || h.length < 6) return 0;
  const prices = h.map(x => x.price);
  const rAvg = prices.slice(-3).reduce((a,b) => a+b,0) / 3;
  const oAvg = prices.slice(-6,-3).reduce((a,b) => a+b,0) / 3;
  return oAvg === 0 ? 0 : (rAvg - oAvg) / oAvg;
}

function getVolatility(token) {
  const h = state.priceHistory[token];
  if (!h || h.length < 5) return 0;
  const prices = h.map(x => x.price);
  const mean = prices.reduce((a,b) => a+b,0) / prices.length;
  const variance = prices.reduce((s,p) => s + Math.pow(p-mean,2), 0) / prices.length;
  return mean === 0 ? 0 : Math.sqrt(variance) / mean;
}

function updateVolumeHistory(token, volume) {
  if (!state.volumeHistory[token]) state.volumeHistory[token] = [];
  state.volumeHistory[token].push({ volume, time: Date.now() });
  if (state.volumeHistory[token].length > 20) state.volumeHistory[token].shift();
}

function getVolumeScore(token, vol) {
  const h = state.volumeHistory[token];
  if (!h || h.length < 3) return 0;
  const mean = h.map(x => x.volume).reduce((a,b) => a+b,0) / h.length;
  return mean === 0 ? 0 : (vol - mean) / mean;
}

function buildReversionScore(opts) {
  if (opts.volatility > 0.10) return 0;
  let score = -opts.zScore * 0.01;
  if (opts.momentum < -0.02) score *= 0.3;
  else if (opts.momentum < 0 && opts.momentum > -0.005) score *= 1.3;
  const volMult = Math.max(0.5, 1 + (opts.volumeScore || 0) * 0.4);
  score *= volMult;
  if (opts.sentimentScore != null) score += opts.sentimentScore * 0.002;
  score *= (opts.timeMultiplier || 1);
  return score;
}

// ── Time Window ───────────────────────────────────────────────────────────────
const TIME_WINDOWS = [
  { name: 'Asia Open',     start: 0,  end: 4,  multiplier: 0.85 },
  { name: 'Europe Open',   start: 7,  end: 10, multiplier: 1.1  },
  { name: 'US Pre-market', start: 12, end: 13, multiplier: 1.05 },
  { name: 'US Open',       start: 13, end: 20, multiplier: 1.2  },
  { name: 'US Close',      start: 20, end: 22, multiplier: 1.0  },
  { name: 'Dead Hours',    start: 22, end: 24, multiplier: 0.75 },
];

function getTimeMultiplier() {
  const hour = new Date().getUTCHours();
  for (const w of TIME_WINDOWS) {
    if (hour >= w.start && hour < w.end) {
      state.timeWindow = w.name + ' (' + w.multiplier + 'x)';
      return w.multiplier;
    }
  }
  state.timeWindow = 'Standard (1.0x)';
  return 1.0;
}

// ── Position Sizing (weighted by signal strength) ─────────────────────────────
function calcPositionSize(score, allScores, liquidSol) {
  const totalScore = allScores.reduce((a,b) => a+b, 0);
  if (totalScore === 0) return 0;
  const weight = score / totalScore;
  const budget = Math.min(liquidSol - CONFIG.MIN_RESERVE_SOL, CONFIG.TOTAL_BUDGET_SOL);
  const perPosition = budget / CONFIG.MAX_POSITIONS;
  const weighted = perPosition * (1 + (weight - 1/allScores.length) * 2);
  return Math.min(Math.max(weighted, 0.05), CONFIG.MAX_TRADE_SOL);
}

// ── Main Strategy Loop ────────────────────────────────────────────────────────
async function evaluateStrategy(connection, wallet) {
  if (checkCircuit(state.currentSol)) {
    log('Circuit open: ' + state.circuitReason);
    return;
  }

  const timeMultiplier = getTimeMultiplier();
  if (timeMultiplier < 0.8) { log('Outside active hours. Skipping.'); return; }

  log('Time: ' + state.timeWindow + ' | Positions: ' + Object.keys(state.positions).length + '/' + CONFIG.MAX_POSITIONS);

  // Sync on-chain balances
  state.currentSol = await getSolBalance(connection, wallet.publicKey);

  // Sync existing positions with on-chain balances
  for (const [token, pos] of Object.entries(state.positions)) {
    const info = TOKENS[token];
    if (!info) continue;
    const onChain = await getTokenBalance(connection, wallet.publicKey, info.mint, info.decimals);
    if (onChain < 0.000001) {
      log('Position ' + token + ' has 0 on-chain balance — removing');
      delete state.positions[token];
    } else {
      state.positions[token].amount = onChain;
    }
  }

  // Scan all tokens
  const tokenData = {};
  for (let i = 0; i < WATCH_TOKENS.length; i++) {
    const name = WATCH_TOKENS[i];
    try {
      const cgId = COINGECKO_IDS[name];
      const cgHeaders = COINGECKO_API_KEY ? { 'x-cg-demo-api-key': COINGECKO_API_KEY } : {};
      const priceRes = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=' + cgId + ',solana&vs_currencies=usd', { timeout: 10000, headers: cgHeaders });
      const priceData = await priceRes.json();
      const tokenUsd = priceData[cgId] && priceData[cgId].usd ? priceData[cgId].usd : null;
      const solUsd = priceData['solana'] && priceData['solana'].usd ? priceData['solana'].usd : null;
      if (!tokenUsd || !solUsd) continue;
      const price = tokenUsd / solUsd;

      updatePriceHistory(name, price);
      if (i > 0) await new Promise(r => setTimeout(r, 1200));

      // Fetch full CoinGecko data for volume/sentiment
      let volumeScore = 0, sentimentScore = null;
      try {
        const cgRes = await fetch('https://api.coingecko.com/api/v3/coins/' + cgId + '?localization=false&tickers=false&community_data=true&developer_data=false', { timeout: 10000, headers: cgHeaders });
        const cgData = await cgRes.json();
        if (cgData.market_data) {
          const vol = cgData.market_data.total_volume && cgData.market_data.total_volume.usd ? cgData.market_data.total_volume.usd : 0;
          updateVolumeHistory(name, vol);
          volumeScore = getVolumeScore(name, vol);
          const upPct = cgData.sentiment_votes_up_percentage || 50;
          const change1h = (cgData.market_data.price_change_percentage_1h_in_currency && cgData.market_data.price_change_percentage_1h_in_currency.usd) || 0;
          const change24h = cgData.market_data.price_change_percentage_24h || 0;
          sentimentScore = ((upPct - (100 - upPct)) / 100) * 0.4 + Math.tanh((change1h * 2 + change24h * 0.5) / 10) * 0.6;
        }
      } catch(e) {}

      const zScore = getZScore(name);
      const momentum = getMomentum(name);
      const volatility = getVolatility(name);
      const score = buildReversionScore({ zScore, momentum, volatility, volumeScore, sentimentScore, timeMultiplier });

      tokenData[name] = { price, zScore, momentum, volatility, volumeScore, sentimentScore, score, solUsd };
      log(name + ': z=' + zScore.toFixed(2) + ' mom=' + (momentum*100).toFixed(2) + '% score=' + score.toFixed(5));
    } catch(e) {
      log('Error scanning ' + name + ': ' + e.message);
    }
  }

  // Update signals state
  state.signals = {};
  Object.entries(tokenData).forEach(([k,v]) => {
    state.signals[k] = {
      price: v.price,
      zScore: v.zScore,
      momentum: v.momentum,
      volatility: v.volatility,
      volumeScore: v.volumeScore,
      sentimentScore: v.sentimentScore,
      compositeScore: v.score,
    };
  });

  // ── Exit existing positions ──
  for (const [token, pos] of Object.entries(state.positions)) {
    const sig = tokenData[token];
    if (!sig) continue;

    const pctFromEntry = pos.entryPrice > 0 ? (sig.price - pos.entryPrice) / pos.entryPrice : 0;
    let exitReason = null;

    if (sig.zScore >= CONFIG.EXIT_Z_SCORE) {
      exitReason = 'reverted to mean (z=' + sig.zScore.toFixed(2) + ')';
    } else if (pctFromEntry < -CONFIG.STOP_LOSS) {
      exitReason = 'stop-loss (' + (pctFromEntry*100).toFixed(1) + '%)';
    }

    if (exitReason) {
      log('🔴 SELL ' + token + ': ' + exitReason);
      await executeSell(connection, wallet, token, pos, sig);
    } else {
      log('Holding ' + token + '. P&L: ' + (pctFromEntry >= 0 ? '+' : '') + (pctFromEntry*100).toFixed(2) + '% | z=' + sig.zScore.toFixed(2));
    }
  }

  // ── Enter new positions ──
  const openCount = Object.keys(state.positions).length;
  const availableSlots = CONFIG.MAX_POSITIONS - openCount;
  if (availableSlots <= 0) { log('All ' + CONFIG.MAX_POSITIONS + ' slots filled.'); saveState(); return; }

  // Find best candidates (not already holding, above threshold)
  const candidates = Object.entries(tokenData)
    .filter(([name, d]) => !state.positions[name] && d.score > CONFIG.BUY_THRESHOLD)
    .sort((a, b) => b[1].score - a[1].score)
    .slice(0, availableSlots);

  if (candidates.length === 0) {
    log('No buy signals. ' + openCount + ' positions open.');
    saveState();
    return;
  }

  const allScores = candidates.map(([,d]) => d.score);
  for (const [name, data] of candidates) {
    const posSize = calcPositionSize(data.score, allScores, state.currentSol);
    if (posSize < 0.05) { log('Position size too small for ' + name); continue; }
    log('🟢 BUY ' + name + ' score=' + data.score.toFixed(5) + ' size=' + posSize.toFixed(4) + ' SOL');
    await executeBuy(connection, wallet, name, data, posSize);
  }

  saveState();
}

// ── Trade Execution ───────────────────────────────────────────────────────────
async function executeBuy(connection, wallet, tokenName, tokenData, tradeSOL) {
  try {
    const solBalance = await getSolBalance(connection, wallet.publicKey);
    const actual = Math.min(tradeSOL, solBalance - CONFIG.MIN_RESERVE_SOL);
    if (actual < 0.05) { log('Not enough SOL for ' + tokenName); return; }

    const tradeAmount = Math.floor(actual * 1e9);
    const quote = await getSwapQuote(TOKENS.SOL.mint, TOKENS[tokenName].mint, tradeAmount);
    if (!quote) return;

    const sig = await executeSwap(connection, wallet, quote);
    if (!sig) return;

    const decimals = TOKENS[tokenName].decimals;
    const tokensReceived = parseFloat(quote.outAmount) / Math.pow(10, decimals);
    state.currentSol = await getSolBalance(connection, wallet.publicKey);

    if (!state.positions) state.positions = {};
    state.positions[tokenName] = {
      amount: tokensReceived,
      entryPrice: tokenData.price,
      entryPriceUsd: tokenData.price * tokenData.solUsd,
      solSpent: actual,
      openedAt: new Date().toISOString(),
    };

    state.trades.unshift({ type: 'BUY', token: tokenName, solSpent: actual, tokensReceived, price: tokenData.price, score: tokenData.score, sig, time: new Date().toISOString() });
    if (state.trades.length > 100) state.trades.pop();

    log('✅ Bought ' + tokenName + ' ' + tokensReceived.toFixed(2) + ' tokens. Tx: ' + sig);
    await saveState();
  } catch(e) { log('Buy failed: ' + e.message); addError(e.message); }
}

async function executeSell(connection, wallet, tokenName, pos, tokenData) {
  try {
    const info = TOKENS[tokenName];
    const onChain = await getTokenBalance(connection, wallet.publicKey, info.mint, info.decimals);
    if (onChain < 0.000001) {
      delete state.positions[tokenName];
      saveState();
      return;
    }

    const rawAmount = Math.floor(onChain * Math.pow(10, info.decimals));
    const quote = await getSwapQuote(info.mint, TOKENS.SOL.mint, rawAmount);
    if (!quote) return;

    const sig = await executeSwap(connection, wallet, quote);
    if (!sig) return;

    const solReceived = parseFloat(quote.outAmount) / 1e9;
    const pnl = solReceived - pos.solSpent;
    state.currentSol = await getSolBalance(connection, wallet.publicKey);
    state.totalPnl += pnl;

    if (pnl < 0) state.consecutiveLosses++;
    else state.consecutiveLosses = 0;

    delete state.positions[tokenName];

    state.trades.unshift({ type: 'SELL', token: tokenName, solReceived, pnl, sig, time: new Date().toISOString() });
    if (state.trades.length > 100) state.trades.pop();

    log('✅ Sold ' + tokenName + '. PnL: ' + (pnl >= 0 ? '+' : '') + pnl.toFixed(4) + ' SOL');
    await saveState();
  } catch(e) { log('Sell failed: ' + e.message); addError(e.message); }
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  if (!PRIVATE_KEY) { console.error('PRIVATE_KEY not set'); process.exit(1); }

  await loadState();

  const connection = new Connection(RPC_URL, 'confirmed');
  const wallet = Keypair.fromSecretKey(bs58.decode(PRIVATE_KEY));

  state.wallet = wallet.publicKey.toString();
  state.running = true;
  if (!state.positions) state.positions = {};

  log('Multi-Position Bot v2 started');
  log('Max positions: ' + CONFIG.MAX_POSITIONS + ' | Budget: ' + CONFIG.TOTAL_BUDGET_SOL + ' SOL');
  log('Wallet: ' + wallet.publicKey.toString());

  state.currentSol = await getSolBalance(connection, wallet.publicKey);
  if (!state.startingSol || state.startingSol === 1.0) state.startingSol = state.currentSol;
  log('Balance: ' + state.currentSol.toFixed(4) + ' SOL');

  await saveState();

  while (true) {
    try { await evaluateStrategy(connection, wallet); }
    catch(e) { log('Loop error: ' + e.message); addError(e.message); }
    await new Promise(r => setTimeout(r, CONFIG.SCAN_INTERVAL_MS));
  }
}

// ── Dashboard Server ──────────────────────────────────────────────────────────
const express = require('express');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, '../public')));
app.use((req, res, next) => { res.header('Access-Control-Allow-Origin', '*'); next(); });

app.get('/api/state', (req, res) => {
  try {
    const portfolioSol = getTotalPortfolioSol();
    res.json({ ...state, portfolioSol });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/health', (req, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log('Dashboard server running on port ' + PORT);
  main().catch(e => { console.error('Fatal:', e); process.exit(1); });
});

module.exports = { state };
