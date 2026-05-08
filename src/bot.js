const { Connection, Keypair, VersionedTransaction, PublicKey } = require('@solana/web3.js');
const { getAssociatedTokenAddress, getAccount } = require('@solana/spl-token');
const fetch = require('node-fetch');
const bs58 = require('bs58');
const fs = require('fs');

const RPC_URL           = process.env.RPC_URL           || 'https://api.mainnet-beta.solana.com';
const PRIVATE_KEY       = process.env.PRIVATE_KEY;
const COINGECKO_API_KEY = process.env.COINGECKO_API_KEY || '';
const STATE_FILE        = './bot2-state.json';

const CONFIG = {
  MAX_POSITIONS:      parseInt(process.env.MAX_POSITIONS    || '4'),
  TRADE_SIZE_SOL:     parseFloat(process.env.TRADE_SIZE_SOL || '0.2'),
  MIN_RESERVE_SOL:    parseFloat(process.env.MIN_RESERVE    || '0.05'),
  STOP_LOSS:          parseFloat(process.env.STOP_LOSS      || '0.25'),
  BUY_THRESHOLD:      parseFloat(process.env.BUY_THRESHOLD  || '0.015'),
  SCAN_INTERVAL_MS:   parseInt(process.env.SCAN_INTERVAL_MS || '60000'),
  MOMENTUM_EXIT:      parseFloat(process.env.MOMENTUM_EXIT  || '-0.005'),
  MIN_VOLUME_USD:     parseFloat(process.env.MIN_VOLUME_USD || '50000'),
  TRAILING_ARM_PCT:   parseFloat(process.env.TRAILING_ARM   || '0.15'),  // trailing stop 15%
  MIN_HOLD_MS:        parseInt(process.env.MIN_HOLD_MS      || String(60 * 60 * 1000)),  // 60 min
  VOL_DROP_THRESHOLD: parseFloat(process.env.VOL_DROP_THRESHOLD || '0.30'), // exit when volume drops 30% from entry
};

// Auto-discovery config
const DISC = {
  MIN_VOLUME:    parseFloat(process.env.DISC_MIN_VOLUME    || '200000'),
  MIN_CHANGE:    parseFloat(process.env.DISC_MIN_CHANGE    || '20'),
  MIN_LIQUIDITY: parseFloat(process.env.DISC_MIN_LIQUIDITY || '30000'),
  MAX_AGE_H:     parseFloat(process.env.DISC_MAX_AGE       || '72'),
  MAX_TOKENS:    parseInt(process.env.DISC_MAX_TOKENS      || '3'),
};

const REDIS_URL   = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const REDIS_KEY   = 'solbot2:momentum:state';
const SOL_MINT    = 'So11111111111111111111111111111111111111112';

const DEFAULT_TOKENS = {
  POPCAT:   { mint: '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr', decimals: 9, cgId: 'popcat' },
  FARTCOIN: { mint: '9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump', decimals: 6, cgId: 'fartcoin' },
  MEW:      { mint: 'MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5',  decimals: 5, cgId: 'cat-in-a-dogs-world' },
  PENGU:    { mint: '2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv', decimals: 6, cgId: 'pudgy-penguins' },
  BONK:     { mint: 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263', decimals: 5, cgId: 'bonk' },
  WIF:      { mint: 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', decimals: 6, cgId: 'dogwifcoin' },
  TROLL:    { mint: '5UUH9RTDiSpq6HKS6bp4NdU9PNJpXRXuiw6ShBTBhgH2', decimals: 6, cgId: 'troll-2' },
  LIMINAL:  { mint: 'CYSVBkXuuDaZ4gNqzQPaWuoxejr6ZgTKLSJeGNdjpump', decimals: 6, cgId: null },
};

let state = {
  running: false, wallet: null, startingSol: 1.0, currentSol: 1.0,
  positions: {}, watchlist: DEFAULT_TOKENS, trades: [],
  lastAction: 'Initializing...', lastCheck: null, errors: [], signals: {},
  priceHistory: {}, timeWindow: null, circuitOpen: false, circuitReason: null,
  consecutiveLosses: 0, totalPnl: 0, autoDiscovered: [], pendingBuys: {},
  boughtMints: {},     // mint→symbol map of tokens the bot has purchased
};

let autoAddedTokens = {};
let warmupScansRemaining = 0;

// ── Persistence ───────────────────────────────────────────────────────────────
async function saveState() {
  try { fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2)); } catch(e) {}
  if (!REDIS_URL || !REDIS_TOKEN) return;
  try {
    const slim = Object.assign({}, state, { priceHistory: {} });
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
      const res = await fetch(REDIS_URL + '/get/' + REDIS_KEY, { headers: { 'Authorization': 'Bearer ' + REDIS_TOKEN } });
      const data = await res.json();
      const raw = data.result || data.value;
      if (raw) {
        let parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
        if (typeof parsed === 'string') parsed = JSON.parse(parsed);
        state = { ...state, ...parsed };
        state.watchlist = Object.assign({}, state.watchlist);
        log('State loaded from Redis'); return;
      }
    } catch(e) { log('Redis load failed: ' + e.message); }
  }
  try {
    if (fs.existsSync(STATE_FILE)) {
      const saved = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      state = { ...state, ...saved };
    }
  } catch(e) { log('Fresh state'); }
}

function log(msg) {
  const ts = new Date().toISOString();
  console.log('[' + ts + '] ' + msg);
  state.lastAction = msg; state.lastCheck = ts;
}

function addError(msg) {
  state.errors.unshift({ time: new Date().toISOString(), msg });
  if (state.errors.length > 20) state.errors.pop();
}

// ── Price History ─────────────────────────────────────────────────────────────
function updatePriceHistory(token, price) {
  if (!state.priceHistory[token]) state.priceHistory[token] = [];
  state.priceHistory[token].push({ price, time: Date.now() });
  if (state.priceHistory[token].length > 60) state.priceHistory[token].shift();
}

function getMomentum(token) {
  const h = state.priceHistory[token];
  if (!h || h.length < 6) return 0;
  const p = h.map(x => x.price);
  const r = p.slice(-3).reduce((a,b)=>a+b,0)/3;
  const o = p.slice(-6,-3).reduce((a,b)=>a+b,0)/3;
  return o===0 ? 0 : (r-o)/o;
}

function getMomentumAcceleration(token) {
  const h = state.priceHistory[token];
  if (!h || h.length < 12) return 0;
  const p = h.map(x => x.price);
  const m1 = (() => { const r=p.slice(-3).reduce((a,b)=>a+b,0)/3, o=p.slice(-6,-3).reduce((a,b)=>a+b,0)/3; return o===0?0:(r-o)/o; })();
  const m2 = (() => { const r=p.slice(-7,-4).reduce((a,b)=>a+b,0)/3, o=p.slice(-10,-7).reduce((a,b)=>a+b,0)/3; return o===0?0:(r-o)/o; })();
  return m1 - m2;
}

function getVolatility(token) {
  const h = state.priceHistory[token];
  if (!h || h.length < 5) return 0;
  const p = h.map(x => x.price);
  const mean = p.reduce((a,b)=>a+b,0)/p.length;
  const variance = p.reduce((s,x)=>s+Math.pow(x-mean,2),0)/p.length;
  return mean===0 ? 0 : Math.sqrt(variance)/mean;
}

function buildMomentumScore(opts) {
  if (opts.volume24h < CONFIG.MIN_VOLUME_USD) return { score: 0, blocked: 'low volume ($' + Math.round(opts.volume24h/1000) + 'k)' };
  if (opts.volatility < 0.005) return { score: 0, blocked: 'low volatility' };
  if (opts.momentum <= 0) return { score: 0, blocked: 'no momentum' };
  let score = opts.momentum * 0.5;
  if (opts.acceleration > 0.005) score *= 1.5;
  score *= Math.max(1, 1 + (opts.volume24h/1000000) * 0.2);
  if (opts.sentimentScore !== null && opts.sentimentScore > 0.2) score *= 1.2;
  if (opts.change24h > 0.2) score *= 1.3;
  if (opts.change24h > 0.5) score *= 1.3;
  return { score, blocked: null };
}

// ── Auto-Discovery ────────────────────────────────────────────────────────────
async function scanForNewTokens() {
  try {
    log('Auto-discovery scan running...');
    const res = await fetch('https://api.dexscreener.com/latest/dex/search?q=solana', { timeout: 15000 });
    const data = await res.json();
    if (!data.pairs) return;
    const now = Date.now();
    const candidates = [];
    const SKIP = ['USDC','USDT','SOL','WSOL','BTC','ETH','WBTC','WETH'];
    for (const pair of data.pairs) {
      try {
        if (pair.chainId !== 'solana') continue;
        const symbol = (pair.baseToken && pair.baseToken.symbol || '').toUpperCase().replace(/[^A-Z0-9]/g,'');
        const mint = pair.baseToken && pair.baseToken.address;
        if (!symbol || !mint || symbol.length > 12) continue;
        if (state.watchlist[symbol] || autoAddedTokens[symbol]) continue;
        if (SKIP.includes(symbol)) continue;
        const volume24h = (pair.volume && pair.volume.h24) || 0;
        const change24h = (pair.priceChange && pair.priceChange.h24) || 0;
        const liquidity = (pair.liquidity && pair.liquidity.usd) || 0;
        const ageH = (now - (pair.pairCreatedAt || 0)) / 3600000;
        if (volume24h < DISC.MIN_VOLUME) continue;
        if (change24h < DISC.MIN_CHANGE) continue;
        if (liquidity < DISC.MIN_LIQUIDITY) continue;
        if (ageH > DISC.MAX_AGE_H) continue;
        candidates.push({ symbol, mint, volume24h, change24h, liquidity, ageH });
      } catch(e) {}
    }
    candidates.sort((a,b) => (b.volume24h*b.change24h) - (a.volume24h*a.change24h));
    const slots = DISC.MAX_TOKENS - Object.keys(autoAddedTokens).length;
    const toAdd = candidates.slice(0, Math.min(slots, 2));
    for (const t of toAdd) {
      const msg = 'Auto-discovered: ' + t.symbol + ' +' + t.change24h.toFixed(1) + '% vol=$' + Math.round(t.volume24h/1000) + 'k liq=$' + Math.round(t.liquidity/1000) + 'k age=' + t.ageH.toFixed(1) + 'h';
      log(msg);
      state.watchlist[t.symbol] = { mint: t.mint, decimals: 6, cgId: null };
      autoAddedTokens[t.symbol] = { addedAt: now, mint: t.mint };
      if (!state.autoDiscovered) state.autoDiscovered = [];
      state.autoDiscovered.unshift({ symbol: t.symbol, time: new Date().toISOString(), change24h: t.change24h, volume24h: t.volume24h });
      if (state.autoDiscovered.length > 20) state.autoDiscovered.pop();
    }
    if (toAdd.length > 0) { log('Added to watchlist: ' + toAdd.map(t=>t.symbol).join(', ')); await saveState(); }
    // Remove stale auto tokens (no position after 24h)
    for (const [sym, info] of Object.entries(autoAddedTokens)) {
      if ((now - info.addedAt) / 3600000 > 24 && !state.positions[sym]) {
        log('Removing stale auto-token: ' + sym);
        delete state.watchlist[sym];
        delete autoAddedTokens[sym];
        await saveState();
      }
    }
  } catch(e) { log('Discovery error: ' + e.message); }
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
  } catch(e) {
    // Fallback 1: getParsedTokenAccountsByOwner works for both SPL and Token-2022
    try {
      const accounts = await connection.getParsedTokenAccountsByOwner(walletPubkey, { mint: new PublicKey(tokenMint) });
      if (accounts.value.length > 0) {
        const uiAmount = accounts.value[0].account.data.parsed.info.tokenAmount.uiAmount;
        if (uiAmount !== null && uiAmount !== undefined) return uiAmount;
        // uiAmount can be null for very small amounts — use amount + decimals instead
        const rawAmount = accounts.value[0].account.data.parsed.info.tokenAmount.amount;
        return parseInt(rawAmount) / Math.pow(10, decimals);
      }
    } catch(e2) {}
    // Fallback 2: Try Token-2022 program explicitly
    try {
      const TOKEN_2022_PROGRAM = new PublicKey('TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb');
      const accounts2022 = await connection.getParsedTokenAccountsByOwner(walletPubkey, { mint: new PublicKey(tokenMint) }, { programId: TOKEN_2022_PROGRAM });
      if (accounts2022.value.length > 0) {
        const uiAmount = accounts2022.value[0].account.data.parsed.info.tokenAmount.uiAmount;
        if (uiAmount !== null && uiAmount !== undefined) return uiAmount;
        const rawAmount = accounts2022.value[0].account.data.parsed.info.tokenAmount.amount;
        return parseInt(rawAmount) / Math.pow(10, decimals);
      }
    } catch(e3) {}
    return 0;
  }
}

async function getSwapQuote(inputMint, outputMint, amount, slippageBps) {
  slippageBps = slippageBps || 200;
  try {
    const url = 'https://lite-api.jup.ag/swap/v1/quote?inputMint=' + inputMint + '&outputMint=' + outputMint + '&amount=' + amount + '&slippageBps=' + slippageBps;
    const res = await fetch(url, { timeout: 15000 });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    return data;
  } catch(e) { log('Quote failed: ' + e.message); return null; }
}

async function executeSwap(connection, wallet, quote) {
  try {
    const res = await fetch('https://lite-api.jup.ag/swap/v1/swap', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ quoteResponse: quote, userPublicKey: wallet.publicKey.toString(), wrapAndUnwrapSol: true, dynamicComputeUnitLimit: true, prioritizationFeeLamports: 'auto' }),
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

// ── Main Strategy Loop ────────────────────────────────────────────────────────
async function evaluateStrategy(connection, wallet) {
  if (state.circuitOpen) { log('Circuit open: ' + state.circuitReason); return; }
  log('Scanning ' + Object.keys(state.watchlist).length + ' tokens | Positions: ' + Object.keys(state.positions).length + '/' + CONFIG.MAX_POSITIONS);
  state.currentSol = await getSolBalance(connection, wallet.publicKey);

  for (const [token, pos] of Object.entries(state.positions)) {
    const info = state.watchlist[token];
    if (!info) continue;
    const onChain = await getTokenBalance(connection, wallet.publicKey, info.mint, info.decimals);
    if (onChain < 0.000001) {
      // Don't remove immediately — RPC nodes can be stale right after a buy
      // Require 2 consecutive zero-balance reads before removing (grace period)
      const zeroCount = (pos.zeroBalanceCount || 0) + 1;
      if (zeroCount >= 2) {
        log('Position ' + token + ' gone (confirmed zero x2) — removing');
        delete state.positions[token];
      } else {
        log('Position ' + token + ' shows zero balance — waiting for confirmation (' + zeroCount + '/2)');
        state.positions[token].zeroBalanceCount = zeroCount;
      }
    } else {
      state.positions[token].amount = onChain;
      state.positions[token].zeroBalanceCount = 0; // reset on any positive balance
    }
  }

  const tokenData = {};
  const watchTokens = Object.keys(state.watchlist);
  let globalSolUsd = 100;
  try {
    const cgHeaders = COINGECKO_API_KEY ? { 'x-cg-demo-api-key': COINGECKO_API_KEY } : {};
    const solRes = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd', { timeout: 10000, headers: cgHeaders });
    const solData = await solRes.json();
    if (solData.solana && solData.solana.usd) globalSolUsd = solData.solana.usd;
  } catch(e) {}

  for (let i = 0; i < watchTokens.length; i++) {
    const name = watchTokens[i];
    const info = state.watchlist[name];
    if (!info) continue;
    try {
      const cgHeaders = COINGECKO_API_KEY ? { 'x-cg-demo-api-key': COINGECKO_API_KEY } : {};
      let tokenUsd = null, solUsd = globalSolUsd, volume24h = 0, change24h = 0, sentimentScore = null;
      if (info.cgId) {
        try {
          const priceRes = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=' + info.cgId + ',solana&vs_currencies=usd', { timeout: 10000, headers: cgHeaders });
          const priceData = await priceRes.json();
          if (priceData[info.cgId] && priceData[info.cgId].usd) {
            tokenUsd = priceData[info.cgId].usd;
            if (priceData.solana && priceData.solana.usd) solUsd = priceData.solana.usd;
          }
        } catch(e) {}
      }
      if (!tokenUsd && info.mint) {
        try {
          const dsRes = await fetch('https://api.dexscreener.com/latest/dex/tokens/' + info.mint, { timeout: 10000 });
          const dsData = await dsRes.json();
          if (dsData.pairs && dsData.pairs.length > 0) {
            const pair = dsData.pairs.sort((a,b) => ((b.liquidity&&b.liquidity.usd)||0) - ((a.liquidity&&a.liquidity.usd)||0))[0];
            if (pair.priceUsd) { tokenUsd = parseFloat(pair.priceUsd); volume24h = (pair.volume&&pair.volume.h24)||0; change24h = (pair.priceChange&&pair.priceChange.h24)||0; }
          }
        } catch(e) {}
      }
      if (!tokenUsd || !solUsd) { if (i > 0) await new Promise(r => setTimeout(r, 800)); continue; }
      const priceSol = tokenUsd / solUsd;
      updatePriceHistory(name, priceSol);
      if (i > 0) await new Promise(r => setTimeout(r, 800));
      if (info.cgId && tokenUsd) {
        try {
          const cgRes = await fetch('https://api.coingecko.com/api/v3/coins/' + info.cgId + '?localization=false&tickers=false&community_data=true&developer_data=false', { timeout: 10000, headers: cgHeaders });
          const cgData = await cgRes.json();
          if (cgData.market_data) {
            volume24h = (cgData.market_data.total_volume&&cgData.market_data.total_volume.usd)||volume24h;
            change24h = cgData.market_data.price_change_percentage_24h||change24h;
            const change1h = (cgData.market_data.price_change_percentage_1h_in_currency&&cgData.market_data.price_change_percentage_1h_in_currency.usd)||0;
            const upPct = cgData.sentiment_votes_up_percentage||50;
            sentimentScore = ((upPct-(100-upPct))/100)*0.4 + Math.tanh((change1h*2+change24h*0.5)/10)*0.6;
          }
        } catch(e) {}
      }
      const momentum = getMomentum(name);
      const acceleration = getMomentumAcceleration(name);
      const volatility = getVolatility(name);
      const signal = buildMomentumScore({ momentum, acceleration, volatility, volume24h, change24h, sentimentScore });
      tokenData[name] = { priceSol, tokenUsd, solUsd, momentum, acceleration, volatility, volume24h, change24h, sentimentScore, score: signal.score, blocked: signal.blocked };
      log(name + ': $' + tokenUsd.toFixed(6) + ' mom=' + (momentum*100).toFixed(2) + '% score=' + signal.score.toFixed(5) + (signal.blocked?' ['+signal.blocked+']':''));
    } catch(e) { log('Error scanning ' + name + ': ' + e.message); }
  }

  state.signals = {};
  Object.entries(tokenData).forEach(([k,v]) => {
    state.signals[k] = { price: v.priceSol, priceUsd: v.tokenUsd, momentum: v.momentum, acceleration: v.acceleration, volatility: v.volatility, volume24h: v.volume24h, change24h: v.change24h, compositeScore: v.score, blocked: v.blocked };
  });

  for (const [token, pos] of Object.entries(state.positions)) {
    const sig = tokenData[token];
    if (!sig) continue;
    const pctFromEntry = pos.entryPrice > 0 ? (sig.priceSol - pos.entryPrice) / pos.entryPrice : 0;
    const pctFromPeak = pos.peakPrice > 0 ? (sig.priceSol - pos.peakPrice) / pos.peakPrice : 0;
    if (sig.priceSol > (pos.peakPrice || 0)) state.positions[token].peakPrice = sig.priceSol;
    const heldMs = Date.now() - new Date(pos.openedAt).getTime();
    const minHoldMet = heldMs >= CONFIG.MIN_HOLD_MS;

    // Volume drop from entry — primary exit signal
    const curVolume = sig.volume24h || 0;
    const volDrop = pos.entryVolume > 0 ? (pos.entryVolume - curVolume) / pos.entryVolume : 0;

    let exitReason = null;
    if (pctFromEntry < -CONFIG.STOP_LOSS) {
      exitReason = 'stop-loss (' + (pctFromEntry*100).toFixed(1) + '%)';
    } else if (pctFromPeak < -CONFIG.TRAILING_ARM_PCT) {
      exitReason = 'trailing stop (' + (pctFromPeak*100).toFixed(1) + '% from peak)';
    } else if (volDrop >= CONFIG.VOL_DROP_THRESHOLD && heldMs >= 60*60*1000) {
      exitReason = 'volume exhausted (' + (volDrop*100).toFixed(0) + '% drop from entry)';
    } else if (minHoldMet && sig.score < CONFIG.MOMENTUM_EXIT && !sig.blocked) {
      exitReason = 'momentum died';
    } else if (minHoldMet && sig.score === 0 && pctFromEntry < -0.02) {
      exitReason = 'no momentum + losing (' + (pctFromEntry*100).toFixed(1) + '%)';
    }
    if (exitReason) { log('SELL ' + token + ': ' + exitReason); await executeSell(connection, wallet, token, pos, sig); }
    else log('Holding ' + token + '. P&L: ' + (pctFromEntry>=0?'+':'') + (pctFromEntry*100).toFixed(2) + '% | mom=' + (sig.momentum*100).toFixed(2) + '%');
  }

  const openCount = Object.keys(state.positions).length;
  const availableSlots = CONFIG.MAX_POSITIONS - openCount;
  if (warmupScansRemaining > 0) {
    warmupScansRemaining--;
    log('Warmup: ' + warmupScansRemaining + ' scans remaining before buys enabled');
  } else if (availableSlots > 0 && state.currentSol > CONFIG.MIN_RESERVE_SOL + CONFIG.TRADE_SIZE_SOL) {
    const candidates = Object.entries(tokenData).filter(([n,d]) => !state.positions[n] && !d.blocked && d.score >= CONFIG.BUY_THRESHOLD).sort((a,b) => b[1].score - a[1].score).slice(0, availableSlots);
    if (candidates.length > 0) { for (const [n,d] of candidates) { if (state.currentSol < CONFIG.MIN_RESERVE_SOL + CONFIG.TRADE_SIZE_SOL) break; log('BUY ' + n + ' score=' + d.score.toFixed(5)); await executeBuy(connection, wallet, n, d); } }
    else log('No momentum signals. Watching...');
  } else if (availableSlots === 0) log('All slots filled.');
  await saveState();
}

async function executeBuy(connection, wallet, tokenName, tokenData) {
  try {
    const info = state.watchlist[tokenName];
    if (!info) return;
    const solBalance = await getSolBalance(connection, wallet.publicKey);
    const tradeSOL = Math.min(CONFIG.TRADE_SIZE_SOL, solBalance - CONFIG.MIN_RESERVE_SOL);
    if (tradeSOL < 0.05) { log('Not enough SOL for ' + tokenName); return; }
    const quote = await getSwapQuote(SOL_MINT, info.mint, Math.floor(tradeSOL * 1e9));
    if (!quote) return;

    // Save intent BEFORE swap — crash recovery uses this to restore position
    if (!state.pendingBuys) state.pendingBuys = {};
    state.pendingBuys[tokenName] = { mint: info.mint, decimals: info.decimals, entryPrice: tokenData.priceSol, entryPriceUsd: tokenData.tokenUsd, solSpent: tradeSOL, volume24h: tokenData.volume24h || 0, time: new Date().toISOString() };
    await saveState();

    const sig = await executeSwap(connection, wallet, quote);
    if (!sig) {
      delete state.pendingBuys[tokenName];
      await saveState();
      return;
    }

    await new Promise(r => setTimeout(r, 5000));
    const onChain = await getTokenBalance(connection, wallet.publicKey, info.mint, info.decimals);
    const tokensReceived = onChain > 0 ? onChain : parseFloat(quote.outAmount) / Math.pow(10, info.decimals);
    state.currentSol = await getSolBalance(connection, wallet.publicKey);
    state.positions[tokenName] = { amount: tokensReceived, entryPrice: tokenData.priceSol, entryPriceUsd: tokenData.tokenUsd, peakPrice: tokenData.priceSol, solSpent: tradeSOL, openedAt: new Date().toISOString(), entryVolume: tokenData.volume24h || 0, zeroBalanceCount: 0 };
    delete state.pendingBuys[tokenName]; // clear intent — fully tracked
    state.trades.unshift({ type: 'BUY', token: tokenName, solSpent: tradeSOL, tokensReceived, price: tokenData.priceSol, priceUsd: tokenData.tokenUsd, score: tokenData.score, sig, time: new Date().toISOString() });
    if (state.trades.length > 100) state.trades.pop();
    if (!state.boughtMints) state.boughtMints = {};
    state.boughtMints[info.mint] = tokenName;  // whitelist this mint
    log('Bought ' + tokenName + ' ' + tokensReceived.toFixed(2) + ' tokens. Tx: ' + sig);
    await saveState();
  } catch(e) { log('Buy failed: ' + e.message); addError(e.message); }
}

async function executeSell(connection, wallet, tokenName, pos, tokenData) {
  try {
    const info = state.watchlist[tokenName];
    if (!info) return;
    // Security: only sell tokens the bot itself bought
    if (!state.boughtMints || !state.boughtMints[info.mint]) {
      log('SECURITY: refusing to sell ' + tokenName + ' — not in bought mints list');
      return;
    }
    const onChain = await getTokenBalance(connection, wallet.publicKey, info.mint, info.decimals);
    if (onChain < 0.000001) { delete state.positions[tokenName]; await saveState(); return; }
    const quote = await getSwapQuote(info.mint, SOL_MINT, Math.floor(onChain * Math.pow(10, info.decimals)));
    if (!quote) return;
    const sig = await executeSwap(connection, wallet, quote);
    if (!sig) return;
    const solReceived = parseFloat(quote.outAmount) / 1e9;
    const pnl = solReceived - pos.solSpent;
    state.currentSol = await getSolBalance(connection, wallet.publicKey);
    state.totalPnl = (state.totalPnl || 0) + pnl;
    if (pnl < 0) state.consecutiveLosses++; else state.consecutiveLosses = 0;
    delete state.positions[tokenName];
    state.trades.unshift({ type: 'SELL', token: tokenName, solReceived, pnl, sig, time: new Date().toISOString() });
    if (state.trades.length > 100) state.trades.pop();
    if (state.boughtMints) delete state.boughtMints[info.mint];  // remove from whitelist
    log('Sold ' + tokenName + '. PnL: ' + (pnl>=0?'+':'') + pnl.toFixed(4) + ' SOL');
    await saveState();
  } catch(e) { log('Sell failed: ' + e.message); addError(e.message); }
}


// ── Bootstrap Price History ───────────────────────────────────────────────────
async function bootstrapPriceHistory() {
  log('Bootstrapping price history...');
  let solUsd = 150;
  try {
    const r = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd', { timeout: 10000 });
    const d = await r.json();
    if (d.solana && d.solana.usd) solUsd = d.solana.usd;
  } catch(e) {}

  const tokens = Object.keys(state.watchlist);
  for (let i = 0; i < tokens.length; i++) {
    const name = tokens[i];
    const info = state.watchlist[name];
    if (!info || !info.mint) continue;
    try {
      const res = await fetch('https://api.dexscreener.com/latest/dex/tokens/' + info.mint, { timeout: 12000 });
      const data = await res.json();
      if (!data.pairs || data.pairs.length === 0) continue;
      const pair = data.pairs.sort(function(a,b){
        return ((b.liquidity&&b.liquidity.usd)||0) - ((a.liquidity&&a.liquidity.usd)||0);
      })[0];
      const curUsd = parseFloat(pair.priceUsd || '0');
      if (!curUsd) continue;
      const change24h = (pair.priceChange && pair.priceChange.h24) || 0;
      const change6h  = (pair.priceChange && pair.priceChange.h6)  || change24h / 4;
      const price24hAgo = curUsd / (1 + change24h / 100);
      const price6hAgo  = curUsd / (1 + change6h  / 100);
      state.priceHistory[name] = [];
      const now = Date.now();
      for (let j = 0; j < 15; j++) {
        const t = j / 14;
        const synthUsd = price24hAgo + (price6hAgo - price24hAgo) * t;
        state.priceHistory[name].push({ price: synthUsd / solUsd, time: now - (19 - j) * 7200000 });
      }
      for (let j = 0; j < 5; j++) {
        const t = j / 4;
        const synthUsd = price6hAgo + (curUsd - price6hAgo) * t;
        state.priceHistory[name].push({ price: synthUsd / solUsd, time: now - (4 - j) * 3600000 });
      }
      log('Bootstrap ' + name + ': 20 pts, mom=' + (getMomentum(name)*100).toFixed(2) + '% vol=' + (getVolatility(name)*100).toFixed(3) + '%');
      if (i < tokens.length - 1) await new Promise(r => setTimeout(r, 500));
    } catch(e) {
      log('Bootstrap failed for ' + name + ': ' + e.message);
    }
  }
  log('Bootstrap complete — 6 scan warmup before new buys enabled.');
  warmupScansRemaining = 6;
}

async function main() {
  if (!PRIVATE_KEY) { console.error('PRIVATE_KEY not set'); process.exit(1); }
  await loadState();
  const connection = new Connection(RPC_URL, 'confirmed');
  const wallet = Keypair.fromSecretKey(bs58.decode(PRIVATE_KEY));
  state.wallet = wallet.publicKey.toString();
  state.running = true;
  if (!state.positions) state.positions = {};
  state.watchlist = Object.assign({}, state.watchlist, DEFAULT_TOKENS);
  log('Momentum Bot v2 started');
  log('Watching ' + Object.keys(state.watchlist).length + ' tokens | ' + CONFIG.MAX_POSITIONS + ' max positions');
  log('Wallet: ' + wallet.publicKey.toString());
  state.currentSol = await getSolBalance(connection, wallet.publicKey);
  if (!state.startingSol) state.startingSol = state.currentSol;
  log('Balance: ' + state.currentSol.toFixed(4) + ' SOL');
  // Recover any pending buys from before a crash
  if (state.pendingBuys && Object.keys(state.pendingBuys).length > 0) {
    log('Checking ' + Object.keys(state.pendingBuys).length + ' pending buy(s) from last session...');
    for (const [name, pb] of Object.entries(state.pendingBuys)) {
      try {
        const onChain = await getTokenBalance(connection, wallet.publicKey, pb.mint, pb.decimals);
        if (onChain > 0.000001) {
          log('Pending buy confirmed: ' + name + ' (' + onChain.toFixed(4) + ' tokens) @ ' + pb.entryPrice.toFixed(8) + ' SOL — restoring with real entry price');
          state.positions[name] = { amount: onChain, entryPrice: pb.entryPrice, entryPriceUsd: pb.entryPriceUsd, peakPrice: pb.entryPrice, solSpent: pb.solSpent, openedAt: pb.time, entryVolume: pb.volume24h, zeroBalanceCount: 0 };
        } else {
          log('Pending buy for ' + name + ' not found on-chain — swap failed, clearing');
        }
        delete state.pendingBuys[name];
      } catch(e) { log('Pending buy check failed for ' + name + ': ' + e.message); }
    }
    await saveState();
  }

  // SECURITY: Only recover positions for tokens the bot actually bought.
  // Airdropped or unknown tokens in the wallet are ignored completely.
  if (!state.boughtMints) state.boughtMints = {};
  log('Checking on-chain balances for bot-purchased positions...');
  for (const [mint, symbol] of Object.entries(state.boughtMints)) {
    if (state.positions[symbol]) continue;
    const info = state.watchlist[symbol];
    if (!info) continue;
    try {
      const onChain = await getTokenBalance(connection, wallet.publicKey, mint, info.decimals);
      if (onChain > 0.000001) {
        log('Recovered purchased position: ' + symbol + ' (' + onChain.toFixed(4) + ' tokens)');
        state.positions[symbol] = { amount: onChain, entryPrice: 0, entryPriceUsd: 0, peakPrice: 0, solSpent: CONFIG.TRADE_SIZE_SOL, openedAt: new Date().toISOString(), zeroBalanceCount: 0 };
      }
    } catch(e) {}
  }
  log('Positions after startup scan: ' + Object.keys(state.positions).join(', ') || 'none');
  await saveState();
  await bootstrapPriceHistory();
  let loopCount = 0;
  while (true) {
    try {
      if (loopCount % 10 === 0) await scanForNewTokens();
      loopCount++;
      await evaluateStrategy(connection, wallet);
    }
    catch(e) { log('Loop error: ' + e.message); addError(e.message); }
    await new Promise(r => setTimeout(r, CONFIG.SCAN_INTERVAL_MS));
  }
}

const express = require('express');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));
app.use((req, res, next) => { res.header('Access-Control-Allow-Origin', '*'); res.header('Access-Control-Allow-Methods', 'GET,POST,DELETE'); res.header('Access-Control-Allow-Headers', 'Content-Type'); next(); });

app.get('/api/state', (req, res) => {
  try {
    let posValue = 0;
    Object.entries(state.positions || {}).forEach(([token, pos]) => {
      const sig = state.signals && state.signals[token];
      if (sig) posValue += sig.price * pos.amount;
    });
    res.json({ ...state, portfolioSol: (state.currentSol || 0) + posValue });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/watchlist', async (req, res) => {
  try {
    const { symbol, mint, decimals, cgId } = req.body;
    if (!symbol || !mint || decimals === undefined || !cgId) return res.status(400).json({ error: 'Missing: symbol, mint, decimals, cgId' });
    state.watchlist[symbol.toUpperCase()] = { mint, decimals: parseInt(decimals), cgId };
    await saveState();
    log('Added ' + symbol.toUpperCase() + ' to watchlist');
    res.json({ ok: true, watchlist: state.watchlist });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/watchlist/:symbol', async (req, res) => {
  try {
    const symbol = req.params.symbol.toUpperCase();
    if (state.positions && state.positions[symbol]) return res.status(400).json({ error: 'Cannot remove token with open position' });
    delete state.watchlist[symbol];
    await saveState();
    res.json({ ok: true, watchlist: state.watchlist });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/health', (req, res) => res.json({ ok: true }));
app.listen(PORT, () => { console.log('Dashboard server running on port ' + PORT); main().catch(e => { console.error('Fatal:', e); process.exit(1); }); });
module.exports = { state };
