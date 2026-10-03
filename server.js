const express = require('express');
const axios = require('axios');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// Upbit API Rate Limiting Delay Helper
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function fetchWithRetry(url, options = {}, retries = 3, backoff = 1000) {
  try {
    const res = await axios.get(url, { timeout: 8000, ...options });
    return res.data;
  } catch (err) {
    if (retries > 0 && err.response && (err.response.status === 429 || err.response.status >= 500)) {
      await delay(backoff);
      return fetchWithRetry(url, options, retries - 1, backoff * 1.5);
    }
    throw err;
  }
}

// 240개 이상의 1시간봉 데이터 페이징 수집 (240SMA 연산 필수 조건)
async function get240HourCandles(market) {
  let candles = [];
  try {
    // 1차: 최근 200개 캔들
    const url1 = `https://api.upbit.com/v1/candles/minutes/60?market=${market}&count=200`;
    const batch1 = await fetchWithRetry(url1);
    candles = candles.concat(batch1);
    await delay(120);

    if (batch1.length > 0) {
      const oldestTime = batch1[batch1.length - 1].candle_date_time_utc;
      // 2차: 과거 100개 추가 캔들
      const url2 = `https://api.upbit.com/v1/candles/minutes/60?market=${market}&to=${oldestTime}Z&count=100`;
      const batch2 = await fetchWithRetry(url2);
      candles = candles.concat(batch2);
    }
  } catch (e) {
    console.error(`Candle fetch error for ${market}:`, e.message);
  }

  // 중복 제거 및 시간순 정렬 (오래된 캔들 -> 최신 캔들)
  const map = new Map();
  candles.forEach((c) => map.set(c.timestamp, c));
  const uniqueCandles = Array.from(map.values()).sort((a, b) => a.timestamp - b.timestamp);

  return uniqueCandles;
}

// SMA 계산기
function calcSMA(candles, period) {
  if (!candles || candles.length < period) return null;
  const closes = candles.map((c) => c.trade_price);
  const result = [];
  for (let i = 0; i < closes.length; i++) {
    if (i < period - 1) {
      result.push(null);
    } else {
      const sum = closes.slice(i - period + 1, i + 1).reduce((a, b) => a + b, 0);
      result.push(sum / period);
    }
  }
  return result;
}

// HH / HL / LH / LL 파동 스윙 탐지
function analyzeStructure(candles) {
  if (candles.length < 30) return { structure: 'UNKNOWN', highs: [], lows: [] };
  const highs = [];
  const lows = [];

  for (let i = 2; i < candles.length - 2; i++) {
    const prev2 = candles[i - 2].high_price;
    const prev1 = candles[i - 1].high_price;
    const currH = candles[i].high_price;
    const next1 = candles[i + 1].high_price;
    const next2 = candles[i + 2].high_price;

    if (currH > prev2 && currH > prev1 && currH > next1 && currH > next2) {
      highs.push({ price: currH, index: i });
    }

    const prev2L = candles[i - 2].low_price;
    const prev1L = candles[i - 1].low_price;
    const currL = candles[i].low_price;
    const next1L = candles[i + 1].low_price;
    const next2L = candles[i + 2].low_price;

    if (currL < prev2L && currL < prev1L && currL < next1L && currL < next2L) {
      lows.push({ price: currL, index: i });
    }
  }

  let isHigherLow = false;
  let isHigherHigh = false;
  if (lows.length >= 2) {
    isHigherLow = lows[lows.length - 1].price > lows[lows.length - 2].price;
  }
  if (highs.length >= 2) {
    isHigherHigh = highs[highs.length - 1].price > highs[highs.length - 2].price;
  }

  let type = 'RANGE/UNKNOWN';
  if (isHigherLow && isHigherHigh) type = 'BULLISH (HH/HL)';
  else if (!isHigherLow && !isHigherHigh) type = 'BEARISH (LH/LL)';

  return { type, highs, lows };
}

// 실시간 스캔 API (Server-Sent Events 기반 진행률 스트리밍)
app.get('/api/scan-stream', async (req, res) => {
  const minVolInBillion = parseFloat(req.query.minVol || '30');

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  const sendEvent = (event, data) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  try {
    sendEvent('progress', { step: 'STEP 1: KRW 마켓 목록 조회 중...', percent: 10 });
    const allMarkets = await fetchWithRetry('https://api.upbit.com/v1/market/all?isDetails=false');
    const krwMarkets = allMarkets.filter((m) => m.market.startsWith('KRW-'));

    sendEvent('progress', { step: 'STEP 2: 24시간 거래대금 필터링 중...', percent: 20 });
    await delay(200);

    // 100개씩 묶어서 Ticker 조회
    const marketCodes = krwMarkets.map((m) => m.market).join(',');
    const tickers = await fetchWithRetry(`https://api.upbit.com/v1/ticker?markets=${marketCodes}`);

    const filtered = tickers.filter((t) => (t.acc_trade_price_24h / 100000000) >= (minVolInBillion * 10));

    sendEvent('progress', {
      step: `STEP 3: ${filtered.length}개 종목 1시간봉 240SMA & 파동 정밀 수집 중...`,
      percent: 30,
    });

    // 비트코인 시황 조회
    let btcTrend = 'UNKNOWN';
    try {
      const btcCandles = await get240HourCandles('KRW-BTC');
      const btc240 = calcSMA(btcCandles, 240);
      if (btcCandles.length > 0 && btc240) {
        const lastPrice = btcCandles[btcCandles.length - 1].trade_price;
        const last240 = btc240[btc240.length - 1];
        btcTrend = lastPrice > last240 ? 'PASS (240SMA 위)' : 'WARNING (240SMA 아래)';
      }
    } catch (e) {
      btcTrend = 'UNKNOWN';
    }

    const results = [];
    let completedCount = 0;

    for (const t of filtered) {
      completedCount++;
      const currentPct = 30 + Math.floor((completedCount / filtered.length) * 65);
      sendEvent('progress', {
        step: `수집 중 (${completedCount}/${filtered.length}): ${t.market}`,
        percent: currentPct,
      });

      const marketInfo = krwMarkets.find((m) => m.market === t.market);
      const name = marketInfo ? marketInfo.korean_name : t.market;
      const candles = await get240HourCandles(t.market);

      if (candles.length < 240) {
        // 데이터 부족 제외
        continue;
      }

      const sma10 = calcSMA(candles, 10);
      const sma20 = calcSMA(candles, 20);
      const sma240 = calcSMA(candles, 240);

      const idx = candles.length - 1;
      const price = candles[idx].trade_price;
      const v10 = sma10[idx];
      const v20 = sma20[idx];
      const v240 = sma240[idx];

      // 데이터 무결성 검사 (NaN 체크)
      if (isNaN(price) || isNaN(v10) || isNaN(v20) || isNaN(v240)) continue;

      const struct = analyzeStructure(candles);

      // 단계 (Stage) 판단
      let stage = 'WATCH';
      let score = 50;
      const flags = [];

      // 추세/위치 검사
      if (price > v240) {
        score += 20;
        flags.push('240SMA 상회');
      }
      if (v10 > v20) {
        score += 15;
        flags.push('10/20SMA 골든');
      }
      if (struct.type === 'BULLISH (HH/HL)') {
        score += 15;
        flags.push('상승 파동 구조');
      }

      // 과열 검사
      const dev20 = ((price - v20) / v20) * 100;
      if (dev20 > 12) {
        stage = 'CHASE';
        flags.push('단기 이격 과열');
      } else if (price > v240 && v10 > v20 && dev20 <= 5) {
        stage = 'READY';
      } else if (struct.type === 'BULLISH (HH/HL)' && dev20 <= 8) {
        stage = 'BUILDING';
      } else if (dev20 > 8 && dev20 <= 12) {
        stage = 'TRIGGERED';
      }

      // R/R 손익비 계산 (구조적 계산)
      const lastLow = struct.lows.length > 0 ? struct.lows[struct.lows.length - 1].price : price * 0.95;
      const invalidationPrice = Math.min(lastLow, v20);
      const targetPrice = price * 1.08;
      const risk = price - invalidationPrice;
      const reward = targetPrice - price;
      const rrRatio = risk > 0 ? (reward / risk).toFixed(2) : 'N/A';

      // 캔들 오버레이용 데이터 준비 (최근 50개)
      const recentCandles = candles.slice(-50).map((c, i) => ({
        time: new Date(c.timestamp).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' }),
        open: c.opening_price,
        high: c.high_price,
        low: c.low_price,
        close: c.trade_price,
        volume: c.candle_acc_trade_volume,
        sma10: sma10[candles.length - 50 + i],
        sma20: sma20[candles.length - 50 + i],
        sma240: sma240[candles.length - 50 + i],
      }));

      results.push({
        market: t.market,
        name,
        price,
        tradePrice24h: Math.floor(t.acc_trade_price_24h / 1000000),
        stage,
        score,
        btcTrend,
        structType: struct.type,
        dev20: dev20.toFixed(2),
        invalidationPrice,
        targetPrice,
        rrRatio,
        flags,
        candles: recentCandles,
        gStatus: {
          G1: price > v240 ? 'PASS' : 'WARNING',
          G2: btcTrend.includes('PASS') ? 'PASS' : 'WARNING',
          G3: 'PASS',
          G4: struct.type.includes('BULLISH') ? 'PASS' : 'UNKNOWN',
          G5: stage !== 'CHASE' ? 'PASS' : 'FAIL',
        },
      });

      await delay(80); // API Rate Limit 안정을 위한 딜레이
    }

    results.sort((a, b) => b.score - a.score);

    sendEvent('progress', { step: '스캔 완료!', percent: 100 });
    sendEvent('complete', {
      timestamp: new Date().toLocaleString('ko-KR'),
      totalScanned: filtered.length,
      candidates: results,
    });
    res.end();
  } catch (err) {
    sendEvent('error', { message: err.message || '스캔 도중 오류가 발생했습니다.' });
    res.end();
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
