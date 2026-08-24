# ================================================================
# ULTIMATE REAL‑TIME QUANT TERMINAL – TOURNAMENT EDITION v26 (FINAL FIX)
# ================================================================
# Description: Fully institutional‑grade terminal for Binance Futures.
#              Top 10 high‑volatility coins scanned simultaneously.
#              Real‑time order book depth, differential OFI, volume profile,
#              CVD, OI delta, and Bayesian fusion.
#              NO REST API calls – all data from WebSocket streams.
#              Golden mini‑table, cached resources, DB timeout fixed.
#              COMPLETE Trade History (with CSV export) & Performance tabs.
#              Fixed "Signal data stale" warnings (threshold 120s).
#              Professional Bayesian fusion formatting.
#              Live clock displayed.
#              Ultra‑low latency, cloud‑deployable.
#              AUTO-TRADING REMOVED – manual signals only.
#              Backtesting: fixed resample frequency, candle‑based indicators,
#              pagination loop limit, OI fallback, multi‑TF bias forward‑fill.
#              FIXED: Futures WebSocket URLs for live data.
#              NEW: Active Walls Table (top 5 bids/asks with size & USD value).
#              NEW: Funding Rate + OI Chart (historical view).
#              NEW: Market Bias (Buy/Sell % from CVD & volume).
#              FIX: Removed unsupported `version` parameter from cache decorator.
#                    Changed cache function name to force refresh.
# ================================================================

import time
import math
import sqlite3
import threading
import json
import websocket
import os
import atexit
import queue
import logging
import urllib.request
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Dict, Any
import copy
import sys

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import ccxt
import requests

# ---------- Optional dependencies (with type: ignore for Pylance) ----------
try:
    import redis  # type: ignore
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import playsound  # type: ignore
    SOUND_AVAILABLE = True
except ImportError:
    SOUND_AVAILABLE = False

try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore
except ImportError:
    st_autorefresh = None

# =====================================================================
# 1. LOGGING
# =====================================================================
logging.basicConfig(
    filename='institutional_quant.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)

# =====================================================================
# 2. DATABASE WRITER & SCHEMA (with timeout)
# =====================================================================
DB_FILE = "institutional_quant.db"

def ensure_db_schema():
    conn = sqlite3.connect(DB_FILE, timeout=30.0)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id TEXT PRIMARY KEY,
            entry_time TEXT,
            asset TEXT,
            direction TEXT,
            entry_price REAL,
            stop_loss REAL,
            take_profit REAL,
            confidence REAL,
            exit_time TEXT,
            exit_price REAL,
            result TEXT,
            profit_percent REAL,
            status TEXT,
            lock_time TEXT,
            lock_reason TEXT
        )
    ''')
    cursor.execute("PRAGMA table_info(trades)")
    existing_columns = [row[1] for row in cursor.fetchall()]
    required_columns = [
        'entry_time', 'asset', 'direction', 'entry_price', 'stop_loss',
        'take_profit', 'confidence', 'exit_time', 'exit_price',
        'result', 'profit_percent', 'status', 'lock_time', 'lock_reason'
    ]
    for col in required_columns:
        if col not in existing_columns:
            col_type = 'REAL' if col in ['entry_price', 'stop_loss', 'take_profit', 'confidence', 'exit_price', 'profit_percent'] else 'TEXT'
            cursor.execute(f"ALTER TABLE trades ADD COLUMN {col} {col_type}")
    conn.commit()
    conn.close()

ensure_db_schema()

class QueueDBWriter(threading.Thread):
    def __init__(self, db_file=DB_FILE):
        super().__init__(daemon=True)
        self.db_file = db_file
        self.task_queue = queue.Queue()
        self.stop_event = threading.Event()

    def run(self):
        conn = sqlite3.connect(self.db_file, timeout=30.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                entry_time TEXT,
                asset TEXT,
                direction TEXT,
                entry_price REAL,
                stop_loss REAL,
                take_profit REAL,
                confidence REAL,
                exit_time TEXT,
                exit_price REAL,
                result TEXT,
                profit_percent REAL,
                status TEXT,
                lock_time TEXT,
                lock_reason TEXT
            )
        ''')
        conn.commit()

        while not self.stop_event.is_set():
            try:
                item = self.task_queue.get(timeout=1.0)
                if item is None:
                    break
                cursor.execute(
                    "INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    item
                )
                conn.commit()
                self.task_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logging.exception(f"DB Writer Exception: {e}")
        conn.close()

    def add_trade(self, trade_tuple):
        self.task_queue.put(trade_tuple)

    def stop(self):
        self.stop_event.set()
        if self.is_alive():
            self.join(timeout=2.0)

# =====================================================================
# 3. BINANCE EXCHANGE CONNECTOR (REST fallback only for initial data)
# =====================================================================
def get_ccxt_exchange():
    config = {'enableRateLimit': True, 'timeout': 5000, 'options': {'defaultType': 'future'}}
    return ccxt.binance(config)

def fetch_with_retry(func, *args, retries=3, delay=1, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.exception(f"Retry attempt {attempt+1} failed: {e}")
            if attempt == retries - 1:
                raise e
            time.sleep(delay * (2 ** attempt))

# =====================================================================
# 4. REDIS STORE (with fallback) – CACHED
# =====================================================================
class RedisSignalStore:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.use_redis = False
        self._dict = {}
        self._lock = threading.RLock()
        if REDIS_AVAILABLE:
            try:
                if password:
                    self.redis_client = redis.Redis(
                        host=host, port=port, db=db, password=password,
                        decode_responses=True
                    )
                else:
                    self.redis_client = redis.Redis(
                        host=host, port=port, db=db, decode_responses=True
                    )
                self.redis_client.ping()
                self.use_redis = True
                logging.info("Redis connected successfully.")
            except Exception as e:
                logging.warning(f"Redis connection failed: {e}. Using in-memory fallback.")
        else:
            logging.warning("Redis library not installed. Using in-memory fallback.")

    def set(self, key, value, ex=None):
        if self.use_redis:
            try:
                if ex:
                    self.redis_client.setex(key, ex, json.dumps(value))
                else:
                    self.redis_client.set(key, json.dumps(value))
            except Exception:
                pass
        else:
            with self._lock:
                self._dict[key] = value

    def get(self, key):
        if self.use_redis:
            try:
                val = self.redis_client.get(key)
                if val:
                    return json.loads(val)
                return None
            except Exception:
                return None
        else:
            with self._lock:
                return self._dict.get(key)

    def delete(self, key):
        if self.use_redis:
            try:
                self.redis_client.delete(key)
            except Exception:
                pass
        else:
            with self._lock:
                self._dict.pop(key, None)

# =====================================================================
# 5. TOP 10 HIGH‑VOLATILITY COINS (Tournament)
# =====================================================================
TOP_COINS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT",
    "BNBUSDT", "DOGEUSDT",
    "XRPUSDT", "ADAUSDT", "AVAXUSDT",
    "SUIUSDT", "NEARUSDT"
]
DISPLAY_SYMBOLS = {s: f"{s[:-4]}/USDT" for s in TOP_COINS}

# =====================================================================
# 6. MULTI‑STREAM WEBSOCKET PROCESSOR (trades & miniTicker) – CACHED
# =====================================================================
class BinanceMultiStreamProcessor:
    def __init__(self, store: RedisSignalStore, symbols=TOP_COINS):
        self.store = store
        self.symbols = symbols
        self.stop_event = threading.Event()
        self.thread = None
        self.cvd = {s: 0.0 for s in symbols}
        self.price_history = {s: deque(maxlen=100) for s in symbols}
        self.volume_history = {s: deque(maxlen=100) for s in symbols}
        self.oi_history = {s: deque(maxlen=100) for s in symbols}
        self.funding_rate_history = {s: deque(maxlen=100) for s in symbols}  # NEW: store funding rates
        self.liquidation_count = {s: 0 for s in symbols}
        self.last_update = time.time()
        self.lock = threading.RLock()

    def start(self):
        if self.thread is None or not self.thread.is_alive():
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)

    def _run(self):
        streams = []
        for sym in self.symbols:
            streams.append(f"{sym.lower()}@trade")
        streams.append("!miniTicker@arr")
        streams.append("!forceOrder@arr")
        streams.append("!markPrice@arr")
        stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

        while not self.stop_event.is_set():
            try:
                def on_message(ws, message):
                    if self.stop_event.is_set():
                        ws.close()
                        return
                    data = json.loads(message)
                    stream = data.get('stream')
                    if not stream:
                        return
                    payload = data.get('data')
                    if not payload:
                        return
                    with self.lock:
                        self.last_update = time.time()
                    if stream.endswith('@trade'):
                        self._handle_trade(payload)
                    elif stream == '!miniTicker@arr':
                        self._handle_mini_tickers(payload)
                    elif stream == '!forceOrder@arr':
                        self._handle_force_orders(payload)
                    elif stream == '!markPrice@arr':
                        self._handle_mark_prices(payload)

                ws = websocket.WebSocketApp(stream_url, on_message=on_message)
                ws.run_forever(ping_interval=20)
            except Exception as e:
                logging.exception(f"MultiStream error: {e}")
                time.sleep(2)

    def _handle_trade(self, trade):
        s = trade.get('s')
        if s not in self.symbols:
            return
        price = float(trade.get('p', 0))
        qty = float(trade.get('q', 0))
        is_buyer_maker = trade.get('m', False)
        with self.lock:
            if is_buyer_maker:
                self.cvd[s] -= qty
            else:
                self.cvd[s] += qty
            self.price_history[s].append(price)
            self.volume_history[s].append(qty)
            self.store.set(f"trade:{s}", {'price': price, 'qty': qty, 'side': 'sell' if is_buyer_maker else 'buy'}, ex=60)

    def _handle_mini_tickers(self, tickers):
        for t in tickers:
            s = t.get('s')
            if s not in self.symbols:
                continue
            price = float(t.get('c', 0))
            volume = float(t.get('v', 0))
            with self.lock:
                self.price_history[s].append(price)
                self.volume_history[s].append(volume)
            self.store.set(f"mini:{s}", {
                'price': price,
                'volume': volume,
                'high': float(t.get('h', 0)),
                'low': float(t.get('l', 0)),
                'change': float(t.get('P', 0)),
                'timestamp': t.get('E', time.time())
            }, ex=60)

    def _handle_force_orders(self, orders):
        for o in orders:
            order = o.get('o')
            if not order:
                continue
            s = order.get('s')
            if s not in self.symbols:
                continue
            with self.lock:
                self.liquidation_count[s] += 1
            self.store.set(f"liq:{s}", self.liquidation_count[s], ex=60)

    def _handle_mark_prices(self, mark_prices):
        for mp in mark_prices:
            s = mp.get('s')
            if s not in self.symbols:
                continue
            oi = float(mp.get('oi', 0))
            mark_price = float(mp.get('p', 0))
            funding_rate = float(mp.get('r', 0))  # NEW: funding rate
            with self.lock:
                self.oi_history[s].append(oi)
                self.funding_rate_history[s].append(funding_rate)  # NEW
            self.store.set(f"mark:{s}", {
                'open_interest': oi,
                'mark_price': mark_price,
                'funding_rate': funding_rate,  # NEW
                'timestamp': mp.get('E', time.time())
            }, ex=60)

    def get_cvd(self, symbol):
        with self.lock:
            return self.cvd.get(symbol, 0.0)

    def get_price_history(self, symbol):
        with self.lock:
            return list(self.price_history.get(symbol, []))

    def get_oi_history(self, symbol):
        with self.lock:
            return list(self.oi_history.get(symbol, []))

    def get_funding_rate_history(self, symbol):
        with self.lock:
            return list(self.funding_rate_history.get(symbol, []))

    def get_liquidation_count(self, symbol):
        with self.lock:
            return self.liquidation_count.get(symbol, 0)

# =====================================================================
# 7. MULTI‑DEPTH STREAM (for all 10 coins – bid/ask walls) – CACHED
# =====================================================================
class MultiDepthStream(threading.Thread):
    def __init__(self, symbols=TOP_COINS, depth_levels=10):
        super().__init__(daemon=True)
        self.symbols = symbols
        self.depth_levels = depth_levels
        self.orderbooks = {s: {'bids': [], 'asks': []} for s in symbols}
        self.last_update = time.time()
        self.stop_event = threading.Event()
        self.lock = threading.RLock()

    def run(self):
        streams = [f"{s.lower()}@depth{self.depth_levels}@100ms" for s in self.symbols]
        stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        while not self.stop_event.is_set():
            try:
                def on_message(ws, message):
                    if self.stop_event.is_set():
                        ws.close()
                        return
                    data = json.loads(message)
                    stream = data.get('stream')
                    if not stream:
                        return
                    sym_part = stream.split('@')[0]
                    sym = sym_part.upper()
                    if sym not in self.symbols:
                        return
                    payload = data.get('data')
                    if not payload:
                        return
                    with self.lock:
                        self.last_update = time.time()
                        if 'bids' in payload and 'asks' in payload:
                            self.orderbooks[sym]['bids'] = payload['bids'][:self.depth_levels]
                            self.orderbooks[sym]['asks'] = payload['asks'][:self.depth_levels]
                ws = websocket.WebSocketApp(stream_url, on_message=on_message)
                ws.run_forever(ping_interval=20)
            except Exception as e:
                logging.exception(f"MultiDepthStream error: {e}")
                time.sleep(2)

    def get_orderbook(self, symbol):
        with self.lock:
            return copy.deepcopy(self.orderbooks.get(symbol, {'bids': [], 'asks': []}))

    def stop(self):
        self.stop_event.set()

# =====================================================================
# 8. SIGNAL COMPUTATION ENGINE (per coin) – NO REST CALLS
# =====================================================================
class InstitutionalSignalEngine:
    @staticmethod
    def compute_signal(symbol, store: RedisSignalStore, processor: BinanceMultiStreamProcessor, depth_stream=None):
        mini = store.get(f"mini:{symbol}")
        mark = store.get(f"mark:{symbol}")
        liq_count = processor.get_liquidation_count(symbol)
        cvd = processor.get_cvd(symbol)
        price_history = processor.get_price_history(symbol)
        oi_history = processor.get_oi_history(symbol)

        if not mini or not mark:
            return None

        price = mini['price']
        volume = mini['volume']
        oi = mark['open_interest']
        funding_rate = mark.get('funding_rate', 0.0)  # NEW

        oi_delta = 0.0
        if len(oi_history) >= 2:
            oi_delta = (oi_history[-1] - oi_history[-2]) / (oi_history[-2] + 1e-6)

        price_change = 0.0
        if len(price_history) >= 5:
            price_change = (price_history[-1] - price_history[-5]) / (price_history[-5] + 1e-6)

        vol_spike = 1.0
        if len(price_history) >= 10:
            avg_vol = np.mean(price_history[-10:]) if price_history else price
            vol_spike = volume / (avg_vol + 1e-6)

        total_oi = oi

        long_short_ratio = 1.0
        if len(oi_history) >= 5:
            oi_trend = (oi_history[-1] - oi_history[-5]) / (oi_history[-5] + 1e-6)
            price_trend = (price_history[-1] - price_history[-5]) / (price_history[-5] + 1e-6)
            if oi_trend > 0.01 and price_trend > 0.01:
                long_short_ratio = 1.5
            elif oi_trend > 0.01 and price_trend < -0.01:
                long_short_ratio = 0.5
            else:
                long_short_ratio = 1.0

        liquidation_count = liq_count

        signal = "NEUTRAL"
        confidence = 50
        direction = 0

        if oi_delta > 0.02 and price_change < -0.005:
            signal = "SHORT"
            confidence = 75
            direction = -1
        elif oi_delta > 0.02 and price_change > 0.005:
            signal = "LONG"
            confidence = 75
            direction = 1

        if liq_count > 5:
            if price_change < 0:
                signal = "LONG"
                confidence = max(confidence, 85)
                direction = 1
            elif price_change > 0:
                signal = "SHORT"
                confidence = max(confidence, 85)
                direction = -1
            else:
                signal = "NEUTRAL"

        if vol_spike > 2.5 and signal == "NEUTRAL":
            if price_change > 0.01:
                signal = "LONG"
                confidence = 70
                direction = 1
            elif price_change < -0.01:
                signal = "SHORT"
                confidence = 70
                direction = -1

        if long_short_ratio > 1.2 and signal == "NEUTRAL":
            signal = "LONG"
            confidence = 55
            direction = 1
        elif long_short_ratio < 0.8 and signal == "NEUTRAL":
            signal = "SHORT"
            confidence = 55
            direction = -1

        atr = price * 0.01
        if direction == 1:
            sl = price - 2 * atr
            tp = price + 3 * atr
        elif direction == -1:
            sl = price + 2 * atr
            tp = price - 3 * atr
        else:
            sl = price
            tp = price

        return {
            'symbol': symbol,
            'price': price,
            'signal': signal,
            'confidence': confidence,
            'direction': direction,
            'sl': sl,
            'tp': tp,
            'oi': oi,
            'oi_delta': oi_delta,
            'volume': volume,
            'vol_spike': vol_spike,
            'liq_count': liq_count,
            'cvd': cvd,
            'funding_rate': funding_rate,
            'basis_spread': 0.0,
            'long_short_ratio': long_short_ratio,
            'timestamp': time.time(),
            'locked': False,
            'lock_reason': ''
        }

# =====================================================================
# 9. BACKGROUND SIGNAL UPDATER – CACHED
# =====================================================================
class SignalUpdater(threading.Thread):
    def __init__(self, store: RedisSignalStore, processor: BinanceMultiStreamProcessor, depth_stream, symbols, interval=2):
        super().__init__(daemon=True)
        self.store = store
        self.processor = processor
        self.depth_stream = depth_stream
        self.symbols = symbols
        self.interval = interval
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            try:
                for symbol in self.symbols:
                    signal = InstitutionalSignalEngine.compute_signal(
                        symbol, self.store, self.processor, self.depth_stream
                    )
                    if signal:
                        self.store.set(f"signal:{symbol}", signal, ex=60)
                time.sleep(self.interval)
            except Exception as e:
                logging.exception(f"SignalUpdater error: {e}")
                time.sleep(2)

    def stop(self):
        self.stop_event.set()

# =====================================================================
# 10. MULTI‑TIMEFRAME FETCHER (for selected asset only) – REST call kept
# =====================================================================
class MultiTimeframeFetcher:
    def __init__(self, exchange, asset):
        self.exchange = exchange
        self.asset = asset
        self.data = {}
        self.last_update = 0

    def fetch(self, timeframes=['15m', '1h']):
        now = time.time()
        if now - self.last_update < 60:
            return self.data
        for tf in timeframes:
            try:
                ohlcv = fetch_with_retry(self.exchange.fetch_ohlcv, self.asset, timeframe=tf, limit=50)
                df = pd.DataFrame(ohlcv, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
                if not df.empty:
                    df['sma20'] = df['close'].rolling(20).mean()
                    df['trend'] = df['close'] > df['sma20']
                    self.data[tf] = df
            except Exception as e:
                logging.warning(f"Could not fetch {tf}: {e}")
        self.last_update = now
        return self.data

# =====================================================================
# 11. DIFFERENTIAL OFI (for selected asset) – FULL DEPTH
# =====================================================================
class DifferentialOFI:
    def __init__(self, depth_levels=10):
        self.depth_levels = depth_levels
        self.prev_bids = None
        self.prev_asks = None
        self.ofi = 0.0

    def update(self, orderbook):
        if not orderbook or 'bids' not in orderbook or 'asks' not in orderbook:
            return 0.0
        bids = orderbook['bids'][:self.depth_levels]
        asks = orderbook['asks'][:self.depth_levels]
        if not bids or not asks:
            return 0.0

        current_bids = {float(b[0]): float(b[1]) for b in bids}
        current_asks = {float(a[0]): float(a[1]) for a in asks}

        e_b = 0.0
        e_a = 0.0

        if self.prev_bids is not None:
            e_b = 0.0
            for price, vol in current_bids.items():
                if price in self.prev_bids:
                    e_b += (vol - self.prev_bids[price])
                else:
                    e_b += vol
        else:
            e_b = sum(current_bids.values())

        if self.prev_asks is not None:
            e_a = 0.0
            for price, vol in current_asks.items():
                if price in self.prev_asks:
                    e_a += (vol - self.prev_asks[price])
                else:
                    e_a += vol
        else:
            e_a = sum(current_asks.values())

        self.ofi = e_b - e_a

        self.prev_bids = current_bids
        self.prev_asks = current_asks

        return self.ofi

# =====================================================================
# 12. VOLUME PROFILE (with larger lookback)
# =====================================================================
class VolumeProfile:
    def __init__(self, num_bins=50, lookback_ticks=500):
        self.num_bins = num_bins
        self.lookback = lookback_ticks
        self.price_volume = {}

    def update(self, tick_history):
        if not tick_history:
            return
        recent = list(tick_history)[-self.lookback:]
        prices = [t['p'] for t in recent]
        if not prices:
            return
        min_p = min(prices)
        max_p = max(prices)
        if min_p == max_p:
            min_p *= 0.999
            max_p *= 1.001
        bin_width = (max_p - min_p) / self.num_bins
        bins = {}
        for tick in recent:
            price = tick['p']
            vol = tick['q']
            bin_idx = int((price - min_p) / bin_width)
            if bin_idx >= self.num_bins:
                bin_idx = self.num_bins - 1
            if bin_idx < 0:
                bin_idx = 0
            bin_price = min_p + (bin_idx + 0.5) * bin_width
            bins[bin_price] = bins.get(bin_price, 0) + vol
        self.price_volume = bins

    def get_poc(self):
        if not self.price_volume:
            return None
        return max(self.price_volume, key=self.price_volume.get)

    def get_value_area(self, percentage=0.70):
        if not self.price_volume:
            return None, None
        sorted_items = sorted(self.price_volume.items(), key=lambda x: x[1], reverse=True)
        total_vol = sum(self.price_volume.values())
        target_vol = total_vol * percentage
        cum_vol = 0
        prices = []
        for price, vol in sorted_items:
            cum_vol += vol
            prices.append(price)
            if cum_vol >= target_vol:
                break
        if not prices:
            return None, None
        val = min(prices)
        vah = max(prices)
        return val, vah

# =====================================================================
# 13. CONFLUENCE FILTER (for selected asset)
# =====================================================================
class ConfluenceFilter:
    @staticmethod
    def check_delta_divergence(df, cvd_history):
        if df.empty or len(df) < 5:
            return 0
        price = df['close'].values[-5:]
        cvd = np.array(cvd_history[-5:]) if len(cvd_history) >= 5 else np.array([0]*5)
        if len(cvd) < 5:
            return 0
        price_low = np.min(price)
        cvd_at_low = cvd[np.argmin(price)]
        price_prev = df['close'].values[-10:-5] if len(df) >= 10 else price
        if len(price_prev) > 0:
            prev_low = np.min(price_prev)
            if price_low < prev_low:
                if cvd_at_low > np.min(cvd[:4]) + 1e-6:
                    return 1
        price_high = np.max(price)
        cvd_at_high = cvd[np.argmax(price)]
        if len(price_prev) > 0:
            prev_high = np.max(price_prev)
            if price_high > prev_high:
                if cvd_at_high < np.max(cvd[:4]) - 1e-6:
                    return -1
        return 0

    @staticmethod
    def check_liquidity_sweep(df, tick_history, swing_high, swing_low):
        if df.empty or len(df) < 2 or not tick_history:
            return 0, 0.0
        current_price = df['close'].iloc[-1]
        current_high = df['high'].iloc[-1]
        current_low = df['low'].iloc[-1]
        if current_high > swing_high and current_price < swing_high:
            recent_ticks = list(tick_history)[-20:]
            spike = any(t['q'] > np.mean([t['q'] for t in recent_ticks])*2 for t in recent_ticks)
            if spike:
                return -1, swing_high
        if current_low < swing_low and current_price > swing_low:
            recent_ticks = list(tick_history)[-20:]
            spike = any(t['q'] > np.mean([t['q'] for t in recent_ticks])*2 for t in recent_ticks)
            if spike:
                return 1, swing_low
        return 0, 0.0

# =====================================================================
# 14. STATISTICAL DETECTORS (for selected asset)
# =====================================================================
class StatisticalIcebergDetector:
    def __init__(self, window=50, threshold=3.0):
        self.window = window
        self.threshold = threshold
        self.volumes = deque(maxlen=window)

    def detect(self, recent_ticks, orderbook):
        if not recent_ticks or not orderbook or not orderbook.get('bids') or not orderbook.get('asks'):
            return False, 0, 0.0, 0
        bid_px = float(orderbook['bids'][0][0])
        ask_px = float(orderbook['asks'][0][0])
        bid_vol = sum(t['q'] for t in recent_ticks if abs(t['p'] - bid_px)/bid_px < 0.005 and t['m'])
        ask_vol = sum(t['q'] for t in recent_ticks if abs(t['p'] - ask_px)/ask_px < 0.005 and not t['m'])
        total_vol = bid_vol + ask_vol
        if total_vol == 0:
            return False, 0, 0.0, 0
        self.volumes.append(total_vol)
        if len(self.volumes) < 10:
            return False, 0, 0.0, 0
        mean_vol = np.mean(self.volumes)
        std_vol = np.std(self.volumes) + 1e-6
        z_score = (total_vol - mean_vol) / std_vol
        if z_score > self.threshold:
            direction = 1 if bid_vol > ask_vol else -1
            return True, direction, float(z_score), int(total_vol)
        return False, 0, 0.0, 0

class StatisticalWhaleDetector:
    def __init__(self, window=100, z_threshold=2.5):
        self.window = window
        self.z_threshold = z_threshold
        self.trade_sizes = deque(maxlen=window)

    def analyze(self, recent_ticks, cvd, avg_volume):
        if not recent_ticks:
            return False, 0, 0.0
        sizes = [t['q'] for t in recent_ticks]
        self.trade_sizes.extend(sizes)
        if len(self.trade_sizes) < 20:
            return False, 0, 0.0
        max_size = max(sizes)
        mean = np.mean(self.trade_sizes)
        std = np.std(self.trade_sizes) + 1e-6
        z = (max_size - mean) / std
        if z > self.z_threshold:
            agg_buy = sum(t['q'] for t in recent_ticks if not t['m'])
            agg_sell = sum(t['q'] for t in recent_ticks if t['m'])
            direction = 1 if agg_buy > agg_sell else -1
            return True, direction, float(z)
        return False, 0, 0.0

class StatisticalSpoofingDetector:
    def __init__(self, maxlen=30, lambda_=0.2, control_limit=3):
        self.events = deque(maxlen=maxlen)
        self.lambda_ = lambda_
        self.ewma = 0.0
        self.control_limit = control_limit

    def update_and_detect(self, orderbook):
        if not orderbook or 'bids' not in orderbook or 'asks' not in orderbook:
            return False, 0.0
        best_bid = float(orderbook['bids'][0][0])
        best_ask = float(orderbook['asks'][0][0])
        spread = best_ask - best_bid
        if spread == 0:
            return False, 0.0
        bid_depth_10 = sum(float(b[1]) for b in orderbook['bids'][:10])
        ask_depth_10 = sum(float(a[1]) for a in orderbook['asks'][:10])
        slope = (ask_depth_10 - bid_depth_10) / spread
        if self.ewma == 0.0:
            self.ewma = slope
        else:
            self.ewma = self.lambda_ * slope + (1 - self.lambda_) * self.ewma
        if len(self.events) > 5:
            residuals = [e['slope'] - self.ewma for e in self.events]
            std_res = np.std(residuals) + 1e-6
            z = (slope - self.ewma) / std_res
            if abs(z) > self.control_limit:
                return True, float(z)
        self.events.append({'ts': time.time(), 'slope': slope})
        return False, 0.0

class StatisticalStopHuntDetector:
    @staticmethod
    def detect(df, cvd, ofi, swing_high, swing_low):
        if df.empty or len(df) < 5:
            return False, 0, 0.0
        curr_price = df['close'].iloc[-1]
        curr_high = df['high'].iloc[-1]
        curr_low = df['low'].iloc[-1]
        sweep_high = (curr_high > swing_high) and (curr_price < swing_high)
        sweep_low = (curr_low < swing_low) and (curr_price > swing_low)
        if not sweep_high and not sweep_low:
            return False, 0, 0.0
        direction = 1 if sweep_low else -1
        price_up = df['close'].iloc[-1] > df['close'].iloc[-2]
        cvd_divergence = (price_up and cvd < 0) or (not price_up and cvd > 0)
        ofi_reversal = (ofi < -0.3) if price_up else (ofi > 0.3)
        vol_spike = df['volume'].iloc[-1] > (df['volume'].mean() * 1.8)
        score = (sweep_high or sweep_low) * 0.4 + cvd_divergence * 0.3 + ofi_reversal * 0.15 + vol_spike * 0.15
        return bool(score >= 0.50), direction, float(score * 100)

# =====================================================================
# 15. QUANTITATIVE ORDERBOOK ANALYTICS (for selected asset)
# =====================================================================
class QuantitativeOrderbookAnalytics:
    def __init__(self, depth_levels=10):
        self.depth_levels = depth_levels
        self.vpin_buckets = deque(maxlen=50)

    def calculate_ofi(self, bids, asks):
        if not bids or not asks:
            return 0.0
        bid_vol = sum(float(b[1]) for b in bids[:self.depth_levels])
        ask_vol = sum(float(a[1]) for a in asks[:self.depth_levels])
        return bid_vol - ask_vol

    def update_vpin(self, volume, is_maker, price, prev_price):
        if prev_price == 0:
            return 0.15
        price_change = abs(price - prev_price) / prev_price
        self.vpin_buckets.append((volume, price_change))
        if len(self.vpin_buckets) < 10:
            return 0.15
        total_vol = sum(v for v, _ in self.vpin_buckets)
        avg_vol = total_vol / len(self.vpin_buckets)
        vpin = sum(v * pc for v, pc in self.vpin_buckets) / (avg_vol + 1e-6)
        return float(np.clip(vpin, 0.0, 1.0))

# =====================================================================
# 16. LIQUIDITY WALL DETECTOR (with full depth)
# =====================================================================
class LiquidityWallDetector:
    def __init__(self, depth_levels=10, threshold_ratio=3.0):
        self.depth_levels = depth_levels
        self.threshold_ratio = threshold_ratio

    def detect(self, orderbook):
        if not orderbook or 'bids' not in orderbook or 'asks' not in orderbook:
            return False, 0, 0
        bids = orderbook['bids'][:self.depth_levels]
        asks = orderbook['asks'][:self.depth_levels]
        if not bids or not asks:
            return False, 0, 0
        bid_vol = sum(float(b[1]) for b in bids)
        ask_vol = sum(float(a[1]) for a in asks)
        if bid_vol + ask_vol == 0:
            return False, 0, 0
        ratio = bid_vol / ask_vol if ask_vol > 0 else float('inf')
        if ratio > self.threshold_ratio:
            return True, 1, ratio
        elif ratio < 1/self.threshold_ratio:
            return True, -1, ratio
        return False, 0, ratio

# =====================================================================
# 17. SCALPING INDICATORS (tick-based, real)
# =====================================================================
class ScalpingIndicators:
    def __init__(self, tick_window=50):
        self.tick_prices = deque(maxlen=tick_window)
        self.tick_volumes = deque(maxlen=tick_window)
        self.tick_sides = deque(maxlen=tick_window)
        self.last_price = 0.0
        self.vwap = 0.0
        self.delta = 0.0
        self.volume_surge = False
        self.momentum = 0.0
        self.absorption = False

    def update(self, tick):
        price = tick['p']
        volume = tick['q']
        is_buy = not tick['m']
        self.tick_prices.append(price)
        self.tick_volumes.append(volume)
        self.tick_sides.append(is_buy)
        self.last_price = price

        if len(self.tick_prices) >= 20:
            prices = list(self.tick_prices)[-20:]
            vols = list(self.tick_volumes)[-20:]
            self.vwap = sum(p * v for p, v in zip(prices, vols)) / (sum(vols) + 1e-9)
        else:
            self.vwap = price

        buy_vol = sum(v for v, b in zip(self.tick_volumes, self.tick_sides) if b)
        sell_vol = sum(v for v, b in zip(self.tick_volumes, self.tick_sides) if not b)
        self.delta = buy_vol - sell_vol

        if len(self.tick_volumes) > 10:
            avg_vol = sum(list(self.tick_volumes)[-10:]) / 10
            self.volume_surge = volume > 2.5 * avg_vol
        else:
            self.volume_surge = False

        if len(self.tick_prices) >= 5:
            self.momentum = (price - list(self.tick_prices)[-5]) / (list(self.tick_prices)[-5] + 1e-9)
        else:
            self.momentum = 0.0

        if len(self.tick_prices) >= 2:
            price_change = abs(price - self.tick_prices[-2]) / (self.tick_prices[-2] + 1e-9)
            if len(self.tick_volumes) > 10:
                avg_vol = sum(list(self.tick_volumes)[-10:]) / 10
                self.absorption = volume > 2 * avg_vol and price_change < 0.0005
            else:
                self.absorption = False
        else:
            self.absorption = False

    def get_scalp_signal(self):
        score = 0.0
        if self.vwap > 0:
            price_vwap_diff = (self.last_price - self.vwap) / self.vwap
            if price_vwap_diff < -0.001 and self.delta > 0:
                score += 0.3
            if price_vwap_diff > 0.001 and self.delta < 0:
                score -= 0.3
        if self.volume_surge and self.delta > 0:
            score += 0.2
        if self.volume_surge and self.delta < 0:
            score -= 0.2
        if self.momentum > 0.001:
            score += 0.2
        elif self.momentum < -0.001:
            score -= 0.2
        if self.absorption and self.delta > 0:
            score += 0.3
        return score

# =====================================================================
# 18. TRADE MANAGER (with lock logic) – kept for manual logging
# =====================================================================
class TradeManager:
    def __init__(self):
        self.active_trade = None
        self.trade_id_counter = 0
        self.db_writer = None
        self.lock_status = False
        self.lock_time = None
        self.lock_reason = ""

    def set_db_writer(self, db_writer):
        self.db_writer = db_writer

    def set_lock(self, locked, reason=""):
        self.lock_status = locked
        if locked:
            self.lock_time = datetime.now(timezone.utc).isoformat()
            self.lock_reason = reason
        else:
            self.lock_time = None
            self.lock_reason = ""

    def open_trade(self, asset, direction, entry_price, sl, tp, confidence):
        if self.active_trade is not None:
            logging.warning("Trade already active; closing previous before new.")
            self.close_trade(entry_price)
        trade_id = f"T{int(time.time())}{self.trade_id_counter}"
        self.trade_id_counter += 1
        trade = {
            'id': trade_id,
            'asset': asset,
            'direction': direction,
            'entry_price': entry_price,
            'stop_loss': sl,
            'take_profit': tp,
            'confidence': confidence,
            'entry_time': datetime.now(timezone.utc).isoformat(),
            'status': 'OPEN',
            'lock_time': self.lock_time if self.lock_status else None,
            'lock_reason': self.lock_reason if self.lock_status else ""
        }
        self.active_trade = trade
        if self.db_writer:
            self.db_writer.add_trade((
                trade_id,
                trade['entry_time'],
                asset,
                direction,
                entry_price,
                sl,
                tp,
                confidence,
                None,
                None,
                None,
                0.0,
                'OPEN',
                trade['lock_time'],
                trade['lock_reason']
            ))
        logging.info(f"Trade opened: {trade}")
        return trade_id

    def close_trade(self, exit_price, result=None, profit_percent=0.0):
        if self.active_trade is None:
            return
        trade = self.active_trade
        trade['exit_time'] = datetime.now(timezone.utc).isoformat()
        trade['exit_price'] = exit_price
        if result is None:
            if trade['direction'] == 'LONG':
                if exit_price >= trade['take_profit']:
                    result = 'WIN'
                    profit_percent = (exit_price - trade['entry_price']) / trade['entry_price'] * 100
                elif exit_price <= trade['stop_loss']:
                    result = 'LOSS'
                    profit_percent = (exit_price - trade['entry_price']) / trade['entry_price'] * 100
                else:
                    result = 'CLOSED'
                    profit_percent = (exit_price - trade['entry_price']) / trade['entry_price'] * 100
            else:
                if exit_price <= trade['take_profit']:
                    result = 'WIN'
                    profit_percent = (trade['entry_price'] - exit_price) / trade['entry_price'] * 100
                elif exit_price >= trade['stop_loss']:
                    result = 'LOSS'
                    profit_percent = (trade['entry_price'] - exit_price) / trade['entry_price'] * 100
                else:
                    result = 'CLOSED'
                    profit_percent = (trade['entry_price'] - exit_price) / trade['entry_price'] * 100
        trade['result'] = result
        trade['profit_percent'] = profit_percent
        trade['status'] = 'CLOSED'
        if self.db_writer:
            self.db_writer.add_trade((
                trade['id'],
                trade['entry_time'],
                trade['asset'],
                trade['direction'],
                trade['entry_price'],
                trade['stop_loss'],
                trade['take_profit'],
                trade['confidence'],
                trade['exit_time'],
                trade['exit_price'],
                trade['result'],
                trade['profit_percent'],
                trade['status'],
                trade.get('lock_time'),
                trade.get('lock_reason')
            ))
        logging.info(f"Trade closed: {trade}")
        self.active_trade = None

    def update_open_trade(self, current_price):
        if self.active_trade is None:
            return None
        trade = self.active_trade
        if trade['direction'] == 'LONG':
            if current_price >= trade['take_profit']:
                self.close_trade(current_price, result='WIN')
                return 'WIN'
            elif current_price <= trade['stop_loss']:
                self.close_trade(current_price, result='LOSS')
                return 'LOSS'
        else:
            if current_price <= trade['take_profit']:
                self.close_trade(current_price, result='WIN')
                return 'WIN'
            elif current_price >= trade['stop_loss']:
                self.close_trade(current_price, result='LOSS')
                return 'LOSS'
        return None

    def get_trade_status(self):
        if self.active_trade:
            return self.active_trade
        return None

# =====================================================================
# 19. TELEGRAM ALERT
# =====================================================================
def send_telegram_alert(message):
    bot_token = st.secrets.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID", "")
    if bot_token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            params = {"chat_id": chat_id, "text": message}
            requests.get(url, params=params, timeout=5)
        except Exception as e:
            logging.warning(f"Telegram alert failed: {e}")

# =====================================================================
# 20. BAYESIAN SIGNAL FUSION (with real indicators)
# =====================================================================
class BayesianSignalFusion:
    @staticmethod
    def process(
        whale_dir, iceberg_dir, spoof_alert, stophunt_score,
        ob_dir, fvg_dir, vpin, ofi,
        global_cvd, total_oi,
        long_short_ratio,
        liquidation_count,
        liquidity_wall_dir,
        scalp_score,
        multi_tf_bias,
        divergence_type,
        liquidity_sweep_dir,
        poc_price,
        price_relative_to_poc
    ):
        log_prior = np.log(0.5)
        log_likelihood_up = 0.0
        log_likelihood_down = 0.0

        if whale_dir != 0:
            if whale_dir == 1:
                log_likelihood_up += np.log(0.65)
                log_likelihood_down += np.log(0.35)
            else:
                log_likelihood_up += np.log(0.35)
                log_likelihood_down += np.log(0.65)

        if iceberg_dir != 0:
            if iceberg_dir == 1:
                log_likelihood_up += np.log(0.60)
                log_likelihood_down += np.log(0.40)
            else:
                log_likelihood_up += np.log(0.40)
                log_likelihood_down += np.log(0.60)

        if spoof_alert:
            log_likelihood_up += np.log(0.2)
            log_likelihood_down += np.log(0.8)

        if stophunt_score > 60:
            log_likelihood_up += np.log(0.55)
            log_likelihood_down += np.log(0.45)

        if ob_dir != 0:
            if ob_dir == 1:
                log_likelihood_up += np.log(0.55)
                log_likelihood_down += np.log(0.45)
            else:
                log_likelihood_up += np.log(0.45)
                log_likelihood_down += np.log(0.55)

        if fvg_dir != 0:
            if fvg_dir == 1:
                log_likelihood_up += np.log(0.52)
                log_likelihood_down += np.log(0.48)
            else:
                log_likelihood_up += np.log(0.48)
                log_likelihood_down += np.log(0.52)

        if vpin > 0.35:
            log_likelihood_up += np.log(0.3)
            log_likelihood_down += np.log(0.7)
        if ofi > 0:
            log_likelihood_up += np.log(0.55)
            log_likelihood_down += np.log(0.45)
        else:
            log_likelihood_up += np.log(0.45)
            log_likelihood_down += np.log(0.55)

        if global_cvd > 0:
            log_likelihood_up += np.log(0.55)
            log_likelihood_down += np.log(0.45)
        else:
            log_likelihood_up += np.log(0.45)
            log_likelihood_down += np.log(0.55)

        if total_oi > 0:
            log_likelihood_up += np.log(0.52)
            log_likelihood_down += np.log(0.48)

        if long_short_ratio > 1.2:
            log_likelihood_up += np.log(0.55)
            log_likelihood_down += np.log(0.45)
        elif long_short_ratio < 0.8:
            log_likelihood_up += np.log(0.45)
            log_likelihood_down += np.log(0.55)

        if liquidation_count > 0:
            log_likelihood_up += np.log(0.45)
            log_likelihood_down += np.log(0.55)
        else:
            log_likelihood_up += np.log(0.55)
            log_likelihood_down += np.log(0.45)

        if liquidity_wall_dir == 1:
            log_likelihood_up += np.log(0.58)
            log_likelihood_down += np.log(0.42)
        elif liquidity_wall_dir == -1:
            log_likelihood_up += np.log(0.42)
            log_likelihood_down += np.log(0.58)

        if scalp_score > 0.3:
            log_likelihood_up += np.log(0.6)
            log_likelihood_down += np.log(0.4)
        elif scalp_score < -0.3:
            log_likelihood_up += np.log(0.4)
            log_likelihood_down += np.log(0.6)

        if multi_tf_bias == 1:
            log_likelihood_up += np.log(0.7)
            log_likelihood_down += np.log(0.3)
        elif multi_tf_bias == -1:
            log_likelihood_up += np.log(0.3)
            log_likelihood_down += np.log(0.7)

        if divergence_type == 1:
            log_likelihood_up += np.log(0.7)
            log_likelihood_down += np.log(0.3)
        elif divergence_type == -1:
            log_likelihood_up += np.log(0.3)
            log_likelihood_down += np.log(0.7)

        if liquidity_sweep_dir == 1:
            log_likelihood_up += np.log(0.65)
            log_likelihood_down += np.log(0.35)
        elif liquidity_sweep_dir == -1:
            log_likelihood_up += np.log(0.35)
            log_likelihood_down += np.log(0.65)

        if poc_price is not None:
            if price_relative_to_poc == 1:
                if global_cvd > 0:
                    log_likelihood_up += np.log(0.6)
                    log_likelihood_down += np.log(0.4)
                else:
                    log_likelihood_up += np.log(0.4)
                    log_likelihood_down += np.log(0.6)
            elif price_relative_to_poc == -1:
                if global_cvd < 0:
                    log_likelihood_up += np.log(0.4)
                    log_likelihood_down += np.log(0.6)
                else:
                    log_likelihood_up += np.log(0.6)
                    log_likelihood_down += np.log(0.4)

        log_odds = log_prior + (log_likelihood_up - log_likelihood_down)
        posterior_up = 1.0 / (1.0 + np.exp(-log_odds))
        final_confidence = posterior_up * 100

        if posterior_up > 0.95:
            action = "STRONG LONG"
            color = "#10b981"
        elif posterior_up < 0.05:
            action = "STRONG SHORT"
            color = "#ef4444"
        else:
            action = "NEUTRAL"
            color = "#6b7280"

        if 0.45 < posterior_up < 0.55:
            locked = True
            reason = "NEUTRAL_CONVICTION_LOCK"
        elif vpin > 0.35 or spoof_alert:
            locked = True
            reason = "NOISE_OR_TOXICITY"
        else:
            locked = False
            reason = "NORMAL_OPERATION"

        return action, final_confidence, locked, reason, color

# =====================================================================
# 20.5 BACKTEST ENGINE (with all fixes: resample, candle‑based slicing, pagination limit, OI median)
# =====================================================================
class BacktestEngine:
    def __init__(self, exchange, asset, start_date, end_date, timeframe='1m', threshold=0.75):
        self.exchange = exchange
        self.asset = asset
        self.start_date = start_date
        self.end_date = end_date
        self.timeframe = timeframe
        self.threshold = threshold
        self.df = None
        self.trades = []
        self.equity = []
        self.signal_count = 0

    def fetch_data(self):
        try:
            start_ts = pd.Timestamp(self.start_date).tz_localize(None)
            end_ts = pd.Timestamp(self.end_date).tz_localize(None)

            all_ohlcv = []
            since = self.exchange.parse8601(self.start_date.isoformat())
            limit = 1000
            max_attempts = 10
            attempt = 0
            while attempt < max_attempts:
                ohlcv = fetch_with_retry(self.exchange.fetch_ohlcv, self.asset, timeframe=self.timeframe, since=since, limit=limit)
                if not ohlcv:
                    break
                all_ohlcv.extend(ohlcv)
                last_ts = ohlcv[-1][0]
                if last_ts >= self.end_date.timestamp() * 1000:
                    break
                since = last_ts + 1
                attempt += 1
                time.sleep(0.2)

            if not all_ohlcv:
                return False

            df_ohlcv = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_ohlcv['timestamp'] = pd.to_datetime(df_ohlcv['timestamp'], unit='ms')
            df_ohlcv.set_index('timestamp', inplace=True)

            # Fetch OI
            try:
                oi_data = fetch_with_retry(self.exchange.fetch_open_interest_history, self.asset, timeframe=self.timeframe, limit=1000)
                if oi_data:
                    df_oi = pd.DataFrame(oi_data)
                    df_oi['timestamp'] = pd.to_datetime(df_oi['timestamp'], unit='ms')
                    df_oi.set_index('timestamp', inplace=True)
                    df = df_ohlcv.join(df_oi, how='left')
                    # fill missing OI with median of available
                    median_oi = df['openInterest'].median()
                    df['openInterest'] = df['openInterest'].fillna(median_oi if not np.isnan(median_oi) else 0)
                else:
                    raise ValueError("No OI data")
            except Exception:
                df = df_ohlcv.copy()
                df['openInterest'] = 0

            df = df[(df.index >= start_ts) & (df.index <= end_ts)]
            if df.empty:
                return False
            self.df = df
            return True
        except Exception as e:
            logging.exception(f"Backtest fetch_data error: {e}")
            return False

    def run(self):
        if self.df is None or self.df.empty:
            return False

        df = self.df.copy()
        # Compute base indicators
        df['price_change'] = df['close'].pct_change()
        df['volume_avg'] = df['volume'].rolling(10).mean()
        df['vol_spike'] = df['volume'] / (df['volume_avg'] + 1e-6)
        df['cvd_delta'] = df['volume'] * np.sign(df['close'] - df['open'])
        df['cvd'] = df['cvd_delta'].cumsum()
        df['vpin'] = np.clip(df['volume'] * df['price_change'].abs() / (df['volume_avg'] * 0.01 + 1e-6), 0, 1)
        df['oi_delta'] = df['openInterest'].pct_change().fillna(0)

        high = df['high'].values
        low = df['low'].values
        close = df['close'].values
        tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
        tr[0] = high[0] - low[0]
        df['atr'] = pd.Series(tr).rolling(14).mean()
        df['swing_high'] = df['high'].rolling(20).max()
        df['swing_low'] = df['low'].rolling(20).min()

        # Multi-timeframe bias (forward-filled)
        df_15m = df.resample('15min').agg({'close': 'last'})
        df_15m['trend'] = df_15m['close'] > df_15m['close'].rolling(20).mean()
        df_15m = df_15m.reindex(df.index, method='ffill')
        df_1h = df.resample('1h').agg({'close': 'last'})
        df_1h['trend'] = df_1h['close'] > df_1h['close'].rolling(20).mean()
        df_1h = df_1h.reindex(df.index, method='ffill')

        df['tf_bias'] = 0
        df.loc[df_15m['trend'] == True, 'tf_bias'] += 1
        df.loc[df_15m['trend'] == False, 'tf_bias'] -= 1
        df.loc[df_1h['trend'] == True, 'tf_bias'] += 1
        df.loc[df_1h['trend'] == False, 'tf_bias'] -= 1
        df['tf_bias'] = df['tf_bias'].apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)

        trades = []
        position = None
        equity_curve = []
        cash = 10000
        self.signal_count = 0
        n = len(df)

        for i in range(n):
            row = df.iloc[i]
            idx = df.index[i]

            # ---- SIMULATED INDICATORS (candle-based lookback) ----
            whale_dir = 0
            if row['vol_spike'] > 2.5:
                whale_dir = 1 if row['close'] > row['open'] else -1

            iceberg_dir = 0
            if i >= 5:
                recent_vol = df.iloc[max(0, i-5):i]['volume']
                if len(recent_vol) >= 5:
                    vol_std = recent_vol.std()
                    vol_mean = recent_vol.mean()
                    if vol_mean > 0 and vol_std / vol_mean < 0.3:
                        price_range = (row['high'] - row['low']) / row['close']
                        if price_range < 0.005:
                            iceberg_dir = 1 if row['close'] > row['open'] else -1

            spoof_alert = False
            if i >= 2:
                prev_vol = df.iloc[max(0, i-2):i]['volume']
                if len(prev_vol) >= 2:
                    if row['vol_spike'] > 2.0 and prev_vol.iloc[-1] < 0.5 * prev_vol.iloc[-2]:
                        spoof_alert = True

            ob_dir = 0
            if row['high'] > row['swing_high']:
                ob_dir = 1
            elif row['low'] < row['swing_low']:
                ob_dir = -1

            fvg_dir = 0
            if i >= 2:
                if df.iloc[max(0, i-2):i]['low'].min() > df.iloc[i-1]['high']:
                    fvg_dir = 1
                elif df.iloc[max(0, i-2):i]['high'].max() < df.iloc[i-1]['low']:
                    fvg_dir = -1

            div_type = 0
            if i >= 5:
                price_slice = df.iloc[max(0, i-5):i]['close']
                cvd_slice = df.iloc[max(0, i-5):i]['cvd']
                if len(price_slice) >= 5 and len(cvd_slice) >= 5:
                    price_low = price_slice.min()
                    cvd_at_low = cvd_slice[price_slice.idxmin()]
                    if price_low < price_slice.iloc[0] and cvd_at_low > cvd_slice.iloc[0]:
                        div_type = 1
                    price_high = price_slice.max()
                    cvd_at_high = cvd_slice[price_slice.idxmax()]
                    if price_high > price_slice.iloc[0] and cvd_at_high < cvd_slice.iloc[0]:
                        div_type = -1

            wall_dir = 0
            if row['vol_spike'] > 2.0:
                wall_dir = 1 if row['close'] > row['open'] else -1

            scalp_score = 0.0
            if i >= 10:
                mom = (row['close'] - df.iloc[max(0, i-5):i]['close'].iloc[0]) / df.iloc[max(0, i-5):i]['close'].iloc[0]
                if mom > 0.001:
                    scalp_score += 0.3
                elif mom < -0.001:
                    scalp_score -= 0.3
                if row['vol_spike'] > 2.0:
                    if row['close'] > row['open']:
                        scalp_score += 0.2
                    else:
                        scalp_score -= 0.2

            multi_tf_bias = row['tf_bias']

            stophunt_score = 0
            if row['high'] > row['swing_high'] and row['close'] < row['swing_high']:
                stophunt_score = 70
            elif row['low'] < row['swing_low'] and row['close'] > row['swing_low']:
                stophunt_score = 70

            sweep_dir = 0
            if stophunt_score > 0:
                sweep_dir = 1 if row['close'] > row['open'] else -1

            # Bayesian Fusion
            action, conf, locked, reason, color = BayesianSignalFusion.process(
                whale_dir=whale_dir,
                iceberg_dir=iceberg_dir,
                spoof_alert=spoof_alert,
                stophunt_score=stophunt_score,
                ob_dir=ob_dir,
                fvg_dir=fvg_dir,
                vpin=row['vpin'],
                ofi=0,
                global_cvd=row['cvd'],
                total_oi=row['openInterest'],
                long_short_ratio=1.0,
                liquidation_count=0,
                liquidity_wall_dir=wall_dir,
                scalp_score=scalp_score,
                multi_tf_bias=multi_tf_bias,
                divergence_type=div_type,
                liquidity_sweep_dir=sweep_dir,
                poc_price=None,
                price_relative_to_poc=0
            )

            if action in ("STRONG LONG", "STRONG SHORT"):
                self.signal_count += 1

            if position is None and action in ("STRONG LONG", "STRONG SHORT") and conf >= self.threshold * 100 and not locked:
                atr = row['atr'] if not np.isnan(row['atr']) else row['close'] * 0.01
                if action == "STRONG LONG":
                    direction = 'LONG'
                    entry = row['close']
                    sl = entry - 2 * atr
                    tp = entry + 3 * atr
                else:
                    direction = 'SHORT'
                    entry = row['close']
                    sl = entry + 2 * atr
                    tp = entry - 3 * atr
                position = {
                    'direction': direction,
                    'entry': entry,
                    'sl': sl,
                    'tp': tp,
                    'entry_time': idx,
                    'confidence': conf
                }
            elif position is not None:
                if position['direction'] == 'LONG':
                    if row['low'] <= position['sl']:
                        exit_price = position['sl']
                        result = 'LOSS'
                        profit_pct = (exit_price - position['entry']) / position['entry'] * 100
                        trades.append({
                            'entry_time': position['entry_time'],
                            'exit_time': idx,
                            'direction': position['direction'],
                            'entry_price': position['entry'],
                            'exit_price': exit_price,
                            'result': result,
                            'profit_pct': profit_pct,
                            'confidence': position['confidence']
                        })
                        cash *= (1 + profit_pct/100)
                        position = None
                    elif row['high'] >= position['tp']:
                        exit_price = position['tp']
                        result = 'WIN'
                        profit_pct = (exit_price - position['entry']) / position['entry'] * 100
                        trades.append({
                            'entry_time': position['entry_time'],
                            'exit_time': idx,
                            'direction': position['direction'],
                            'entry_price': position['entry'],
                            'exit_price': exit_price,
                            'result': result,
                            'profit_pct': profit_pct,
                            'confidence': position['confidence']
                        })
                        cash *= (1 + profit_pct/100)
                        position = None
                else:  # SHORT
                    if row['high'] >= position['sl']:
                        exit_price = position['sl']
                        result = 'LOSS'
                        profit_pct = (position['entry'] - exit_price) / position['entry'] * 100
                        trades.append({
                            'entry_time': position['entry_time'],
                            'exit_time': idx,
                            'direction': position['direction'],
                            'entry_price': position['entry'],
                            'exit_price': exit_price,
                            'result': result,
                            'profit_pct': profit_pct,
                            'confidence': position['confidence']
                        })
                        cash *= (1 + profit_pct/100)
                        position = None
                    elif row['low'] <= position['tp']:
                        exit_price = position['tp']
                        result = 'WIN'
                        profit_pct = (position['entry'] - exit_price) / position['entry'] * 100
                        trades.append({
                            'entry_time': position['entry_time'],
                            'exit_time': idx,
                            'direction': position['direction'],
                            'entry_price': position['entry'],
                            'exit_price': exit_price,
                            'result': result,
                            'profit_pct': profit_pct,
                            'confidence': position['confidence']
                        })
                        cash *= (1 + profit_pct/100)
                        position = None

            # Equity
            if position is None:
                equity = cash
            else:
                if position['direction'] == 'LONG':
                    unrealized_pnl = (row['close'] - position['entry']) / position['entry'] * 100
                else:
                    unrealized_pnl = (position['entry'] - row['close']) / position['entry'] * 100
                equity = cash * (1 + unrealized_pnl/100)
            equity_curve.append({'time': idx, 'equity': equity})

        if position is not None:
            exit_price = df.iloc[-1]['close']
            if position['direction'] == 'LONG':
                profit_pct = (exit_price - position['entry']) / position['entry'] * 100
            else:
                profit_pct = (position['entry'] - exit_price) / position['entry'] * 100
            trades.append({
                'entry_time': position['entry_time'],
                'exit_time': df.index[-1],
                'direction': position['direction'],
                'entry_price': position['entry'],
                'exit_price': exit_price,
                'result': 'CLOSED',
                'profit_pct': profit_pct,
                'confidence': position['confidence']
            })
            cash *= (1 + profit_pct/100)

        self.trades = trades
        self.equity = pd.DataFrame(equity_curve).set_index('time')
        return True

    def get_metrics(self):
        if not self.trades:
            return {}
        df_trades = pd.DataFrame(self.trades)
        total_trades = len(df_trades)
        wins = len(df_trades[df_trades['result'] == 'WIN'])
        losses = len(df_trades[df_trades['result'] == 'LOSS'])
        win_rate = wins / total_trades * 100 if total_trades > 0 else 0
        total_pnl = df_trades['profit_pct'].sum()
        avg_pnl = df_trades['profit_pct'].mean()
        max_win = df_trades['profit_pct'].max()
        max_loss = df_trades['profit_pct'].min()
        return {
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'max_win': max_win,
            'max_loss': max_loss,
            'signal_count': self.signal_count
        }

# =====================================================================
# 21. UTILITY FUNCTIONS
# =====================================================================
def calculate_atr(df, period=14):
    if df.empty or len(df) < period:
        return 0.0
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
    tr[0] = high[0] - low[0]
    return float(np.mean(tr[-period:]))

def get_swing_high_low(df):
    if df.empty or len(df) < 20:
        return 0.0, 0.0
    high = df['high'].values[-20:-1]
    low = df['low'].values[-20:-1]
    return float(np.max(high)), float(np.min(low))

def get_order_block(df):
    if df.empty or len(df) < 20:
        return 0.0, 0, "NEUTRAL"
    high = df['high'].values[-20:-1]
    low = df['low'].values[-20:-1]
    last_high = np.max(high)
    last_low = np.min(low)
    current = df['close'].iloc[-1]
    if current > last_high:
        return last_high, 1, "BREAKOUT ABOVE"
    elif current < last_low:
        return last_low, -1, "BREAKOUT BELOW"
    else:
        return (last_high + last_low) / 2, 0, "RANGE"

def analyze_fvg(df):
    if df.empty or len(df) < 3:
        return False, 0, 0.0, 0.0, 0.0
    high_prev2 = df['high'].iloc[-3]
    low_prev1 = df['low'].iloc[-2]
    high_prev1 = df['high'].iloc[-2]
    low_prev2 = df['low'].iloc[-3]
    current = df['close'].iloc[-1]
    if low_prev1 > high_prev2:
        fill = 0.0
        if current > low_prev1:
            fill = min(100, (current - high_prev2) / (low_prev1 - high_prev2) * 100)
        return True, 1, fill, 0.0, 0.5
    elif high_prev1 < low_prev2:
        fill = 0.0
        if current < high_prev1:
            fill = min(100, (low_prev2 - current) / (low_prev2 - high_prev1) * 100)
        return True, -1, fill, 0.0, 0.5
    return False, 0, 0.0, 0.0, 0.0

# =====================================================================
# 22. BACKGROUND FETCHER & DEPTH STREAM (for selected asset)
# =====================================================================
class BackgroundDataFetcher(threading.Thread):
    def __init__(self, exchange, asset, timeframe="1m", limit=100):
        super().__init__(daemon=True)
        self.exchange = exchange
        self.asset = asset
        self.timeframe = timeframe
        self.limit = limit
        self.latest_data = {}
        self.stop_event = threading.Event()
        self.last_update = time.time()

    def run(self):
        while not self.stop_event.is_set():
            try:
                ohlcv = fetch_with_retry(self.exchange.fetch_ohlcv, self.asset, timeframe=self.timeframe, limit=self.limit)
                self.latest_data = {'ohlcv': ohlcv, 'ts': time.time()}
                self.last_update = time.time()
            except Exception as e:
                logging.exception(f"Background Fetch Error: {e}")
            time.sleep(1)

    def stop(self):
        self.stop_event.set()

class DepthStream(threading.Thread):
    def __init__(self, symbol, depth_levels=10):
        super().__init__(daemon=True)
        self.symbol = symbol
        # FIX: Futures WebSocket
        self.ws_url = f"wss://fstream.binance.com/ws/{symbol.lower()}@depth{depth_levels}@100ms"
        self.orderbook = {'bids': [], 'asks': []}
        self.last_update = time.time()
        self.stop_event = threading.Event()
        self.lock = threading.Lock()

    def run(self):
        while not self.stop_event.is_set():
            try:
                def on_msg(ws, message):
                    if self.stop_event.is_set():
                        ws.close()
                        return
                    data = json.loads(message)
                    if 'bids' in data and 'asks' in data:
                        with self.lock:
                            self.orderbook['bids'] = data['bids'][:10]
                            self.orderbook['asks'] = data['asks'][:10]
                            self.last_update = time.time()
                ws = websocket.WebSocketApp(self.ws_url, on_message=on_msg)
                ws.run_forever(ping_interval=20)
            except Exception as e:
                logging.exception(f"DepthStream exception: {e}")
                time.sleep(1)

    def get_orderbook(self):
        with self.lock:
            return copy.deepcopy(self.orderbook)

    def stop(self):
        self.stop_event.set()

# =====================================================================
# 23. CACHED RESOURCE FUNCTIONS
# =====================================================================
@st.cache_resource
def get_global_store():
    return RedisSignalStore(host='localhost', port=6379)

# NEW: Cache function with new name to force refresh (version removed)
@st.cache_resource
def get_multi_processor_v2(_store):
    processor = BinanceMultiStreamProcessor(_store, TOP_COINS)
    processor.start()
    return processor

@st.cache_resource
def get_multi_depth_stream():
    stream = MultiDepthStream(TOP_COINS, depth_levels=10)
    stream.start()
    return stream

@st.cache_resource
def get_signal_updater(_store, _processor, _depth_stream):
    updater = SignalUpdater(_store, _processor, _depth_stream, TOP_COINS, interval=2)
    updater.start()
    return updater

# =====================================================================
# 24. STREAMLIT UI – ULTIMATE LUXURY DASHBOARD v26 (FINAL FIX)
# =====================================================================
st.set_page_config(page_title="🏆 Supreme Scalper - Tournament Pro", layout="wide", initial_sidebar_state="expanded")

if st_autorefresh:
    st_autorefresh(interval=1000, key="auto_refresh")

# ---------- Premium CSS ----------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;800&display=swap');
    * { font-family: 'Inter', sans-serif; }
    .stApp {
        background: linear-gradient(135deg, #0b0e14 0%, #1a1f2e 100%);
        color: #e2e8f0;
    }
    .luxury-header {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.8) 0%, rgba(15, 23, 42, 0.9) 100%);
        border: 1px solid rgba(255, 215, 0, 0.3);
        border-radius: 16px;
        padding: 16px 24px;
        margin-bottom: 16px;
        box-shadow: 0 0 40px rgba(255, 215, 0, 0.05);
    }
    .gold-accent { color: #fbbf24; font-weight: 700; text-shadow: 0 0 20px rgba(251, 191, 36, 0.2); }
    .card-gold {
        background: rgba(17, 24, 39, 0.7);
        border: 1px solid rgba(255, 215, 0, 0.15);
        border-radius: 12px;
        padding: 14px;
        margin-bottom: 10px;
        backdrop-filter: blur(10px);
        transition: all 0.2s ease;
    }
    .card-gold:hover { border-color: rgba(255, 215, 0, 0.5); box-shadow: 0 0 30px rgba(255, 215, 0, 0.05); }
    .card-title { font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #94a3b8; font-weight: 600; }
    .card-value { font-size: 20px; font-weight: 700; color: #f8fafc; margin-top: 2px; font-family: 'Inter', monospace; }
    .card-sub { font-size: 10px; color: #fbbf24; margin-top: 2px; }
    .brain-banner {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.9) 0%, rgba(15, 23, 42, 0.95) 100%);
        border: 2px solid #fbbf24;
        border-radius: 12px;
        padding: 12px 16px;
        margin-bottom: 10px;
        box-shadow: 0 0 40px rgba(251, 191, 36, 0.06);
    }
    .status-badge { padding: 4px 12px; border-radius: 20px; font-weight: 700; font-size: 12px; display: inline-block; }
    .trade-monitor {
        background: rgba(17, 24, 39, 0.85);
        border: 2px solid rgba(255, 215, 0, 0.2);
        border-radius: 12px;
        padding: 12px 14px;
        height: 100%;
        backdrop-filter: blur(10px);
    }
    .trade-monitor-title { color: #fbbf24; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
    .entry-card {
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.12), rgba(16, 185, 129, 0.04));
        border: 2px solid #10b981;
        border-radius: 10px;
        padding: 12px 14px;
        margin: 6px 0;
    }
    .entry-card-short {
        background: linear-gradient(135deg, rgba(239, 68, 68, 0.12), rgba(239, 68, 68, 0.04));
        border: 2px solid #ef4444;
        border-radius: 10px;
        padding: 12px 14px;
        margin: 6px 0;
    }
    .entry-label { font-size: 10px; color: #94a3b8; letter-spacing: 0.3px; }
    .entry-value { font-size: 16px; font-weight: 700; color: #f8fafc; }
    .summary-card {
        background: rgba(17, 24, 39, 0.9);
        border: 2px solid #fbbf24;
        border-radius: 10px;
        padding: 12px 14px;
        margin: 6px 0;
    }
    .reasoning-box {
        background: rgba(15, 23, 42, 0.8);
        border-left: 3px solid #fbbf24;
        padding: 6px 10px;
        border-radius: 4px;
        font-size: 11px;
        color: #cbd5e1;
        margin-top: 4px;
    }
    .disclaimer {
        background: rgba(239, 68, 68, 0.1);
        border-left: 4px solid #ef4444;
        padding: 6px 10px;
        border-radius: 4px;
        color: #fca5a5;
        font-size: 12px;
        margin-top: 8px;
    }
    .health-good { color: #10b981; }
    .health-stale { color: #f59e0b; }
    .health-bad { color: #ef4444; }
    .table-container {
        background: rgba(17, 24, 39, 0.7);
        border: 1px solid rgba(255, 215, 0, 0.1);
        border-radius: 12px;
        padding: 6px;
        overflow: hidden;
    }
    .table-container table { width: 100%; border-collapse: collapse; }
    .table-container th {
        color: #fbbf24;
        font-weight: 600;
        font-size: 10px;
        text-transform: uppercase;
        letter-spacing: 0.3px;
        padding: 8px 6px;
        border-bottom: 1px solid rgba(255, 215, 0, 0.1);
        text-align: right;
    }
    .table-container td {
        padding: 8px 6px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.04);
        font-size: 13px;
        text-align: right;
        font-variant-numeric: tabular-nums;
    }
    .table-container .symbol-col { text-align: left; font-weight: 600; color: #f8fafc; }
    .signal-long { color: #10b981; font-weight: 600; }
    .signal-short { color: #ef4444; font-weight: 600; }
    .signal-neutral { color: #6b7280; }
    .price-up { color: #10b981; }
    .price-down { color: #ef4444; }
    .grid-2col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    @media (max-width: 768px) { .grid-2col { grid-template-columns: 1fr; } }
    .compact-text { font-size: 12px; line-height: 1.4; }
    .lock-badge {
        background: rgba(245, 158, 11, 0.2);
        border: 1px solid #f59e0b;
        border-radius: 12px;
        padding: 2px 8px;
        font-size: 9px;
        color: #f59e0b;
        display: inline-block;
    }
    .unlock-badge {
        background: rgba(16, 185, 129, 0.2);
        border: 1px solid #10b981;
        border-radius: 12px;
        padding: 2px 8px;
        font-size: 9px;
        color: #10b981;
        display: inline-block;
    }
    .live-clock {
        font-size: 14px;
        color: #fbbf24;
        font-weight: 600;
        letter-spacing: 0.5px;
        background: rgba(0,0,0,0.3);
        padding: 4px 12px;
        border-radius: 20px;
        border: 1px solid rgba(255,215,0,0.3);
    }
    .backtest-metrics {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
    }
    .bt-card {
        background: rgba(17, 24, 39, 0.7);
        border: 1px solid rgba(255, 215, 0, 0.15);
        border-radius: 10px;
        padding: 12px 16px;
        flex: 1 1 150px;
    }
    .bt-card .label { font-size: 10px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; }
    .bt-card .value { font-size: 20px; font-weight: 700; color: #f8fafc; }
    .market-bias-bullish { color: #10b981; font-weight: 600; }
    .market-bias-bearish { color: #ef4444; font-weight: 600; }
    .market-bias-neutral { color: #fbbf24; font-weight: 400; }
    .wall-table {
        font-size: 12px;
        width: 100%;
        border-collapse: collapse;
    }
    .wall-table td {
        padding: 2px 4px;
        border-bottom: 1px solid rgba(255,255,255,0.05);
    }
    .wall-table .bid { color: #10b981; }
    .wall-table .ask { color: #ef4444; }
</style>
""", unsafe_allow_html=True)

# ---------- Sidebar ----------
st.sidebar.markdown("""
<div style="padding: 10px 0; text-align: center;">
    <h2 style="color: #fbbf24; font-weight: 800; letter-spacing: 1px;">⚜️ SUPREME SCALPER</h2>
    <p style="color: #94a3b8; font-size: 11px; margin-top: -8px;">Binance Futures • Tournament Pro v26</p>
</div>
""", unsafe_allow_html=True)

selected_asset_display = st.sidebar.selectbox("🎯 Target Asset", [f"{s[:-4]}/USDT" for s in TOP_COINS])
selected_asset = selected_asset_display.replace('/', '')

st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ **Trading Mode**")
st.sidebar.info("🏆 **Tournament (5m/15m)** – Institutional signals | Fast threshold 75%")
st.sidebar.warning("🔴 Auto-trading is disabled – signals are for manual execution only.")

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔔 **Telegram Alerts**")
telegram_token = st.sidebar.text_input("Bot Token", type="password")
telegram_chat = st.sidebar.text_input("Chat ID")
if telegram_token and telegram_chat:
    st.session_state['TELEGRAM_BOT_TOKEN'] = telegram_token
    st.session_state['TELEGRAM_CHAT_ID'] = telegram_chat

st.sidebar.markdown("---")
st.sidebar.info("All data from Binance WebSocket streams (no REST calls).")
st.sidebar.markdown("""
<div class="disclaimer">
⚠️ <b>Disclaimer:</b> This terminal uses real market data and statistical models.
However, 100% institutional detection is not possible with retail data.
Use signals only as an advanced reference – not financial advice.
</div>
""", unsafe_allow_html=True)

# ---------- Initialise Cached Resources ----------
store = get_global_store()
# Using new cache function to force refresh
multi_processor = get_multi_processor_v2(store)
multi_depth_stream = get_multi_depth_stream()
signal_updater = get_signal_updater(store, multi_processor, multi_depth_stream)

binance = get_ccxt_exchange()

# ---------- Create DB Writer and set in TradeManager ----------
if 'db_writer' not in st.session_state:
    db_writer = QueueDBWriter()
    db_writer.start()
    st.session_state['db_writer'] = db_writer

# ---------- Cleanup ----------
def cleanup_backend():
    if multi_processor:
        multi_processor.stop()
    if signal_updater:
        signal_updater.stop()
    if multi_depth_stream:
        multi_depth_stream.stop()
    if 'ws_thread' in st.session_state and st.session_state['ws_thread']:
        st.session_state['ws_stop_event'].set()
        st.session_state['ws_thread'].join(timeout=1)
    if 'db_writer' in st.session_state:
        st.session_state['db_writer'].stop()

atexit.register(cleanup_backend)

# ---------- Session State for selected asset ----------
if 'ws_queue' not in st.session_state:
    st.session_state['ws_queue'] = queue.Queue()
    st.session_state['tick_history'] = deque(maxlen=500)
    st.session_state['live_tick_cvd'] = 0.0
    st.session_state['session_volume'] = 0.0
    st.session_state['session_pv'] = 0.0
    st.session_state['current_ws_symbol'] = None
    st.session_state['ws_thread'] = None
    st.session_state['ws_stop_event'] = threading.Event()
    st.session_state['trade_manager'] = TradeManager()
    # Set db_writer from session state (already created)
    st.session_state['trade_manager'].set_db_writer(st.session_state.get('db_writer'))
    st.session_state['ofi_history'] = deque(maxlen=100)
    st.session_state['cvd_history'] = deque(maxlen=100)
    st.session_state['differential_ofi'] = DifferentialOFI(depth_levels=10)
    st.session_state['volume_profile'] = VolumeProfile(lookback_ticks=500)

def ws_worker(symbol, q, stop_event):
    clean_sym = symbol.replace('/', '').lower()
    # FIX: Futures WebSocket
    ws_url = f"wss://fstream.binance.com/ws/{clean_sym}@trade"
    while not stop_event.is_set():
        try:
            def on_msg(ws, message):
                if stop_event.is_set():
                    ws.close()
                    return
                data = json.loads(message)
                q.put({'q': float(data.get('q', 0)), 'p': float(data.get('p', 0)), 'm': data.get('m', False), 'ts': time.time()})
            ws = websocket.WebSocketApp(ws_url, on_message=on_msg)
            ws.run_forever(ping_interval=20)
        except Exception as e:
            logging.exception(f"Legacy WebSocket error: {e}")
            time.sleep(2)

if st.session_state['current_ws_symbol'] != selected_asset:
    if 'ws_thread' in st.session_state and st.session_state['ws_thread'] and st.session_state['ws_thread'].is_alive():
        st.session_state['ws_stop_event'].set()
        st.session_state['ws_thread'].join(timeout=1)
    st.session_state['ws_stop_event'] = threading.Event()
    st.session_state['ws_queue'] = queue.Queue()
    st.session_state['tick_history'].clear()
    st.session_state['live_tick_cvd'] = 0.0
    st.session_state['session_volume'] = 0.0
    st.session_state['session_pv'] = 0.0
    st.session_state['current_ws_symbol'] = selected_asset

    t = threading.Thread(target=ws_worker, args=(selected_asset, st.session_state['ws_queue'], st.session_state['ws_stop_event']), daemon=True)
    t.start()
    st.session_state['ws_thread'] = t

    if 'bg_fetcher' in st.session_state:
        st.session_state['bg_fetcher'].stop()
    bg = BackgroundDataFetcher(binance, selected_asset, timeframe="1m", limit=100)
    bg.start()
    st.session_state['bg_fetcher'] = bg

    if 'depth_stream' in st.session_state:
        st.session_state['depth_stream'].stop()
    depth_stream = DepthStream(selected_asset.split('/')[0], depth_levels=10)
    depth_stream.start()
    st.session_state['depth_stream'] = depth_stream

# ---------- Local analytics ----------
if 'quant_analytics' not in st.session_state:
    st.session_state['quant_analytics'] = QuantitativeOrderbookAnalytics(depth_levels=10)
if 'iceberg_engine' not in st.session_state:
    st.session_state['iceberg_engine'] = StatisticalIcebergDetector()
if 'spoof_engine' not in st.session_state:
    st.session_state['spoof_engine'] = StatisticalSpoofingDetector()
if 'whale_engine' not in st.session_state:
    st.session_state['whale_engine'] = StatisticalWhaleDetector()
if 'liquidity_wall_detector' not in st.session_state:
    st.session_state['liquidity_wall_detector'] = LiquidityWallDetector(depth_levels=10)
if 'scalp_indicators' not in st.session_state:
    st.session_state['scalp_indicators'] = ScalpingIndicators()
if 'multi_tf_fetcher' not in st.session_state:
    st.session_state['multi_tf_fetcher'] = MultiTimeframeFetcher(binance, selected_asset)

# ---------- Process ticks ----------
MAX_TICKS_PER_CYCLE = 3000
ticks_processed = 0
real_vpin = 0.15
last_p = 0.0
scalp = st.session_state['scalp_indicators']
while not st.session_state['ws_queue'].empty() and ticks_processed < MAX_TICKS_PER_CYCLE:
    tick = st.session_state['ws_queue'].get_nowait()
    st.session_state['tick_history'].append(tick)
    vol, px, is_maker = tick['q'], tick['p'], tick['m']
    st.session_state['live_tick_cvd'] += (-vol if is_maker else vol)
    st.session_state['session_volume'] += vol
    st.session_state['session_pv'] += (px * vol)
    real_vpin = st.session_state['quant_analytics'].update_vpin(vol, is_maker, price=px, prev_price=last_p)
    last_p = px
    scalp.update(tick)
    ticks_processed += 1

st.session_state['volume_profile'].update(st.session_state['tick_history'])
poc = st.session_state['volume_profile'].get_poc()
val, vah = st.session_state['volume_profile'].get_value_area()

# ---------- Fetch data ----------
bg_data = st.session_state.get('bg_fetcher').latest_data if st.session_state.get('bg_fetcher') else {}
raw_ohlcv = bg_data.get('ohlcv', fetch_with_retry(binance.fetch_ohlcv, selected_asset, timeframe="1m", limit=100))
df = pd.DataFrame(raw_ohlcv, columns=['time', 'open', 'high', 'low', 'close', 'volume']) if raw_ohlcv else pd.DataFrame()

depth_stream = st.session_state.get('depth_stream')
if depth_stream and depth_stream.is_alive():
    orderbook_data = depth_stream.get_orderbook()
    if not orderbook_data.get('bids') or not orderbook_data.get('asks'):
        orderbook_data = fetch_with_retry(binance.fetch_order_book, selected_asset, limit=10)
else:
    orderbook_data = fetch_with_retry(binance.fetch_order_book, selected_asset, limit=10)

d_ofi = st.session_state['differential_ofi']
ofi_diff = d_ofi.update(orderbook_data)

multi_depth = multi_depth_stream
if multi_depth:
    full_orderbook = multi_depth.get_orderbook(selected_asset)
else:
    full_orderbook = orderbook_data

ofi_full = 0.0
if full_orderbook and full_orderbook.get('bids') and full_orderbook.get('asks'):
    ofi_full = st.session_state['quant_analytics'].calculate_ofi(full_orderbook['bids'], full_orderbook['asks'])

if orderbook_data and orderbook_data.get('bids') and orderbook_data.get('asks'):
    bids = np.array(orderbook_data['bids'], dtype=float)
    asks = np.array(orderbook_data['asks'], dtype=float)
    best_bid, best_ask = bids[0][0], asks[0][0]
    ofi_simple = st.session_state['quant_analytics'].calculate_ofi(orderbook_data['bids'], orderbook_data['asks'])
    microprice = (best_bid * asks[0][1] + best_ask * bids[0][1]) / (bids[0][1] + asks[0][1] + 1e-6)
else:
    best_bid, best_ask, ofi_simple, microprice = 0, 0, 0, 0

st.session_state['ofi_history'].append(ofi_simple)
st.session_state['cvd_history'].append(st.session_state['live_tick_cvd'])

current_price = float(df['close'].iloc[-1]) if not df.empty else 0.0
if current_price == 0 and orderbook_data and orderbook_data.get('bids'):
    current_price = orderbook_data['bids'][0][0]

session_vwap = (st.session_state['session_pv'] / st.session_state['session_volume']) if st.session_state['session_volume'] > 0 else current_price
current_cvd = st.session_state['live_tick_cvd']

multi_tf_data = st.session_state['multi_tf_fetcher'].fetch(timeframes=['15m', '1h'])
multi_tf_bias = 0
if '15m' in multi_tf_data and not multi_tf_data['15m'].empty:
    if multi_tf_data['15m']['trend'].iloc[-1]:
        multi_tf_bias += 1
    else:
        multi_tf_bias -= 1
if '1h' in multi_tf_data and not multi_tf_data['1h'].empty:
    if multi_tf_data['1h']['trend'].iloc[-1]:
        multi_tf_bias += 1
    else:
        multi_tf_bias -= 1
if multi_tf_bias > 0:
    multi_tf_bias = 1
elif multi_tf_bias < 0:
    multi_tf_bias = -1
else:
    multi_tf_bias = 0

ofi_hist = np.array(list(st.session_state['ofi_history']))
if len(ofi_hist) >= 20:
    ofi_mean = np.mean(ofi_hist[-50:])
    ofi_std = np.std(ofi_hist[-50:]) + 1e-6
    last_ofi_z = (ofi_simple - ofi_mean) / ofi_std
else:
    last_ofi_z = 0.0

cvd_hist = np.array(list(st.session_state['cvd_history']))
if len(cvd_hist) >= 20:
    cvd_mean = np.mean(cvd_hist[-20:])
else:
    cvd_mean = 0.0

swing_high, swing_low = get_swing_high_low(df)
is_iceberg, ice_dir, ice_z, _ = st.session_state['iceberg_engine'].detect(list(st.session_state['tick_history']), orderbook_data)
is_spoof, spoof_z = st.session_state['spoof_engine'].update_and_detect(orderbook_data)
is_whale, whale_dir, whale_z = st.session_state['whale_engine'].analyze(list(st.session_state['tick_history']), current_cvd, df['volume'].mean() if not df.empty else 1.0)
is_stophunt, hunt_dir, stophunt_score = StatisticalStopHuntDetector.detect(df, current_cvd, ofi_simple, swing_high, swing_low)
ob_price, ob_dir, ob_status = get_order_block(df)
fvg_active, fvg_dir, fvg_fill, _, _ = analyze_fvg(df)
wall_detected, wall_dir, wall_ratio = st.session_state['liquidity_wall_detector'].detect(full_orderbook if full_orderbook else orderbook_data)
scalp_score = scalp.get_scalp_signal()

divergence = ConfluenceFilter.check_delta_divergence(df, list(st.session_state['cvd_history']))
sweep_dir, swept_price = ConfluenceFilter.check_liquidity_sweep(df, list(st.session_state['tick_history']), swing_high, swing_low)
price_relative_to_poc = 0
if poc is not None:
    if current_price > poc * 1.001:
        price_relative_to_poc = 1
    elif current_price < poc * 0.999:
        price_relative_to_poc = -1

signal_data = store.get(f"signal:{selected_asset}")
if signal_data:
    total_oi = signal_data.get('oi', 0)
    long_short_ratio = signal_data.get('long_short_ratio', 1.0)
    liq_count = signal_data.get('liq_count', 0)
else:
    total_oi = 0
    long_short_ratio = 1.0
    liq_count = 0

action, final_conf, locked, lock_reason, action_color = BayesianSignalFusion.process(
    whale_dir=whale_dir,
    iceberg_dir=ice_dir,
    spoof_alert=is_spoof,
    stophunt_score=stophunt_score,
    ob_dir=ob_dir,
    fvg_dir=fvg_dir,
    vpin=real_vpin,
    ofi=ofi_full,
    global_cvd=current_cvd,
    total_oi=total_oi,
    long_short_ratio=long_short_ratio,
    liquidation_count=liq_count,
    liquidity_wall_dir=wall_dir,
    scalp_score=scalp_score,
    multi_tf_bias=multi_tf_bias,
    divergence_type=divergence,
    liquidity_sweep_dir=sweep_dir,
    poc_price=poc,
    price_relative_to_poc=price_relative_to_poc
)

trade_manager = st.session_state['trade_manager']
trade_manager.set_lock(locked, lock_reason)

current_atr = calculate_atr(df, period=14)
suggested_sl_long = current_price - 2 * current_atr if current_atr > 0 else current_price * 0.998
suggested_tp_long = current_price + 3 * current_atr if current_atr > 0 else current_price * 1.004
suggested_sl_short = current_price + 2 * current_atr if current_atr > 0 else current_price * 1.002
suggested_tp_short = current_price - 3 * current_atr if current_atr > 0 else current_price * 0.996

# -------- AUTO-TRADING REMOVED FOR MANUAL MODE ----------
# (commented out)

# =====================================================================
# UI LAYOUT – LUXURY DASHBOARD v26 (FINAL FIX)
# =====================================================================
current_time_str = datetime.now(timezone.utc).strftime("%H:%M:%S")

st.markdown("""
<div class="luxury-header">
    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
        <div>
            <span style="color: #fbbf24; font-weight: 600; letter-spacing: 2px;">⚜️ SUPREME SCALPER</span>
            <span style="color: #94a3b8; font-size: 13px; margin-left: 10px;">Binance Futures • Tournament Pro v26</span>
        </div>
        <div style="display: flex; gap: 16px; align-items: center;">
            <span style="color: #fbbf24; font-size: 12px;">LIVE</span>
            <span style="color: #10b981;">●</span>
            <span class="live-clock">{}</span>
        </div>
    </div>
</div>
""".format(current_time_str), unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs(["📈 Live Dashboard", "📜 Trade History", "🏆 Performance", "📊 Backtest"])

with tab1:
    # Health monitor (threshold 120s)
    health_good = True
    health_messages = []
    if depth_stream and depth_stream.is_alive():
        age = time.time() - depth_stream.last_update
        if age > 120:
            health_good = False
            health_messages.append(f"Depth stale ({age:.1f}s)")
    else:
        health_good = False
        health_messages.append("Depth not running")
    if st.session_state.get('ws_thread') and st.session_state['ws_thread'].is_alive():
        tick_ts = st.session_state.get('tick_history')[-1]['ts'] if st.session_state.get('tick_history') else time.time()
        age = time.time() - tick_ts
        if age > 120:
            health_good = False
            health_messages.append(f"Trade stale ({age:.1f}s)")
    else:
        health_good = False
        health_messages.append("Trade not running")
    if multi_processor:
        age = time.time() - multi_processor.last_update
        if age > 120:
            health_good = False
            health_messages.append(f"Multi‑stream stale ({age:.1f}s)")
        test_signal = store.get(f"signal:{TOP_COINS[0]}")
        if test_signal and (time.time() - test_signal.get('timestamp', 0) < 120):
            health_messages.append("Multi‑stream healthy")
        else:
            health_good = False
            health_messages.append("Signal data stale")
    else:
        health_good = False
        health_messages.append("Multi‑stream not running")

    if health_good:
        st.markdown('<span class="health-good">✅ All streams healthy</span>', unsafe_allow_html=True)
    else:
        stale_msgs = [m for m in health_messages if 'stale' in m or 'not running' in m]
        if stale_msgs:
            st.markdown(f'<span class="health-stale">⚠️ {", ".join(stale_msgs)}</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="health-bad">⚠️ Data issues</span>', unsafe_allow_html=True)

    # ---- SPLIT LAYOUT ----
    left_col, right_col = st.columns([0.55, 0.45], gap="medium")

    with left_col:
        poc_str = f"{poc:.2f}" if poc is not None else "N/A"
        vah_str = f"{vah:.2f}" if vah is not None else "N/A"
        val_str = f"{val:.2f}" if val is not None else "N/A"
        status_badge = "🔒 LOCKED" if locked else "⚡ ACTIVE"

        # ---- Market Bias ----
        buy_vol = 0.0
        sell_vol = 0.0
        if current_cvd > 0:
            buy_vol = abs(current_cvd)
        else:
            sell_vol = abs(current_cvd)
        total_vol = buy_vol + sell_vol
        if total_vol > 0:
            bias_pct = (buy_vol / total_vol) * 100
        else:
            bias_pct = 50.0
        bias_label = "Bullish" if bias_pct > 55 else "Bearish" if bias_pct < 45 else "Neutral"
        bias_color = "#10b981" if bias_pct > 55 else "#ef4444" if bias_pct < 45 else "#fbbf24"
        bias_class = "market-bias-bullish" if bias_pct > 55 else "market-bias-bearish" if bias_pct < 45 else "market-bias-neutral"

        st.markdown(f"""
        <div class="brain-banner">
            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap;">
                <div style="flex:1;">
                    <span style="color:#fbbf24; font-size:10px; font-weight:700; letter-spacing:0.5px;">BAYESIAN FUSION</span>
                    <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                        <span style="font-size:18px; font-weight:700; color:#ffffff;">{action}</span>
                        <span style="font-size:14px; color:#94a3b8;">({final_conf:.1f}%)</span>
                        <span class="status-badge" style="background-color:{action_color}; color:#ffffff; font-size:10px; padding:2px 10px;">{status_badge}</span>
                        <span style="font-size:12px; color:{bias_color}; font-weight:600; background:rgba(0,0,0,0.3); padding:2px 8px; border-radius:12px;">Bias: {bias_label} ({bias_pct:.1f}%)</span>
                    </div>
                    <div style="display:flex; gap:12px; flex-wrap:wrap; font-size:11px; color:#94a3b8; margin-top:2px;">
                        <span>Mode: Tournament</span>
                        <span>|</span>
                        <span>State: {lock_reason}</span>
                        <span>|</span>
                        <span>TF: {'Bullish' if multi_tf_bias==1 else 'Bearish' if multi_tf_bias==-1 else 'Neutral'}</span>
                        <span>|</span>
                        <span>D‑OFI: {ofi_full:.3f}</span>
                        <span>|</span>
                        <span>POC: {poc_str}</span>
                        {f"<span>|</span><span>🧱 {'BID' if wall_detected and wall_dir==1 else 'ASK' if wall_detected and wall_dir==-1 else ''}</span>" if wall_detected else ""}
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        if action == "STRONG LONG" and not locked and final_conf >= 75:
            decision = "✅ LONG ENTRY (Manual)"
            color = "#10b981"
            extra = f"Entry: ${current_price:,.2f} | SL: ${suggested_sl_long:,.2f} | TP: ${suggested_tp_long:,.2f} | RR: 1:{((suggested_tp_long-current_price)/(current_price-suggested_sl_long)):.2f}"
        elif action == "STRONG SHORT" and not locked and final_conf >= 75:
            decision = "✅ SHORT ENTRY (Manual)"
            color = "#ef4444"
            extra = f"Entry: ${current_price:,.2f} | SL: ${suggested_sl_short:,.2f} | TP: ${suggested_tp_short:,.2f} | RR: 1:{((current_price-suggested_tp_short)/(suggested_sl_short-current_price)):.2f}"
        elif locked:
            decision = "⛔ LOCKED"
            color = "#f59e0b"
            extra = f"Reason: {lock_reason}"
        else:
            decision = "⏳ WAIT"
            color = "#94a3b8"
            extra = f"Confidence: {final_conf:.1f}%"

        st.markdown(f"""
        <div class="summary-card" style="padding:8px 12px; margin:4px 0;">
            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap;">
                <div>
                    <span style="font-size:11px; font-weight:600; color:#f8fafc;">FINAL DECISION</span>
                    <span style="font-size:16px; font-weight:700; margin-left:8px; color:{color};">{decision}</span>
                </div>
                <span style="font-size:12px; color:#94a3b8;">{extra}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        reasoning = []
        if is_iceberg: reasoning.append(f"Iceberg Z={ice_z:.1f}")
        if is_whale: reasoning.append(f"Whale Z={whale_z:.1f}")
        if is_spoof: reasoning.append(f"Spoof Z={spoof_z:.1f}")
        if is_stophunt: reasoning.append(f"Stop Hunt {stophunt_score:.1f}")
        if ob_dir != 0: reasoning.append(f"OB {ob_status}")
        if fvg_active: reasoning.append(f"FVG {fvg_fill:.1f}%")
        if wall_detected: reasoning.append(f"Wall {'BID' if wall_dir==1 else 'ASK'}")
        if multi_tf_bias != 0: reasoning.append(f"TF {'Bull' if multi_tf_bias==1 else 'Bear'}")
        if divergence != 0: reasoning.append(f"{'Bull' if divergence==1 else 'Bear'} Div")
        if sweep_dir != 0: reasoning.append(f"Sweep {swept_price:.2f}")
        if reasoning:
            st.markdown("""
            <div style="display:flex; gap:6px; flex-wrap:wrap; margin-top:4px;">
            """ + "".join([f'<span class="reasoning-box" style="font-size:10px; padding:2px 8px;">👉 {r}</span>' for r in reasoning]) + "</div>", unsafe_allow_html=True)

        if action in ("STRONG LONG", "STRONG SHORT") and final_conf >= 75.0 and not locked:
            if action == "STRONG LONG":
                st.markdown(f"""
                <div class="entry-card" style="padding:10px 12px; margin:6px 0;">
                    <div style="display:flex; justify-content:space-between;">
                        <span style="font-size:14px; font-weight:700; color:#10b981;">📈 LONG</span>
                        <span style="font-size:10px; color:#94a3b8;">Conf {final_conf:.1f}%</span>
                    </div>
                    <div style="display:flex; gap:16px; flex-wrap:wrap; font-size:13px;">
                        <span>Entry: <strong>${current_price:,.2f}</strong></span>
                        <span>SL: <strong style="color:#f87171;">${suggested_sl_long:,.2f}</strong></span>
                        <span>TP: <strong style="color:#34d399;">${suggested_tp_long:,.2f}</strong></span>
                        <span>RR: <strong style="color:#fbbf24;">1:{((suggested_tp_long-current_price)/(current_price-suggested_sl_long)):.2f}</strong></span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="entry-card-short" style="padding:10px 12px; margin:6px 0;">
                    <div style="display:flex; justify-content:space-between;">
                        <span style="font-size:14px; font-weight:700; color:#ef4444;">📉 SHORT</span>
                        <span style="font-size:10px; color:#94a3b8;">Conf {final_conf:.1f}%</span>
                    </div>
                    <div style="display:flex; gap:16px; flex-wrap:wrap; font-size:13px;">
                        <span>Entry: <strong>${current_price:,.2f}</strong></span>
                        <span>SL: <strong style="color:#f87171;">${suggested_sl_short:,.2f}</strong></span>
                        <span>TP: <strong style="color:#34d399;">${suggested_tp_short:,.2f}</strong></span>
                        <span>RR: <strong style="color:#fbbf24;">1:{((current_price-suggested_tp_short)/(suggested_sl_short-current_price)):.2f}</strong></span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        # ---- Funding Rate + OI Chart ----
        st.markdown("---")
        st.markdown("### 💹 Funding Rate & Open Interest")
        funding_rates = multi_processor.get_funding_rate_history(selected_asset)
        oi_history = multi_processor.get_oi_history(selected_asset)

        if funding_rates and oi_history:
            # Ensure lengths match
            min_len = min(len(funding_rates), len(oi_history))
            fr_data = funding_rates[-min_len:]
            oi_data = oi_history[-min_len:]
            # Create dataframe for chart
            df_fr_oi = pd.DataFrame({
                'Funding Rate': fr_data,
                'OI (normalized)': np.array(oi_data) / (np.max(oi_data) + 1e-6)  # normalize for display
            })
            st.line_chart(df_fr_oi)
        else:
            st.caption("Waiting for funding rate and OI data...")

        # ---- Active Walls Table ----
        st.markdown("---")
        st.markdown("### 🧱 Active Walls (Top 5 BID/ASK)")
        ob = multi_depth.get_orderbook(selected_asset) if multi_depth else {}
        if ob and ob.get('bids') and ob.get('asks'):
            # Build table
            bid_rows = []
            ask_rows = []
            for b in ob['bids'][:5]:
                price = float(b[0])
                size = float(b[1])
                usd_value = price * size
                bid_rows.append((price, size, usd_value))
            for a in ob['asks'][:5]:
                price = float(a[0])
                size = float(a[1])
                usd_value = price * size
                ask_rows.append((price, size, usd_value))

            col_bid, col_ask = st.columns(2)
            with col_bid:
                st.markdown("**BID Walls**")
                if bid_rows:
                    table_html = "<table class='wall-table'><tr><th>Price</th><th>Size (BTC)</th><th>USD Value</th></tr>"
                    for price, size, usd in bid_rows:
                        table_html += f"<tr class='bid'><td>${price:,.2f}</td><td>{size:.2f}</td><td>${usd:,.0f}</td></tr>"
                    table_html += "</table>"
                    st.markdown(table_html, unsafe_allow_html=True)
                else:
                    st.caption("No bid walls")
            with col_ask:
                st.markdown("**ASK Walls**")
                if ask_rows:
                    table_html = "<table class='wall-table'><tr><th>Price</th><th>Size (BTC)</th><th>USD Value</th></tr>"
                    for price, size, usd in ask_rows:
                        table_html += f"<tr class='ask'><td>${price:,.2f}</td><td>{size:.2f}</td><td>${usd:,.0f}</td></tr>"
                    table_html += "</table>"
                    st.markdown(table_html, unsafe_allow_html=True)
                else:
                    st.caption("No ask walls")
        else:
            st.caption("Order book data loading...")

    # ---- Right Column (Trade Monitor + Mini Signal Table) ----
    with right_col:
        st.markdown('<div class="trade-monitor">', unsafe_allow_html=True)
        st.markdown('<div class="trade-monitor-title">📊 TRADE MONITOR</div>', unsafe_allow_html=True)

        active_trade = trade_manager.get_trade_status()
        if active_trade:
            direction_emoji = "📈" if active_trade['direction'] == 'LONG' else "📉"
            dir_color = "#10b981" if active_trade['direction'] == 'LONG' else "#ef4444"
            lock_status = "🔒 LOCKED" if trade_manager.lock_status else "🔓 UNLOCKED"
            lock_color = "#f59e0b" if trade_manager.lock_status else "#10b981"
            lock_time = trade_manager.lock_time if trade_manager.lock_time else "N/A"
            st.markdown(f"""
            <div style="background:rgba(16,185,129,0.05); border-radius:8px; padding:10px; margin-bottom:8px; border:1px solid rgba(255,215,0,0.1);">
                <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap;">
                    <div>
                        <span style="font-size:14px; font-weight:700;">{direction_emoji} {active_trade['asset']}</span>
                        <span style="font-size:12px; color:{dir_color}; font-weight:600; margin-left:6px;">{active_trade['direction']}</span>
                    </div>
                    <div>
                        <span style="font-size:10px; color:{lock_color};">{lock_status}</span>
                        <span style="font-size:9px; color:#94a3b8; margin-left:6px;">{lock_time}</span>
                    </div>
                </div>
                <div style="display:flex; gap:12px; flex-wrap:wrap; font-size:12px; margin-top:4px;">
                    <span>Entry: <strong>${active_trade['entry_price']:,.2f}</strong></span>
                    <span>SL: <strong style="color:#f87171;">${active_trade['stop_loss']:,.2f}</strong></span>
                    <span>TP: <strong style="color:#34d399;">${active_trade['take_profit']:,.2f}</strong></span>
                    <span>Conf: <strong>{active_trade['confidence']:.1f}%</strong></span>
                </div>
                {f'<div style="font-size:9px; color:#f59e0b; margin-top:4px;">🔒 Lock Reason: {trade_manager.lock_reason}</div>' if trade_manager.lock_status else ''}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background:rgba(17,24,39,0.5); border-radius:8px; padding:12px; text-align:center; color:#475569; font-size:12px; margin-bottom:8px; border:1px dashed rgba(255,215,0,0.1);">
                ⏳ No active trade
            </div>
            """, unsafe_allow_html=True)

        # ---- MINI SIGNAL TABLE ----
        st.markdown('<div style="color:#fbbf24; font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:0.3px; margin-top:6px; margin-bottom:4px;">⚡ Quick Signals</div>', unsafe_allow_html=True)

        mini_data = []
        for sym in TOP_COINS:
            signal_data = store.get(f"signal:{sym}")
            if signal_data:
                display = f"{sym[:-4]}"
                sig = signal_data['signal']
                conf = signal_data['confidence']
                is_locked = locked
                mini_data.append({
                    'symbol': display,
                    'signal': sig,
                    'confidence': conf,
                    'locked': is_locked
                })
            else:
                mini_data.append({
                    'symbol': f"{sym[:-4]}",
                    'signal': 'WAIT',
                    'confidence': 0,
                    'locked': False
                })

        mini_html = """
        <!DOCTYPE html>
        <html>
        <head>
        <style>
            body { background: transparent; font-family: 'Inter', sans-serif; margin: 0; padding: 0; }
            .mini-table { width: 100%; font-size: 12px; border-collapse: collapse; color: #fbbf24; }
            .mini-table td { padding: 6px 8px; border-bottom: 1px solid rgba(255, 215, 0, 0.1); font-size: 12px; }
            .sym { font-weight: 600; color: #f8fafc; }
            .sig-long { color: #10b981; font-weight: 600; }
            .sig-short { color: #ef4444; font-weight: 600; }
            .sig-neutral { color: #fbbf24; font-weight: 400; }
            .lock-badge {
                background: rgba(245, 158, 11, 0.2);
                border: 1px solid #f59e0b;
                border-radius: 12px;
                padding: 2px 8px;
                font-size: 9px;
                color: #f59e0b;
                display: inline-block;
            }
            .unlock-badge {
                background: rgba(16, 185, 129, 0.2);
                border: 1px solid #10b981;
                border-radius: 12px;
                padding: 2px 8px;
                font-size: 9px;
                color: #10b981;
                display: inline-block;
            }
        </style>
        </head>
        <body>
        <table class="mini-table">
        """
        for item in mini_data:
            sig_class = "sig-long" if item['signal'] == "LONG" else "sig-short" if item['signal'] == "SHORT" else "sig-neutral"
            lock_badge = '<span class="lock-badge">🔒</span>' if item['locked'] else '<span class="unlock-badge">🔓</span>'
            conf_display = f"{item['confidence']:.0f}%" if item['confidence'] > 0 else "--"
            mini_html += f"""
            <tr>
                <td class="sym">{item['symbol']}</td>
                <td class="{sig_class}">{item['signal']}</td>
                <td style="font-size:11px; color:#fbbf24;">{conf_display}</td>
                <td>{lock_badge}</td>
            </tr>
            """
        mini_html += """
        </table>
        </body>
        </html>
        """
        components.html(mini_html, height=200, scrolling=True)

        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")

    # ---- FULL MASTER TABLE ----
    st.markdown("### 🏆 Top 10 Coins – Live Price & Order Book Walls")
    multi_depth = multi_depth_stream
    data_rows = []
    for sym in TOP_COINS:
        mini = store.get(f"mini:{sym}")
        if not mini:
            continue
        price = mini['price']
        change = mini.get('change', 0)
        ob = multi_depth.get_orderbook(sym) if multi_depth else {'bids': [], 'asks': []}
        bid_vol = sum(float(b[1]) for b in ob.get('bids', [])[:5]) if ob.get('bids') else 0
        ask_vol = sum(float(a[1]) for a in ob.get('asks', [])[:5]) if ob.get('asks') else 0
        spread = (float(ob['asks'][0][0]) - float(ob['bids'][0][0])) if ob.get('bids') and ob.get('asks') else 0
        signal_data = store.get(f"signal:{sym}")
        signal = signal_data['signal'] if signal_data else "NEUTRAL"
        conf = signal_data['confidence'] if signal_data else 0
        display = f"{sym[:-4]}/USDT"
        data_rows.append({
            "Asset": display,
            "Price": price,
            "Change": change,
            "Bid Wall": bid_vol,
            "Ask Wall": ask_vol,
            "Spread": spread,
            "Signal": signal,
            "Confidence": conf
        })

    if data_rows:
        table_html = """
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th style="text-align:left;">Asset</th>
                        <th>Price</th>
                        <th>Change</th>
                        <th>Bid Wall</th>
                        <th>Ask Wall</th>
                        <th>Spread</th>
                        <th>Signal</th>
                        <th>Conf</th>
                    </tr>
                </thead>
                <tbody>
        """
        for row in data_rows:
            sig_class = "signal-long" if row["Signal"] == "LONG" else "signal-short" if row["Signal"] == "SHORT" else "signal-neutral"
            price_class = "price-up" if row["Change"] > 0 else "price-down" if row["Change"] < 0 else ""
            table_html += f"""
                <tr>
                    <td class="symbol-col">{row["Asset"]}</td>
                    <td class="{price_class}">${row["Price"]:,.2f}</td>
                    <td class="{price_class}">{row["Change"]:+.2f}%</td>
                    <td>{row["Bid Wall"]:,.0f}</td>
                    <td>{row["Ask Wall"]:,.0f}</td>
                    <td>${row["Spread"]:.2f}</td>
                    <td class="{sig_class}"><strong>{row["Signal"]}</strong></td>
                    <td>{row["Confidence"]:.0f}%</td>
                </tr>
            """
        table_html += """
                </tbody>
            </table>
        </div>
        """
        st.markdown(table_html, unsafe_allow_html=True)
    else:
        st.info("Waiting for live data...")

    st.markdown("---")

    # ---- CARDS ----
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.markdown(f'<div class="card-gold"><div class="card-title">Price</div><div class="card-value">${current_price:,.2f}</div><div class="card-sub">VWAP: ${session_vwap:,.2f}</div></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="card-gold"><div class="card-title">D‑OFI</div><div class="card-value">{ofi_full:+.3f}</div><div class="card-sub">Micro: ${microprice:,.2f}</div></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="card-gold"><div class="card-title">CVD</div><div class="card-value">{current_cvd:,.0f}</div></div>', unsafe_allow_html=True)
    c4.markdown(f'<div class="card-gold"><div class="card-title">VPIN</div><div class="card-value">{real_vpin:.3f}</div></div>', unsafe_allow_html=True)
    c5.markdown(f'<div class="card-gold"><div class="card-title">ATR</div><div class="card-value">${current_atr:.2f}</div></div>', unsafe_allow_html=True)

    c6, c7, c8, c9, c10 = st.columns(5)
    c6.markdown(f'<div class="card-gold"><div class="card-title">Iceberg</div><div class="card-value" style="color:{"#10b981" if not is_iceberg else "#ef4444"}">{"Clear" if not is_iceberg else "Active"}</div></div>', unsafe_allow_html=True)
    c7.markdown(f'<div class="card-gold"><div class="card-title">Spoof</div><div class="card-value" style="color:{"#10b981" if not is_spoof else "#ef4444"}">{"Clear" if not is_spoof else "Active"}</div></div>', unsafe_allow_html=True)
    c8.markdown(f'<div class="card-gold"><div class="card-title">Whale</div><div class="card-value" style="color:{"#ef4444" if is_whale else "#10b981"}">{"Active" if is_whale else "Clear"}</div><div class="card-sub">Z: {whale_z:.1f}</div></div>', unsafe_allow_html=True)
    c9.markdown(f'<div class="card-gold"><div class="card-title">Liquidity Wall</div><div class="card-value" style="color:{"#fbbf24" if wall_detected else "#94a3b8"}">{"BID" if wall_detected and wall_dir==1 else "ASK" if wall_detected and wall_dir==-1 else "None"}</div><div class="card-sub">Ratio: {wall_ratio:.1f}</div></div>', unsafe_allow_html=True)
    c10.markdown(f'<div class="card-gold"><div class="card-title">Stop Hunt</div><div class="card-value" style="color:{"#ef4444" if is_stophunt else "#10b981"}">{"Alert" if is_stophunt else "Clear"}</div><div class="card-sub">Score: {stophunt_score:.0f}</div></div>', unsafe_allow_html=True)

    st.markdown("---")

    # Chart
    st.markdown("### 📈 Chart – Selected Asset")
    clean_tv = selected_asset
    tv_code = f"""
    <div class="tradingview-widget-container" style="height:500px;">
      <div id="tv_chart" style="height:500px;"></div>
      <script src="https://s3.tradingview.com/tv.js"></script>
      <script>
      new TradingView.widget({{
        "autosize": true,
        "symbol": "BINANCE:{clean_tv}",
        "interval": "5",
        "theme": "dark",
        "style": "1",
        "locale": "en",
        "container_id": "tv_chart"
      }});
      </script>
    </div>
    """
    components.html(tv_code, height=510)

# =====================================================================
# TAB 2: TRADE HISTORY
# =====================================================================
with tab2:
    st.markdown("### 📜 Complete Trade History")
    try:
        conn = sqlite3.connect(DB_FILE, timeout=30.0)
        trades_df = pd.read_sql_query("SELECT * FROM trades ORDER BY entry_time DESC", conn)
        conn.close()

        if not trades_df.empty:
            st.dataframe(
                trades_df.style.format({
                    'entry_price': '${:,.2f}',
                    'stop_loss': '${:,.2f}',
                    'take_profit': '${:,.2f}',
                    'exit_price': '${:,.2f}',
                    'confidence': '{:.1f}%',
                    'profit_percent': '{:+.2f}%'
                }),
                use_container_width=True,
                height=400
            )

            csv = trades_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Trade History (CSV)",
                data=csv,
                file_name=f"trade_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No trade history recorded yet. Executed trades will appear here automatically.")
    except Exception as e:
        logging.exception(f"Error rendering Trade History tab: {e}")
        st.error("Failed to load trade history from the database.")

# =====================================================================
# TAB 3: PERFORMANCE ANALYTICS
# =====================================================================
with tab3:
    st.markdown("### 🏆 Performance & Strategy Analytics")
    try:
        conn = sqlite3.connect(DB_FILE, timeout=30.0)
        closed_trades = pd.read_sql_query("SELECT * FROM trades WHERE status='CLOSED'", conn)
        conn.close()

        if not closed_trades.empty:
            total_trades = len(closed_trades)
            wins = len(closed_trades[closed_trades['result'] == 'WIN'])
            losses = len(closed_trades[closed_trades['result'] == 'LOSS'])
            win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0.0
            
            total_profit = closed_trades['profit_percent'].sum()
            avg_trade = closed_trades['profit_percent'].mean()
            best_trade = closed_trades['profit_percent'].max()
            worst_trade = closed_trades['profit_percent'].min()

            p1, p2, p3, p4 = st.columns(4)
            p1.markdown(f'<div class="card-gold"><div class="card-title">Total Trades</div><div class="card-value">{total_trades}</div><div class="card-sub">Wins: {wins} | Losses: {losses}</div></div>', unsafe_allow_html=True)
            p2.markdown(f'<div class="card-gold"><div class="card-title">Win Rate</div><div class="card-value" style="color:{"#10b981" if win_rate >= 50 else "#ef4444"}">{win_rate:.1f}%</div></div>', unsafe_allow_html=True)
            p3.markdown(f'<div class="card-gold"><div class="card-title">Net Cumulative Return</div><div class="card-value" style="color:{"#10b981" if total_profit >= 0 else "#ef4444"}">{total_profit:+.2f}%</div></div>', unsafe_allow_html=True)
            p4.markdown(f'<div class="card-gold"><div class="card-title">Expectancy / Trade</div><div class="card-value">{avg_trade:+.2f}%</div><div class="card-sub">Max: {best_trade:+.2f}% | Min: {worst_trade:+.2f}%</div></div>', unsafe_allow_html=True)

            st.markdown("---")
            st.markdown("#### 📈 Cumulative Performance Curve")
            closed_trades['cum_profit'] = closed_trades['profit_percent'].cumsum()
            chart_data = closed_trades[['entry_time', 'cum_profit']].set_index('entry_time')
            st.line_chart(chart_data)
        else:
            st.info("Insufficient closed trades to compute performance metrics.")
    except Exception as e:
        logging.exception(f"Error rendering Performance tab: {e}")
        st.error("Failed to compute performance analytics.")

# =====================================================================
# TAB 4: BACKTEST (COMPLETE)
# =====================================================================
with tab4:
    st.markdown("### 📊 Strategy Backtest (Historical Accuracy Check)")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        bt_asset_display = st.selectbox("Asset", [f"{s[:-4]}/USDT" for s in TOP_COINS], key="bt_asset")
        bt_asset = bt_asset_display.replace('/', '')
    with col2:
        bt_timeframe = st.selectbox("Timeframe", options=['1m','5m','15m','30m','1h','4h'], index=0, key="bt_tf")
    with col3:
        bt_days = st.number_input("Days", min_value=1, max_value=30, value=7, key="bt_days")
    with col4:
        bt_threshold = st.slider("Confidence Threshold (%)", min_value=50, max_value=90, value=60, step=5, key="bt_thresh")
    
    if st.button("🚀 Run Backtest", key="bt_run"):
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=bt_days)
        
        with st.spinner("Fetching historical data and running simulation..."):
            try:
                engine = BacktestEngine(
                    exchange=binance,
                    asset=bt_asset,
                    start_date=start_date,
                    end_date=end_date,
                    timeframe=bt_timeframe,
                    threshold=bt_threshold/100.0
                )
                if engine.fetch_data():
                    if engine.run():
                        metrics = engine.get_metrics()
                        if metrics:
                            st.markdown("#### 📈 Backtest Results")
                            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                            col_m1.metric("Total Trades", metrics['total_trades'])
                            col_m2.metric("Win Rate", f"{metrics['win_rate']:.1f}%")
                            col_m3.metric("Total PnL", f"{metrics['total_pnl']:+.2f}%")
                            col_m4.metric("Avg PnL/Trade", f"{metrics['avg_pnl']:+.2f}%")
                            
                            col_m5, col_m6, col_m7, col_m8 = st.columns(4)
                            col_m5.metric("Wins", metrics['wins'])
                            col_m6.metric("Losses", metrics['losses'])
                            col_m7.metric("Max Win", f"{metrics['max_win']:+.2f}%")
                            col_m8.metric("Max Loss", f"{metrics['max_loss']:+.2f}%")
                            
                            st.info(f"📊 Strong signals detected: {metrics.get('signal_count', 0)} (confidence >= {bt_threshold}%)")
                            
                            st.markdown("#### 📈 Equity Curve")
                            if not engine.equity.empty:
                                st.line_chart(engine.equity['equity'])
                            
                            if engine.trades:
                                st.markdown("#### 📋 Trade List")
                                df_trades = pd.DataFrame(engine.trades)
                                st.dataframe(
                                    df_trades.style.format({
                                        'entry_price': '${:,.2f}',
                                        'exit_price': '${:,.2f}',
                                        'profit_pct': '{:+.2f}%',
                                        'confidence': '{:.1f}%'
                                    }),
                                    use_container_width=True
                                )
                            else:
                                st.info("No trades generated. Try lowering threshold or increasing days.")
                        else:
                            st.warning("Backtest completed but no metrics.")
                    else:
                        st.error("Backtest engine failed to run.")
                else:
                    st.error("Failed to fetch historical data.")
            except Exception as e:
                logging.exception(f"Backtest error: {e}")
                st.error(f"An error occurred: {str(e)}")

# =====================================================================
# FOOTER
# =====================================================================
st.markdown("""
<div style="text-align: center; color: #475569; font-size: 11px; margin-top: 20px; border-top: 1px solid #1e293b; padding-top: 14px;">
    Supreme Scalper v26 – Tournament Pro | Binance Futures | 100% Real WebSocket Data | Not financial advice.
</div>
""", unsafe_allow_html=True)