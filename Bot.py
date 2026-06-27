import time
import requests
import ccxt
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timezone, timedelta

# ═══════════════════════════════════
# CONFIG
# ═══════════════════════════════════
NTFY_URL         = "https://ntfy.sh/Mrunknown_786"
SYMBOLS          = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'AVAX/USDT', 'BNB/USDT']
TF_ENTRY         = '15m'
TF_BIAS          = '1h'
PERF_FILE        = 'performance.json'
SCAN_SLEEP       = 60
DAILY_LOSS_LIMIT = 3
MIN_CONFIDENCE   = 55
COOLDOWN_MINUTES = 30

HIGH_IMPACT_NEWS = [(8,30),(12,30),(14,0),(18,0),(19,0)]
NEWS_BLOCK_MIN   = 45

# ═══════════════════════════════════
# LOGGING
# ═══════════════════════════════════
def log(msg):
    now = datetime.now(timezone.utc).strftime('%H:%M:%S')
    print(f"[{now}] {msg}", flush=True)

# ═══════════════════════════════════
# NOTIFICATION
# ═══════════════════════════════════
def notify(title, msg, tags="chart_with_upwards_trend"):
    try:
        r = requests.post(
            NTFY_URL,
            data=msg.encode('utf-8'),
            headers={"Title": title, "Priority": "high", "Tags": tags},
            timeout=15
        )
        log(f"NOTIF: {title} [{r.status_code}]")
    except Exception as e:
        log(f"NOTIF ERR: {e}")

# ═══════════════════════════════════
# EXCHANGE FAILOVER
# ═══════════════════════════════════
def connect_exchange():
    brokers = [
        ('MEXC',    ccxt.mexc,    {'enableRateLimit': True, 'options': {'defaultType': 'spot'}}),
        ('Binance', ccxt.binance, {'enableRateLimit': True, 'options': {'defaultType': 'spot'}}),
        ('Bybit',   ccxt.bybit,   {'enableRateLimit': True, 'options': {'defaultType': 'spot'}}),
    ]
    for name, cls, cfg in brokers:
        try:
            ex = cls(cfg)
            ex.fetch_ticker('BTC/USDT')
            log(f"Exchange: {name} connected")
            return ex, name
        except Exception as e:
            log(f"Exchange {name} failed: {e}")
    log("FATAL: All exchanges failed!")
    exit(1)

def safe_fetch(exchange, exchange_name, symbol, timeframe, limit=200):
    for attempt in range(3):
        try:
            data = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df   = pd.DataFrame(data, columns=['ts','open','high','low','close','volume'])
            return df
        except Exception as e:
            log(f"Fetch attempt {attempt+1} {symbol}: {e}")
            time.sleep(2)
    return None

# ═══════════════════════════════════
# STORAGE
# ═══════════════════════════════════
class Storage:
    def __init__(self):
        self.data = self._load()

    def _load(self):
        if os.path.exists(PERF_FILE):
            try:
                with open(PERF_FILE, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            'strategies': {},
            'pairs': {},
            'sessions': {'Asia': {}, 'London': {}, 'NewYork': {}, 'Off': {}},
            'daily': {},
            'weekly': {},
            'monthly': {},
            'active_trades': {},
            'missed_opportunities': [],
            'signal_cooldowns': {},
            'watchlist_scores': {},
            'daily_losses': 0,
            'last_loss_reset': '',
            'kill_switch_until': '',
            'recovery_mode': False,
            'last_daily_report': '',
            'last_weekly_report': '',
            'last_monthly_report': '',
        }

    def save(self):
        try:
            with open(PERF_FILE, 'w') as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            log(f"SAVE ERR: {e}")

    def is_kill_switch(self):
        ks = self.data.get('kill_switch_until', '')
        if not ks:
            return False
        try:
            until = datetime.fromisoformat(ks)
            if datetime.now(timezone.utc) < until:
                return True
            self.data['kill_switch_until'] = ''
            self.data['daily_losses'] = 0
            self.data['recovery_mode'] = False
            self.save()
            return False
        except Exception:
            return False

    def activate_kill_switch(self):
        until = datetime.now(timezone.utc) + timedelta(hours=24)
        self.data['kill_switch_until'] = until.isoformat()
        self.data['recovery_mode'] = True
        self.save()
        notify(
            "KILL SWITCH ON",
            f"Daily loss limit hit!\nStopped 24hrs\nResumes: {until.strftime('%d-%b %H:%M')} UTC",
            tags="rotating_light,red_circle"
        )

    def reset_daily(self):
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if self.data.get('last_loss_reset') != today:
            self.data['daily_losses'] = 0
            self.data['last_loss_reset'] = today
            self.save()

    def is_cooldown(self, symbol, strategy):
        key  = f"{symbol}_{strategy}"
        last = self.data['signal_cooldowns'].get(key, '')
        if not last:
            return False
        try:
            elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds()
            return elapsed < COOLDOWN_MINUTES * 60
        except Exception:
            return False

    def set_cooldown(self, symbol, strategy):
        key = f"{symbol}_{strategy}"
        self.data['signal_cooldowns'][key] = datetime.now(timezone.utc).isoformat()
        self.save()

    def get_strategy_weight(self, strategy):
        s      = self.data['strategies'].get(strategy, {})
        base_w = s.get('weight', 1.0)
        recent = s.get('recent_trades', [])
        if not recent:
            return base_w
        now     = datetime.now(timezone.utc)
        decayed = 0.0
        total_w = 0.0
        for t in recent[-20:]:
            try:
                age_h   = (now - datetime.fromisoformat(t['time'])).total_seconds() / 3600
                decay   = max(0.1, 1.0 - (age_h / 48))
                val     = 1.0 if t['result'] == 'win' else -1.0
                decayed += val * decay
                total_w += decay
            except Exception:
                pass
        if total_w > 0:
            adj    = (decayed / total_w) * 0.3
            base_w = max(0.2, min(2.0, base_w + adj))
        return base_w

    def is_strategy_retired(self, strategy):
        return self.data['strategies'].get(strategy, {}).get('weight', 1.0) < 0.3

    def get_pair_weight(self, symbol):
        return self.data['pairs'].get(symbol, {}).get('weight', 1.0)

    def update_watchlist(self, symbol, score):
        if symbol not in self.data['watchlist_scores']:
            self.data['watchlist_scores'][symbol] = []
        self.data['watchlist_scores'][symbol].append({
            'score': score,
            'time': datetime.now(timezone.utc).isoformat()
        })
        self.data['watchlist_scores'][symbol] = self.data['watchlist_scores'][symbol][-20:]

    def get_watchlist_priority(self):
        scores = {}
        for sym, entries in self.data['watchlist_scores'].items():
            if entries:
                recent = entries[-5:]
                scores[sym] = sum(e['score'] for e in recent) / len(recent)
        return sorted(scores.keys(), key=lambda x: scores.get(x, 0), reverse=True)

    def record_signal(self, symbol, strategy, session, entry, sl, tp1, tp2, confidence):
        rr = round((tp1 - entry) / max(entry - sl, 1e-10), 2)
        self.data['active_trades'][symbol] = {
            'symbol': symbol, 'strategy': strategy, 'session': session,
            'entry': entry, 'sl': sl, 'tp1': tp1, 'tp2': tp2,
            'rr': rr, 'confidence': confidence, 'status': 'open',
            'tp1_hit': False, 'dca_done': False, 'trailing_sl': sl,
            'time': datetime.now(timezone.utc).isoformat()
        }
        self.set_cooldown(symbol, strategy)
        self.save()

    def record_missed(self, symbol, strategy, reason, score):
        self.data['missed_opportunities'].append({
            'symbol': symbol, 'strategy': strategy,
            'reason': reason, 'score': score,
            'time': datetime.now(timezone.utc).isoformat()
        })
        self.data['missed_opportunities'] = self.data['missed_opportunities'][-100:]
        self.save()

    def close_trade(self, symbol, result):
        if symbol not in self.data['active_trades']:
            return
        trade = self.data['active_trades'].pop(symbol)
        trade['result'] = result
        trade['status'] = 'closed'
        trade['closed'] = datetime.now(timezone.utc).isoformat()
        strat   = trade['strategy']
        session = trade.get('session', 'Off')
        now     = datetime.now(timezone.utc)
        today   = now.strftime('%Y-%m-%d')
        week    = f"{now.year}-W{now.isocalendar()[1]}"
        month   = now.strftime('%Y-%m')

        if strat not in self.data['strategies']:
            self.data['strategies'][strat] = {'wins': 0, 'losses': 0, 'weight': 1.0, 'recent_trades': []}
        s = self.data['strategies'][strat]
        if result == 'win':
            s['wins']   += 1
            s['weight']  = min(2.0, s['weight'] + 0.1)
        else:
            s['losses'] += 1
            s['weight']  = max(0.2, s['weight'] - 0.1)
            self.data['daily_losses'] = self.data.get('daily_losses', 0) + 1
        s['recent_trades'].append({'result': result, 'time': trade['closed']})
        s['recent_trades'] = s['recent_trades'][-50:]

        if symbol not in self.data['pairs']:
            self.data['pairs'][symbol] = {'wins': 0, 'losses': 0, 'weight': 1.0}
        p = self.data['pairs'][symbol]
        p[result + 's'] += 1
        p['weight'] = min(2.0, p['weight'] + 0.1) if result == 'win' else max(0.2, p['weight'] - 0.1)

        if session not in self.data['sessions']:
            self.data['sessions'][session] = {}
        sess = self.data['sessions'][session]
        if strat not in sess:
            sess[strat] = {'wins': 0, 'losses': 0}
        sess[strat][result + 's'] += 1

        if today not in self.data['daily']:
            self.data['daily'][today] = {'wins': 0, 'losses': 0, 'trades': []}
        self.data['daily'][today][result + 's'] += 1
        self.data['daily'][today]['trades'].append(trade)

        if week not in self.data['weekly']:
            self.data['weekly'][week] = {'wins': 0, 'losses': 0}
        self.data['weekly'][week][result + 's'] += 1

        if month not in self.data['monthly']:
            self.data['monthly'][month] = {'wins': 0, 'losses': 0}
        self.data['monthly'][month][result + 's'] += 1

        if self.data.get('daily_losses', 0) >= DAILY_LOSS_LIMIT:
            self.activate_kill_switch()

        self.save()
        log(f"Trade closed: {symbol} {result}")

    def get_best(self):
        best_s = max(
            self.data['strategies'],
            key=lambda x: self.data['strategies'][x].get('weight', 1.0),
            default='N/A'
        )
        best_p = max(
            self.data['pairs'],
            key=lambda x: self.data['pairs'][x].get('weight', 1.0),
            default='N/A'
        )
        return best_s, best_p

    def send_reports(self):
        now   = datetime.now(timezone.utc)
        today = now.strftime('%Y-%m-%d')
        yest  = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        week  = f"{now.year}-W{now.isocalendar()[1]}"
        lw    = f"{(now-timedelta(weeks=1)).year}-W{(now-timedelta(weeks=1)).isocalendar()[1]}"
        month = now.strftime('%Y-%m')
        lm    = (now - timedelta(days=30)).strftime('%Y-%m')

        if now.hour == 6 and self.data.get('last_daily_report') != today:
            d  = self.data['daily'].get(yest, {})
            w  = d.get('wins', 0)
            l  = d.get('losses', 0)
            t  = w + l
            wr = round(w / t * 100, 1) if t > 0 else 0
            best_s, best_p = self.get_best()
            strat_lines = ""
            for sn, sv in self.data['strategies'].items():
                strat_lines += f"  {sn}: {sv.get('wins',0)}W/{sv.get('losses',0)}L wt:{sv.get('weight',1.0):.1f}\n"
            sess_lines = ""
            for sn, sv in self.data['sessions'].items():
                tw = sum(sv.get(x, {}).get('wins', 0) for x in sv)
                tl = sum(sv.get(x, {}).get('losses', 0) for x in sv)
                if tw + tl > 0:
                    sess_lines += f"  {sn}: {tw}W/{tl}L\n"
            notify(
                "Daily Report",
                f"Date: {yest}\nTotal:{t} W:{w} L:{l} WR:{wr}%\n\nStrategies:\n{strat_lines}\nSessions:\n{sess_lines}\nBest Strategy: {best_s}\nBest Pair: {best_p}",
                tags="bar_chart,calendar"
            )
            self.data['last_daily_report'] = today
            self.save()

        if now.weekday() == 0 and now.hour == 7 and self.data.get('last_weekly_report') != week:
            wd  = self.data['weekly'].get(lw, {})
            ww  = wd.get('wins', 0)
            wl  = wd.get('losses', 0)
            wt  = ww + wl
            wwr = round(ww / wt * 100, 1) if wt > 0 else 0
            notify("Weekly Report", f"Week: {lw}\nTotal:{wt} W:{ww} L:{wl} WR:{wwr}%", tags="bar_chart")
            self.data['last_weekly_report'] = week
            self.save()

        if now.day == 1 and now.hour == 8 and self.data.get('last_monthly_report') != month:
            md  = self.data['monthly'].get(lm, {})
            mw  = md.get('wins', 0)
            ml  = md.get('losses', 0)
            mt  = mw + ml
            mwr = round(mw / mt * 100, 1) if mt > 0 else 0
            notify("Monthly Report", f"Month: {lm}\nTotal:{mt} W:{mw} L:{ml} WR:{mwr}%", tags="bar_chart,tada")
            self.data['last_monthly_report'] = month
            self.save()

# ═══════════════════════════════════
# INDICATORS
# ═══════════════════════════════════
def calc_ema(df, p):
    return df['close'].ewm(span=p, adjust=False).mean()

def calc_atr(df, p=14):
    hl  = df['high'] - df['low']
    hpc = abs(df['high'] - df['close'].shift(1))
    lpc = abs(df['low']  - df['close'].shift(1))
    return pd.concat([hl, hpc, lpc], axis=1).max(axis=1).rolling(p).mean()

def calc_rsi(df, p=14):
    delta = df['close'].diff()
    gain  = delta.where(delta > 0, 0).rolling(p).mean()
    loss  = (-delta.where(delta < 0, 0)).rolling(p).mean()
    rs    = gain / (loss + 1e-10)
    return 100 - (100 / (1 + rs))

def get_ema_slope(df, p=20):
    e = calc_ema(df, p)
    return (e.iloc[-1] - e.iloc[-5]) / max(abs(e.iloc[-5]), 1e-10) * 100

# ═══════════════════════════════════
# MARKET DETECTION
# ═══════════════════════════════════
def detect_market(df):
    atr_v      = calc_atr(df).iloc[-1]
    atr_avg    = calc_atr(df).mean()
    vol_ma     = df['volume'].rolling(20).mean().iloc[-1]
    recent_vol = df['volume'].iloc[-5:].mean()
    slope      = get_ema_slope(df, 20)
    is_high_vol = recent_vol > vol_ma * 1.5
    is_high_atr = atr_v > atr_avg * 1.3
    is_trending = abs(slope) > 0.05
    if is_high_vol and is_high_atr:
        mkt = "VOLATILE"
    elif is_trending and is_high_vol:
        mkt = "TRENDING"
    elif not is_trending and not is_high_vol:
        mkt = "QUIET"
    else:
        mkt = "RANGING"
    log(f"MKT: {mkt} slope:{slope:.3f} atr_ratio:{atr_v/max(atr_avg,1e-10):.2f}")
    return mkt, atr_v, atr_avg

def get_1h_bias(exchange, exchange_name, symbol):
    try:
        data = exchange.fetch_ohlcv(symbol, TF_BIAS, limit=50)
        df   = pd.DataFrame(data, columns=['ts','open','high','low','close','volume'])
        e20  = calc_ema(df, 20).iloc[-1]
        e50  = calc_ema(df, 50).iloc[-1]
        c    = df['close'].iloc[-1]
        if c > e20 and e20 > e50:
            return "BULLISH"
        if c < e20 and e20 < e50:
            return "BEARISH"
        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"

# ═══════════════════════════════════
# SESSION
# ═══════════════════════════════════
def get_session():
    h = datetime.now(timezone.utc).hour
    if 1 <= h < 8:
        return "Asia"
    if 8 <= h < 13:
        return "London"
    if 13 <= h < 18:
        return "NewYork"
    return "Off"

def session_bonus(storage, session, strategy):
    sess = storage.data['sessions'].get(session, {})
    s    = sess.get(strategy, {})
    w    = s.get('wins', 0)
    l    = s.get('losses', 0)
    if w + l < 3:
        return 0
    wr = w / (w + l)
    if wr >= 0.7:
        return 10
    if wr >= 0.5:
        return 5
    return -5

# ═══════════════════════════════════
# ADAPTIVE CONFIDENCE
# ═══════════════════════════════════
def calc_confidence(df, strategy, storage, session, bias, mkt, atr_v, atr_avg, extra=0):
    score   = 0
    reasons = []
    c       = df.iloc[-1]
    vol_ma  = df['volume'].rolling(20).mean().iloc[-1]

    if c['volume'] > vol_ma * 2.0:
        score += 20
        reasons.append("strong_vol")
    elif c['volume'] > vol_ma * 1.2:
        score += 10
        reasons.append("good_vol")

    if c['close'] > c['open']:
        score += 8
        reasons.append("bull_candle")

    if atr_v > atr_avg * 1.2:
        score += 10
        reasons.append("high_atr")
    elif atr_v < atr_avg * 0.7:
        score -= 5
        reasons.append("low_atr")

    slope = get_ema_slope(df, 20)
    if slope > 0.05:
        score += 8
        reasons.append("bull_slope")

    rsi_v = calc_rsi(df).iloc[-1]
    if 40 <= rsi_v <= 65:
        score += 5
        reasons.append("rsi_ok")
    elif rsi_v > 75:
        score -= 10
        reasons.append("overbought")

    w = storage.get_strategy_weight(strategy)
    if w >= 1.5:
        score += 15
        reasons.append("strat_high")
    elif w >= 1.0:
        score += 8
        reasons.append("strat_ok")
    else:
        score -= 10
        reasons.append("strat_low")

    pw = storage.get_pair_weight(strategy)
    if pw >= 1.3:
        score += 5
        reasons.append("pair_good")

    sb = session_bonus(storage, session, strategy)
    score += sb
    if sb > 0:
        reasons.append(f"sess_bonus")

    if bias == "BULLISH":
        score += 8
        reasons.append("1h_bull")
    elif bias == "BEARISH":
        score -= 8
        reasons.append("1h_bear")

    if mkt in ["TRENDING","VOLATILE"] and strategy in ['EMA_PULLBACK','BREAKOUT','DAY_BREAKOUT']:
        score += 8
        reasons.append("mkt_fit")
    elif mkt == "RANGING" and strategy == 'SWEEP':
        score += 8
        reasons.append("mkt_fit")
    elif mkt == "QUIET":
        score -= 10
        reasons.append("quiet")

    if storage.data.get('recovery_mode'):
        score -= 15
        reasons.append("recovery")

    score = max(0, min(100, score + extra))
    log(f"CONF {strategy}: {score}/100 [{','.join(reasons)}]")
    return score, reasons

# ═══════════════════════════════════
# DYNAMIC TARGETS
# ═══════════════════════════════════
def calc_dynamic_targets(entry, atr_v, atr_avg, confidence):
    is_fast = atr_v > atr_avg * 1.2
    if is_fast:
        tp1_pct = 0.02
        tp2_pct = 0.03
    else:
        tp1_pct = 0.003
        tp2_pct = 0.005
    sl  = entry - atr_v * 1.5
    tp1 = entry * (1 + tp1_pct)
    tp2 = entry * (1 + tp2_pct)
    mode = "strong" if confidence >= 75 else "normal" if confidence >= 55 else "weak"
    log(f"TARGETS {mode} tp1:{tp1:.4f} tp2:{tp2:.4f} sl:{sl:.4f}")
    return sl, tp1, tp2

def find_dca_zone(df):
    return df['low'].iloc[-20:].min()

# ═══════════════════════════════════
# SIGNAL SENDER
# ═══════════════════════════════════
def send_signal(active_trades, storage, df, symbol, strategy, market, confidence, reasons, session):
    c       = df.iloc[-1]
    entry   = c['close']
    atr_v   = calc_atr(df).iloc[-1]
    atr_avg = calc_atr(df).mean()
    sl, tp1, tp2 = calc_dynamic_targets(entry, atr_v, atr_avg, confidence)
    rr      = round((tp1 - entry) / max(entry - sl, 1e-10), 1)
    dca_z   = find_dca_zone(df)
    profit_sl = entry * 1.002
    msg = (
        f"Coin: {symbol}\n"
        f"Strategy: {strategy}\n"
        f"Market: {market}\n"
        f"Session: {session}\n"
        f"Confidence: {confidence}/100\n"
        f"Signals: {', '.join(reasons[:3])}\n"
        f"Entry: {entry:.4f}\n"
        f"TP1:   {tp1:.4f} (+{((tp1-entry)/entry*100):.2f}%)\n"
        f"TP2:   {tp2:.4f} (+{((tp2-entry)/entry*100):.2f}%)\n"
        f"SL:    {sl:.4f} (-{((entry-sl)/entry*100):.2f}%)\n"
        f"DCA:   {dca_z:.4f}\n"
        f"RR:    1:{rr}"
    )
    notify(f"BUY | {strategy}", msg)
    active_trades[symbol] = {
        'entry': entry, 'tp1': tp1, 'tp2': tp2,
        'sl': sl, 'trailing_sl': sl, 'profit_sl': profit_sl,
        'strategy': strategy, 'session': session,
        'tp1_hit': False, 'dca_done': False,
        'dca_zone': dca_z, 'confidence': confidence
    }
    storage.record_signal(symbol, strategy, session, entry, sl, tp1, tp2, confidence)
    storage.update_watchlist(symbol, confidence)
    log(f"SIGNAL: {symbol} {strategy} conf:{confidence} entry:{entry:.4f} rr:1:{rr}")

# ═══════════════════════════════════
# STRATEGIES
# ═══════════════════════════════════
def run_sweep(df, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias):
    strat = 'SWEEP'
    if storage.is_strategy_retired(strat):
        return
    if storage.is_cooldown(symbol, strat):
        return
    try:
        c      = df.iloc[-1]
        p      = df.iloc[-2]
        swing  = df['low'].iloc[-20:-1].min()
        vol_ma = df['volume'].rolling(20).mean().iloc[-1]
        swept  = p['low'] < swing and c['close'] > swing
        bull   = c['close'] > c['open']
        vol_ok = c['volume'] > vol_ma * 1.2
        log(f"SWEEP {symbol} swept:{swept} bull:{bull} vol:{vol_ok}")
        if swept and bull and vol_ok:
            conf, reasons = calc_confidence(df, strat, storage, session, bias, mkt, atr_v, atr_avg, extra=10)
            if conf >= MIN_CONFIDENCE:
                send_signal(active_trades, storage, df, symbol, strat, mkt, conf, reasons, session)
            else:
                storage.record_missed(symbol, strat, f"low_conf:{conf}", conf)
    except Exception as e:
        log(f"SWEEP ERR: {e}")

def run_ema_pullback(df, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias):
    strat = 'EMA_PULLBACK'
    if storage.is_strategy_retired(strat):
        return
    if storage.is_cooldown(symbol, strat):
        return
    try:
        df2        = df.copy()
        df2['e20'] = calc_ema(df2, 20)
        df2['e50'] = calc_ema(df2, 50)
        df2['e200']= calc_ema(df2, 200)
        vol_ma     = df2['volume'].rolling(20).mean().iloc[-1]
        c          = df2.iloc[-1]
        p          = df2.iloc[-2]
        trend      = c['e50'] > c['e200']
        touched    = p['low'] <= p['e20'] * 1.002
        bounced    = c['close'] > c['e20'] and c['close'] > c['open']
        vol_ok     = c['volume'] > vol_ma
        log(f"EMA {symbol} trend:{trend} touch:{touched} bounce:{bounced}")
        if touched and bounced and vol_ok:
            extra = 12 if trend else 5
            conf, reasons = calc_confidence(df, strat, storage, session, bias, mkt, atr_v, atr_avg, extra=extra)
            if conf >= MIN_CONFIDENCE:
                send_signal(active_trades, storage, df, symbol, strat, mkt, conf, reasons, session)
            else:
                storage.record_missed(symbol, strat, f"low_conf:{conf}", conf)
    except Exception as e:
        log(f"EMA ERR: {e}")

def run_breakout(df, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias):
    strat = 'BREAKOUT'
    if storage.is_strategy_retired(strat):
        return
    if storage.is_cooldown(symbol, strat):
        return
    try:
        vol_ma = df['volume'].rolling(20).mean().iloc[-1]
        resist = df['high'].iloc[-20:-2].max()
        c      = df.iloc[-1]
        p      = df.iloc[-2]
        broke    = p['close'] > resist and p['volume'] > vol_ma * 1.5
        retested = c['low'] <= resist * 1.002 and c['close'] > resist
        bull     = c['close'] > c['open']
        log(f"BREAKOUT {symbol} broke:{broke} retest:{retested} bull:{bull}")
        if broke and retested and bull:
            conf, reasons = calc_confidence(df, strat, storage, session, bias, mkt, atr_v, atr_avg, extra=12)
            if conf >= MIN_CONFIDENCE:
                send_signal(active_trades, storage, df, symbol, strat, mkt, conf, reasons, session)
            else:
                storage.record_missed(symbol, strat, f"low_conf:{conf}", conf)
    except Exception as e:
        log(f"BREAKOUT ERR: {e}")

def run_day_breakout(df, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias):
    strat = 'DAY_BREAKOUT'
    if storage.is_strategy_retired(strat):
        return
    if storage.is_cooldown(symbol, strat):
        return
    try:
        vol_ma   = df['volume'].rolling(20).mean().iloc[-1]
        day_high = df['high'].iloc[-96:-1].max()
        c        = df.iloc[-1]
        broke_h  = c['close'] > day_high and c['volume'] > vol_ma * 1.3 and c['close'] > c['open']
        log(f"DAY {symbol} day_h:{day_high:.4f} close:{c['close']:.4f} broke:{broke_h}")
        if broke_h:
            conf, reasons = calc_confidence(df, strat, storage, session, bias, mkt, atr_v, atr_avg, extra=10)
            if conf >= MIN_CONFIDENCE:
                send_signal(active_trades, storage, df, symbol, strat, mkt, conf, reasons, session)
            else:
                storage.record_missed(symbol, strat, f"low_conf:{conf}", conf)
    except Exception as e:
        log(f"DAY ERR: {e}")

def run_opportunity(df, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias):
    strat = 'OPPORTUNITY'
    if storage.is_cooldown(symbol, strat):
        return
    try:
        c      = df.iloc[-1]
        vol_ma = df['volume'].rolling(20).mean().iloc[-1]
        body   = abs(c['close'] - c['open'])
        avg_b  = abs(df['close'] - df['open']).rolling(20).mean().iloc[-1]
        big_v  = c['volume'] > vol_ma * 2.5
        big_c  = body > avg_b * 2.0
        bull   = c['close'] > c['open']
        log(f"OPP {symbol} big_vol:{big_v} big_c:{big_c} bull:{bull}")
        if big_v and big_c and bull:
            conf, reasons = calc_confidence(df, strat, storage, session, bias, mkt, atr_v, atr_avg, extra=5)
            if conf >= MIN_CONFIDENCE:
                send_signal(active_trades, storage, df, symbol, strat, mkt, conf, reasons, session)
    except Exception as e:
        log(f"OPP ERR: {e}")

# ═══════════════════════════════════
# MONITOR
# ═══════════════════════════════════
def monitor_trade(df, symbol, active_trades, storage):
    if symbol not in active_trades:
        return
    t = active_trades[symbol]
    c = df.iloc[-1]
    log(f"MON {symbol} H:{c['high']:.4f} L:{c['low']:.4f} TSL:{t['trailing_sl']:.4f}")

    if c['high'] >= t['tp2']:
        notify(
            "TP2 HIT! Full Target!",
            f"Coin: {symbol}\nTP2: {t['tp2']:.4f}\nStrategy: {t['strategy']}\nConf: {t.get('confidence',0)}/100",
            tags="trophy,fire"
        )
        storage.close_trade(symbol, 'win')
        active_trades.pop(symbol, None)
        return

    if c['high'] >= t['tp1'] and not t.get('tp1_hit'):
        new_sl = t['profit_sl']
        t['tp1_hit']     = True
        t['trailing_sl'] = new_sl
        notify(
            "TP1 HIT! Lock Profit!",
            f"Coin: {symbol}\nTP1: {t['tp1']:.4f} HIT!\nUpdate SL to: {new_sl:.4f}\nTrailing SL ON!",
            tags="money_bag,lock"
        )
        log(f"TP1 hit {symbol} trail SL:{new_sl:.4f}")

    if t.get('tp1_hit'):
        step      = t['entry'] * 0.002
        new_trail = c['close'] - step
        if new_trail > t['trailing_sl']:
            t['trailing_sl'] = new_trail
            log(f"TRAIL updated {symbol} -> {new_trail:.4f}")

    if c['low'] <= t['trailing_sl']:
        result = 'win' if t.get('tp1_hit') else 'loss'
        notify(
            f"SL HIT {'(Profit Locked)' if result == 'win' else '(Loss)'}",
            f"Coin: {symbol}\nTrail SL: {t['trailing_sl']:.4f}\nResult: {result.upper()}\nStrategy: {t['strategy']}",
            tags="money_bag" if result == 'win' else "red_circle"
        )
        storage.close_trade(symbol, result)
        active_trades.pop(symbol, None)
        return

    if not t.get('dca_done') and c['low'] < t['entry'] * 0.995:
        dca_z = t.get('dca_zone', t['entry'] * 0.99)
        notify(
            "DCA Alert!",
            f"Coin: {symbol}\nPrice below entry!\nDCA Zone: {dca_z:.4f}\nEntry: {t['entry']:.4f}\nAvg if DCA: {((t['entry']+dca_z)/2):.4f}",
            tags="warning"
        )
        t['dca_done'] = True

# ═══════════════════════════════════
# HOURLY STATUS
# ═══════════════════════════════════
def hourly_status(state, active_trades, storage, exchange_name):
    if time.time() - state['last_report'] < 3600:
        return
    best_s, best_p = storage.get_best()
    active   = list(active_trades.keys()) or ["None"]
    priority = storage.get_watchlist_priority()[:3] or ["N/A"]
    notify(
        "Bot Active Alhamdulillah",
        f"Scans: {state['scan_count']}\n"
        f"Active: {', '.join(active)}\n"
        f"Daily Losses: {storage.data.get('daily_losses',0)}/{DAILY_LOSS_LIMIT}\n"
        f"Kill Switch: {'ON' if storage.is_kill_switch() else 'OFF'}\n"
        f"Recovery: {'ON' if storage.data.get('recovery_mode') else 'OFF'}\n"
        f"Exchange: {exchange_name}\n"
        f"Best Strategy: {best_s}\n"
        f"Best Pair: {best_p}\n"
        f"Top Watch: {', '.join(priority)}",
        tags="robot,white_check_mark"
    )
    state['last_report'] = time.time()
    state['scan_count']  = 0

# ═══════════════════════════════════
# NEWS
# ═══════════════════════════════════
def is_high_impact_news():
    now = datetime.now(timezone.utc)
    cur = now.hour * 60 + now.minute
    for (h, m) in HIGH_IMPACT_NEWS:
        if abs(cur - (h * 60 + m)) <= NEWS_BLOCK_MIN:
            return True
    return False

# ═══════════════════════════════════
# MAIN
# ═══════════════════════════════════
def main():
    exchange, exchange_name = connect_exchange()
    storage       = Storage()
    active_trades = {}
    start_time    = time.time()
    state         = {'last_report': time.time(), 'scan_count': 0}

    log("Genius Adaptive Sniper Bot v2 starting...")
    notify(
        "Bot Started",
        f"Genius Sniper v2 Live!\nBTC ETH SOL AVAX BNB\nExchange: {exchange_name}\n24/7 Halal Spot\nMin Conf: {MIN_CONFIDENCE}/100",
        tags="rocket"
    )

    while True:
        if time.time() - start_time > 19800:
            notify("Auto Restart", "5.5hr done restarting", tags="arrows_counterclockwise")
            log("Restarting...")
            break

        try:
            storage.send_reports()
            storage.reset_daily()
            hourly_status(state, active_trades, storage, exchange_name)

            now = datetime.now(timezone.utc)
            log(f"TIME {now.strftime('%d-%b %H:%M')} UTC")

            if storage.is_kill_switch():
                log("KILL SWITCH — skip trading")
                time.sleep(SCAN_SLEEP)
                continue

            if is_high_impact_news():
                log("News block 10min")
                time.sleep(600)
                continue

            session = get_session()
            state['scan_count'] += 1
            log(f"=== SCAN #{state['scan_count']} | {session} ===")

            priority_syms = storage.get_watchlist_priority()
            scan_order    = [s for s in priority_syms if s in SYMBOLS]
            for s in SYMBOLS:
                if s not in scan_order:
                    scan_order.append(s)

            for symbol in scan_order:
                log(f"-- {symbol} --")
                df15 = safe_fetch(exchange, exchange_name, symbol, TF_ENTRY)
                if df15 is None or df15.empty or len(df15) < 100:
                    log(f"{symbol}: no data skip")
                    continue

                if symbol in active_trades:
                    monitor_trade(df15, symbol, active_trades, storage)
                    continue

                try:
                    mkt, atr_v, atr_avg = detect_market(df15)
                    bias = get_1h_bias(exchange, exchange_name, symbol)
                    log(f"{symbol} mkt:{mkt} bias:{bias}")

                    if mkt == "QUIET":
                        log(f"{symbol}: quiet skip")
                        continue

                    if mkt in ["RANGING", "VOLATILE"]:
                        run_sweep(df15, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias)

                    if symbol not in active_trades:
                        run_ema_pullback(df15, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias)

                    if symbol not in active_trades:
                        run_breakout(df15, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias)

                    if symbol not in active_trades:
                        run_day_breakout(df15, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias)

                    if symbol not in active_trades:
                        run_opportunity(df15, symbol, active_trades, storage, mkt, atr_v, atr_avg, session, bias)

                    storage.update_watchlist(symbol, atr_v / max(atr_avg, 1e-10) * 50)

                except Exception as e:
                    log(f"Symbol ERR {symbol}: {e}")
                    continue

                time.sleep(1)

            log(f"=== SCAN #{state['scan_count']} DONE | wait {SCAN_SLEEP}s ===")
            time.sleep(SCAN_SLEEP)

        except Exception as e:
            log(f"MAIN ERR: {e}")
            time.sleep(60)
            continue

if __name__ == '__main__':
    main()
