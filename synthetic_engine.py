import time
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Optional, List, Literal


class TimeframeConfig:
    """
    Config for a synthetic timeframe.
    kind="duration": fixed number of seconds per bar (e.g. 1s, 5s, 1m).
    kind="calendar": bars follow calendar boundaries (day, week, month, quarter, year, multi-year).
    """
    def __init__(
        self,
        name: str,
        kind: Literal["duration", "calendar"],
        size_seconds: Optional[int] = None,
        calendar_type: Optional[str] = None,
        years_span: Optional[int] = None,
    ):
        self.name = name
        self.kind = kind
        self.size_seconds = size_seconds
        self.calendar_type = calendar_type  # "day", "week", "month", "quarter", "year", "multi-year"
        self.years_span = years_span        # for multi-year (2y, 5y, 10y)


class SyntheticCandle:
    """
    Tracks a single synthetic bar: start/end timestamps and OHLCV.
    """
    def __init__(self, start_ts: int, end_ts: int, price: float, volume: float = 0.0):
        self.start_ts = int(start_ts)
        self.end_ts = int(end_ts)
        self.o = float(price)
        self.h = float(price)
        self.l = float(price)
        self.c = float(price)
        self.v = float(volume or 0.0)

    def update(self, price: float, volume: float = 0.0):
        p = float(price)
        self.c = p
        if p > self.h:
            self.h = p
        if p < self.l:
            self.l = p
        self.v += float(volume or 0.0)

    def to_dict(self, now_ts: Optional[float] = None) -> Dict:
        now_ts = now_ts or time.time()
        elapsed = max(0, int(now_ts - self.start_ts))
        total = max(1, int(self.end_ts - self.start_ts))
        remaining = max(0, total - elapsed)
        complete = now_ts >= self.end_ts

        start_dt = datetime.utcfromtimestamp(self.start_ts)
        iso_start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "t": iso_start,
            "o": self.o,
            "h": self.h,
            "l": self.l,
            "c": self.c,
            "v": self.v,
            "elapsed": elapsed,
            "remaining": remaining,
            "complete": complete,
        }


class SyntheticEngine:
    """
    Synthetic Candle Engine.

    Maintains latest candle for each (symbol, timeframe).
    For live use: ingest_tick() is called for each real tick.
    """
    def __init__(self, tf_configs: List[TimeframeConfig]):
        # state[symbol][tf_name] = SyntheticCandle
        self.state: Dict[str, Dict[str, SyntheticCandle]] = defaultdict(dict)
        self.tf_configs: Dict[str, TimeframeConfig] = {cfg.name: cfg for cfg in tf_configs}

    def _bounds_duration(self, ts: int, size_seconds: int):
        start_ts = ts - (ts % size_seconds)
        end_ts = start_ts + size_seconds
        return int(start_ts), int(end_ts)

    def _bounds_calendar(self, ts: int, cfg: TimeframeConfig):
        dt = datetime.utcfromtimestamp(ts)

        # Day
        if cfg.calendar_type == "day":
            start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)

        # Week (ISO: Monday start)
        elif cfg.calendar_type == "week":
            start_day = dt - timedelta(days=dt.weekday())
            start = start_day.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=7)

        # Month
        elif cfg.calendar_type == "month":
            start = datetime(dt.year, dt.month, 1)
            if dt.month == 12:
                end = datetime(dt.year + 1, 1, 1)
            else:
                end = datetime(dt.year, dt.month + 1, 1)

        # Quarter (3-month blocks: Jan–Mar, Apr–Jun, Jul–Sep, Oct–Dec)
        elif cfg.calendar_type == "quarter":
            q_index = (dt.month - 1) // 3  # 0,1,2,3
            first_month = 1 + 3 * q_index
            start = datetime(dt.year, first_month, 1)
            end_month = first_month + 3
            end_year = dt.year
            if end_month > 12:
                end_month -= 12
                end_year += 1
            end = datetime(end_year, end_month, 1)

        # Year
        elif cfg.calendar_type == "year":
            start = datetime(dt.year, 1, 1)
            end = datetime(dt.year + 1, 1, 1)

        # Multi-year: 2y, 5y, 10y
        elif cfg.calendar_type == "multi-year" and cfg.years_span:
            span = cfg.years_span
            base_year = (dt.year // span) * span
            start = datetime(base_year, 1, 1)
            end = datetime(base_year + span, 1, 1)

        else:
            # Fallback: treat as a 1-year bar
            start = datetime(dt.year, 1, 1)
            end = datetime(dt.year + 1, 1, 1)

        return int(start.timestamp()), int(end.timestamp())

    def _bounds_for_tf(self, ts: int, cfg: TimeframeConfig):
        if cfg.kind == "duration":
            return self._bounds_duration(ts, cfg.size_seconds)
        return self._bounds_calendar(ts, cfg)

    def ingest_tick(self, symbol: str, price: float, volume: float, ts: int):
        """
        Ingest a real tick (symbol, price, volume, unix_ts_seconds) and
        update all timeframe candles for this symbol.
        """
        if not self.tf_configs:
            return

        for tf_name, cfg in self.tf_configs.items():
            start_ts, end_ts = self._bounds_for_tf(ts, cfg)
            sym_state = self.state[symbol]
            bar = sym_state.get(tf_name)

            # New bar if none or boundaries changed
            if bar is None or bar.start_ts != start_ts or bar.end_ts != end_ts:
                bar = SyntheticCandle(start_ts, end_ts, price, volume)
                sym_state[tf_name] = bar
            else:
                bar.update(price, volume)

    def get_candle(self, symbol: str, tf_name: str) -> Optional[Dict]:
        """
        Return the latest candle for (symbol, timeframe) or None.
        """
        sym_state = self.state.get(symbol)
        if not sym_state:
            return None
        bar = sym_state.get(tf_name)
        if not bar:
            return None
        return bar.to_dict()
