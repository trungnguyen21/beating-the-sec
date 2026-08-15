"""
BSEC endurance load test — public API endpoints.

Simulates ~1,000 concurrent users in a repeating wave pattern (no time limit).
Stop manually with Ctrl-C or the web UI stop button.

Traffic math (at 1k users, wait_time avg 20s):
  Total:                     ~50 req/s  (~3,000 req/min)
  GET /api/calendar 24h:     ~13 req/s  (  ~810 req/min)  27%
  GET /api/calendar extended:  ~5 req/s  (  ~270 req/min)   9%
  GET /api/guilds/{id}/watch: ~13 req/s  (  ~810 req/min)  27%
  POST .../watch/add:          ~9 req/s  (  ~540 req/min)  18%
  POST .../watch/remove:       ~9 req/s  (  ~540 req/min)  18%

At trough (300 users):       ~15 req/s  (  ~900 req/min)

Jitter: ±10% of the current target, applied via two overlapping sine waves
at different periods (137s and 89s). They drift in and out of phase so the
load looks organic rather than perfectly flat during holds.

Wave cycle (60 min, repeating):
  0–5 min   ramp  100 → peak   (default 1,000)
  5–20 min  hold  peak  + jitter
  20–25 min drop  peak → trough (default 300)
  25–40 min hold  trough + jitter
  40–45 min ramp  trough → peak
  45–60 min hold  peak  + jitter
  → repeat

Run:
    locust -f locustfile.py --host http://localhost:8000

Headless (no UI, runs until Ctrl-C):
    locust -f locustfile.py --host http://localhost:8000 --headless

Custom params (also available in web UI → Advanced options):
    --guild-ids 123,456    real guild IDs (blank = synthetic, 409s expected)
    --read-only            skip write endpoints
    --calendar-max-hours   upper bound for ?hours= (default 168)
    --peak-users           peak user count per wave (default 1000)
    --trough-users         trough user count per wave (default 300)
    --jitter-pct           jitter amplitude as fraction of target (default 0.10)
"""

import math
import random

from locust import HttpUser, LoadTestShape, between, events, task


# ── Custom parameters ────────────────────────────────────────────────────────

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--guild-ids", type=str, default="",
        help="Comma-separated real guild IDs. Blank = synthetic IDs (409s on write endpoints, expected).",
        env_var="LOCUST_GUILD_IDS",
    )
    parser.add_argument(
        "--read-only", action="store_true", default=False,
        help="Skip /watch/add and /watch/remove.",
        env_var="LOCUST_READ_ONLY",
    )
    parser.add_argument(
        "--calendar-max-hours", type=int, default=168,
        help="Upper bound for ?hours= on /api/calendar (default 168).",
        env_var="LOCUST_CALENDAR_MAX_HOURS",
    )
    parser.add_argument(
        "--peak-users", type=int, default=1000,
        help="User count at wave peaks (default 1000).",
        env_var="LOCUST_PEAK_USERS",
    )
    parser.add_argument(
        "--trough-users", type=int, default=300,
        help="User count at wave troughs (default 300).",
        env_var="LOCUST_TROUGH_USERS",
    )
    parser.add_argument(
        "--jitter-pct", type=float, default=0.10,
        help="Jitter amplitude as a fraction of the target user count (default 0.10 = ±10%%).",
        env_var="LOCUST_JITTER_PCT",
    )


def _guild_ids(env) -> list[str]:
    raw = env.parsed_options.guild_ids if env.parsed_options else ""
    if raw:
        return [g.strip() for g in raw.split(",") if g.strip()]
    return [str(100_000_000_000_000_000 + i) for i in range(20)]


# ── User behaviour ────────────────────────────────────────────────────────────

_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "AMD", "INTC", "NFLX", "ORCL", "CRM", "ADBE", "QCOM", "AVGO",
]


class PublicAPIUser(HttpUser):
    """
    Endurance user — long wait between actions to simulate realistic humans,
    not a stress hammer. At 5k users this produces ~250 req/s sustained.
    """

    # Avg 20s between actions → endurance, not stress
    wait_time = between(10, 30)

    def on_start(self) -> None:
        self.guild_id = random.choice(_guild_ids(self.environment))
        self.read_only = self.environment.parsed_options.read_only
        self.cal_max = self.environment.parsed_options.calendar_max_hours

    @task(3)
    def get_calendar_default(self) -> None:
        self.client.get("/api/calendar", name="/api/calendar?hours=24")

    @task(1)
    def get_calendar_extended(self) -> None:
        hours = random.randint(25, self.cal_max)
        self.client.get(f"/api/calendar?hours={hours}", name="/api/calendar?hours=extended")

    @task(3)
    def get_watchlist(self) -> None:
        self.client.get(
            f"/api/guilds/{self.guild_id}/watch",
            name="/api/guilds/{guild_id}/watch",
        )

    @task(2)
    def add_tickers(self) -> None:
        if self.read_only:
            return
        tickers = random.sample(_TICKERS, k=random.randint(1, 3))
        with self.client.post(
            f"/api/guilds/{self.guild_id}/watch/add",
            json={"tickers": tickers},
            name="/api/guilds/{guild_id}/watch/add",
            catch_response=True,
        ) as resp:
            # 409 = guild not set up — expected for synthetic IDs, not a failure
            if resp.status_code in (200, 409):
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}: {resp.text[:120]}")

    @task(2)
    def remove_tickers(self) -> None:
        if self.read_only:
            return
        tickers = random.sample(_TICKERS, k=random.randint(1, 2))
        with self.client.post(
            f"/api/guilds/{self.guild_id}/watch/remove",
            json={"tickers": tickers},
            name="/api/guilds/{guild_id}/watch/remove",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 409):
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}: {resp.text[:120]}")


# ── Wave shape (repeating, no end) ────────────────────────────────────────────

def _jitter(t: int, base: int, pct: float) -> int:
    """
    Smooth organic jitter via two sine waves with prime-ish periods (137s, 89s).
    They drift in/out of phase so holds never look perfectly flat.
    Only applied during hold phases — ramps stay clean.
    """
    amplitude = int(base * pct)
    wave = (
        0.6 * math.sin(t * 2 * math.pi / 137) +
        0.4 * math.sin(t * 2 * math.pi / 89)
    )
    return int(amplitude * wave)


class WaveShape(LoadTestShape):
    """
    Repeating 60-minute wave with two peaks per cycle and organic jitter on
    hold phases. Runs indefinitely — stop via web UI or Ctrl-C.

    Peak/trough/jitter come from --peak-users / --trough-users / --jitter-pct.

    Cycle layout (seconds):
        0   – 300   ramp  100 → peak
        300 – 1200  hold  peak   ± jitter   (15 min)
        1200– 1500  ramp  peak → trough
        1500– 2400  hold  trough ± jitter   (15 min)
        2400– 2700  ramp  trough → peak
        2700– 3600  hold  peak   ± jitter   (15 min)
        → repeat from 0
    """

    _CYCLE = 3600

    def tick(self):
        opts = self.runner.environment.parsed_options
        peak       = opts.peak_users   if opts else 1000
        trough     = opts.trough_users if opts else 300
        jitter_pct = opts.jitter_pct   if opts else 0.10

        t = int(self.get_run_time()) % self._CYCLE

        if t < 300:
            # Ramp up: 100 → peak (clean, no jitter)
            users = int(100 + (peak - 100) * t / 300)
            return users, 50

        elif t < 1200:
            # Hold peak + jitter
            users = max(1, peak + _jitter(t, peak, jitter_pct))
            return users, 50

        elif t < 1500:
            # Ramp down: peak → trough (clean)
            progress = (t - 1200) / 300
            users = int(peak - (peak - trough) * progress)
            return users, 50

        elif t < 2400:
            # Hold trough + jitter
            users = max(1, trough + _jitter(t, trough, jitter_pct))
            return users, 50

        elif t < 2700:
            # Ramp up: trough → peak (clean)
            progress = (t - 2400) / 300
            users = int(trough + (peak - trough) * progress)
            return users, 50

        else:
            # Hold second peak + jitter
            users = max(1, peak + _jitter(t, peak, jitter_pct))
            return users, 50
