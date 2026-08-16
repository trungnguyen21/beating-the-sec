"""
BSEC endurance load test — public API endpoints.

Simulates up to --peak-users concurrent users in an unpredictable chaotic
pattern (no time limit). Stop manually with Ctrl-C or the web UI stop button.

Traffic math (at 1k peak users, wait_time avg 20s):
  Total:                     ~50 req/s  (~3,000 req/min)
  GET /api/calendar 24h:     ~13 req/s  (  ~810 req/min)  27%
  GET /api/calendar extended:  ~5 req/s  (  ~270 req/min)   9%
  GET /api/guilds/{id}/watch: ~13 req/s  (  ~810 req/min)  27%
  POST .../watch/add:          ~9 req/s  (  ~540 req/min)  18%
  POST .../watch/remove:       ~9 req/s  (  ~540 req/min)  18%

Shape: weighted Markov chain over 4 states (dead / low / normal / spike).
Each state picks a random duration; the next state is chosen by probability
so the pattern is never the same run to run. Ramps use a smooth ease-in-out
curve. Dead zones can hit near-zero; spikes reach --peak-users.

Run:
    locust -f locustfile.py --host http://localhost:8000

Headless (no UI, runs until Ctrl-C):
    locust -f locustfile.py --host http://localhost:8000 --headless

Custom params (also available in web UI → Advanced options):
    --guild-ids 123,456    real guild IDs (blank = synthetic, 409s expected)
    --read-only            skip write endpoints
    --calendar-max-hours   upper bound for ?hours= (default 168)
    --peak-users           ceiling for spike state (default 1000)
    --trough-users         unused by chaotic shape (kept for compatibility)
    --jitter-pct           unused by chaotic shape (kept for compatibility)
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


# ── Chaotic state-machine shape ───────────────────────────────────────────────
#
# Instead of a fixed repeating wave, this uses a weighted Markov chain over
# four states. Each state picks a random duration and the next state is chosen
# probabilistically, so the pattern never repeats the same way twice.
#
# States (user counts and durations scale with --peak-users / --trough-users):
#   dead   — near-zero traffic, simulates a quiet period / overnight lull
#   low    — light traffic, below the trough floor
#   normal — sustained mid-level load
#   spike  — burst up to peak, short-lived
#
# Transition weights (read as: from state X, go to Y with relative weight W):
#   dead   → low(6)  normal(3)  spike(1)
#   low    → dead(2) normal(5)  spike(2)  low(1)
#   normal → low(3)  dead(1)    spike(3)  normal(3)
#   spike  → normal(5) low(3)   dead(2)
#
# Ramps between states use a smooth ease-in-out curve so lines in the graph
# look organic rather than perfectly linear.

_STATE_CFG = {
    #          lo_frac  hi_frac  min_s  max_s
    "dead":   (0.00,    0.05,    60,    300),
    "low":    (0.10,    0.35,    120,   600),
    "normal": (0.35,    0.75,    180,   900),
    "spike":  (0.80,    1.00,    60,    240),
}

_TRANSITIONS = {
    "dead":   [("low", 6),   ("normal", 3), ("spike", 1)],
    "low":    [("dead", 2),  ("normal", 5), ("spike", 2), ("low", 1)],
    "normal": [("low", 3),   ("dead", 1),   ("spike", 3), ("normal", 3)],
    "spike":  [("normal", 5),("low", 3),    ("dead", 2)],
}

_RAMP_S = 45  # seconds for smooth transition between states


def _ease(x: float) -> float:
    """Smooth-step: ease-in-out curve, input 0–1 → output 0–1."""
    return x * x * (3 - 2 * x)


def _state_users(state: str, peak: int) -> int:
    lo_f, hi_f, _, _ = _STATE_CFG[state]
    return random.randint(int(peak * lo_f), max(1, int(peak * hi_f)))


def _state_duration(state: str) -> int:
    _, _, dmin, dmax = _STATE_CFG[state]
    return random.randint(dmin, dmax)


def _next_state(current: str) -> str:
    opts = _TRANSITIONS[current]
    return random.choices([s for s, _ in opts], weights=[w for _, w in opts])[0]


class ChaoticShape(LoadTestShape):
    """
    Unpredictable state-machine shape — random spikes, dead zones, and
    sustained loads with no fixed cycle. Runs indefinitely.

    --peak-users   ceiling for spike state  (default 1000)
    --trough-users unused (shape derives floors from peak fractions)
    """

    def __init__(self):
        super().__init__()
        self._cur_state: str = "low"
        self._cur_users: int = 0
        self._state_end: float = 0.0
        self._ramp_start: float | None = None
        self._ramp_from: int = 0
        self._ramp_to: int = 0
        self._next_st: str = "low"
        self._ready: bool = False

    def tick(self):
        opts = self.runner.environment.parsed_options
        peak = opts.peak_users if opts else 1000
        t = self.get_run_time()

        if not self._ready:
            self._ready = True
            self._cur_users = _state_users(self._cur_state, peak)
            self._state_end = t + _state_duration(self._cur_state)

        # Mid-ramp
        if self._ramp_start is not None:
            elapsed = t - self._ramp_start
            if elapsed >= _RAMP_S:
                self._cur_state = self._next_st
                self._cur_users = self._ramp_to
                self._state_end = t + _state_duration(self._cur_state)
                self._ramp_start = None
            else:
                progress = _ease(elapsed / _RAMP_S)
                users = int(self._ramp_from + (self._ramp_to - self._ramp_from) * progress)
                return max(1, users), 50

        # State expired — pick next
        if t >= self._state_end:
            self._next_st = _next_state(self._cur_state)
            self._ramp_from = self._cur_users
            self._ramp_to = _state_users(self._next_st, peak)
            self._ramp_start = t

        return max(1, self._cur_users), 50
