# Development Guide

## Local dev

```bash
docker compose up -d              # postgres + migrate + admin + pipeline + scheduler + bot
uv run pytest -q -m "not e2e"     # test suite
```

- Local Postgres: `127.0.0.1:5433` (creds `bsec`/`bsec`, db `beatingthesec`)
- Admin dashboard: `http://localhost:8000/admin`
- After changing code: `docker compose up -d --build <service>` to rebuild + recreate just that service

## Discord bot: dev vs. prod

Two separate Discord applications — never mix tokens.

| | Bot | Token | Guild(s) | Command sync |
|---|---|---|---|---|
| **Dev** (local) | `[TEST]-FinancialsWatcher` | local `.env` | Signal Trading only | instant (`DISCORD_DEV_GUILD_ID` set) |
| **Prod** (droplet) | `FinancialsWatcher` | droplet `.env` | all real guilds | global, ~1hr propagation |

Test bot changes locally in Signal Trading first. Never put the prod token in local `.env`.

## Ship a change

```bash
git add -A && git commit -m "..." && git push origin feat/discord-bot
ssh <user>@<server_ip>
cd /opt/beating-sec
git pull
docker compose up -d --build      # rebuilds changed services, reruns migrate (idempotent)
```

## Verify after deploy

```bash
docker compose ps                                # all services Up
docker compose logs bot --tail 20                # "Synced commands globally", correct guild count
curl -u admin:<ADMIN_PASSWORD> http://<SERVER_IP>:8000/admin -o /dev/null -w "%{http_code}\n"
```

## Earnings calendar refresh

`db/refresh_calendar.py` runs automatically — no host cron needed. The `scheduler` service
(`db/scheduler_loop.py`) runs it once on container startup, then every day at 05:00 ET, forever
(`restart: unless-stopped` survives crashes/reboots).

```bash
docker compose ps scheduler                # confirm it's running
docker compose logs -f scheduler           # "Next earnings-calendar refresh in X.Xh"
```

To trigger a refresh manually right now, without disturbing the running scheduler loop, run a
one-off container from the same image (works locally or on the droplet — same command):

```bash
docker compose run --rm scheduler python -m db.refresh_calendar
```

## Notes

- `.env` is never committed (local and droplet copies are independent, gitignored)
- Droplet Postgres is bound to `127.0.0.1:5433` only — reach it via `ssh -L 15433:127.0.0.1:5433 <user>@<server_ip>` if you need direct access
- No CI/CD yet — `pytest` and deploy are both manual
