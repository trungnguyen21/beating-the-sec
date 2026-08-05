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
ssh root@137.184.156.7
cd /opt/beating-sec
git pull
docker compose up -d --build      # rebuilds changed services, reruns migrate (idempotent)
```

## Verify after deploy

```bash
docker compose ps                                # all services Up
docker compose logs bot --tail 20                # "Synced commands globally", correct guild count
curl -u admin:<ADMIN_PASSWORD> http://137.184.156.7:8000/admin -o /dev/null -w "%{http_code}\n"
```

## Notes

- `.env` is never committed (local and droplet copies are independent, gitignored)
- Droplet Postgres is bound to `127.0.0.1:5433` only — reach it via `ssh -L 15433:127.0.0.1:5433 root@137.184.156.7` if you need direct access
- No CI/CD yet — `pytest` and deploy are both manual
