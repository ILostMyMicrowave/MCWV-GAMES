# MCWV Games

Standalone Discord games/economy bot extracted from MCWV BOT.

## Isolation guarantees

This project has its own:

- Discord application and token
- GitHub repository and deployment history
- Render service
- PostgreSQL database
- health endpoint and logs

It does **not** contain ticket, application, clan-management, invite, giveaway,
war-management, dashboard, or MCWV admin-web commands. Its 30 registered guild
commands are game/economy commands only.

Nothing in this repository automatically modifies or deploys MCWV BOT. Do not
reuse MCWV BOT's Discord token or edit its Render service when deploying this bot.

## Included commands

`/games`, `/gamesguide`, `/gamesadmin`, `/coins`, `/daily`, `/pay`, `/deposit`,
`/withdraw`, `/shop`, `/cases`, `/case`, `/caseadmin`, `/coinsadmin`, `/guess`,
`/pets`, `/hatch`, `/eggs`, `/duel`, `/scramble`, `/hangman`, `/petdle`, `/spin`,
`/scratch`, `/bingo`, `/trivia`, `/historytrivia`, `/lottery`, `/tower`,
`/toweranswer`, and `/top`.

## 1. Create the Discord application

1. Open <https://discord.com/developers/applications>.
2. Select **New Application** and name it **MCWV Games**.
3. On **Bot**, copy/reset the new token and save it privately.
4. Enable **Server Members Intent** and **Message Content Intent**.
5. On **Installation**, configure a Guild Install with `bot` and
   `applications.commands` scopes.
6. Grant only View Channels, Send Messages, Embed Links, Attach Files, Add
   Reactions, Read Message History, Use External Emojis, and Manage Roles.
7. Invite it to the MCWV server.
8. Put its Discord role above every role cases are allowed to award. Do not give
   it Administrator.

Never commit or share either bot token. MCWV Games must use a new token.

## 2. Create an isolated games database

Create a new PostgreSQL/Supabase database and keep its connection string. The bot
creates its own `settings` and `mcwv_*` game tables on first startup.

To retain existing balances/cases/statistics, perform the one-time migration
before enabling public games:

```bash
export SOURCE_DATABASE_URL='existing MCWV BOT database URL'
export DATABASE_URL='new MCWV Games database URL'
export MIGRATE_CONFIRM='COPY_MCWV_GAMES'
python migrate_game_data.py
```

Safety properties of the migration:

- refuses identical source/destination URLs;
- reads the source in PostgreSQL read-only mode;
- copies only the explicit `mcwv_*` game-table allowlist;
- copies only `settings` keys beginning with `games_`;
- never copies ticket, clan, application, invite, giveaway, user, or war tables;
- wraps destination changes in a transaction;
- forces `games_enabled=0` after migration, so the new bot starts testing-only.

Back up both databases first. Disable games on the old bot and let active rounds
finish before taking the migration snapshot.

## 3. Create the GitHub repository

Create an empty private repository named `MCWV-GAMES`, then push this directory:

```bash
git init
git add .
git commit -m "Initial standalone MCWV Games bot"
git branch -M main
git remote add origin YOUR_PRIVATE_REPOSITORY_URL
git push -u origin main
```

Check `git status` before pushing. `.env` files are ignored; `.env.example`
contains names only.

## 4. Deploy to Render

1. In a separate Render workspace, choose **New → Web Service**.
2. Connect the private `MCWV-GAMES` repository and select `main`.
3. Use the repository's Dockerfile runtime and the Free instance type while testing.
4. Set the health-check path to `/health`.
5. Add the runtime secrets below in Render's Environment settings.
6. Create the service and inspect its build and runtime logs.
7. Keep Auto-Deploy enabled only after private validation is complete.

Required runtime variables:

```text
DISCORD_TOKEN=<new MCWV Games token>
GUILD_ID=<MCWV Discord server ID>
OWNER_ID=<owner Discord user ID>
DATABASE_URL=<new isolated games database URL>
DB_SSLMODE=require
CLAN_NAME=MCWV
PORT=10000
```

Optional:

```text
MCWV_READONLY_DATABASE_URL=<SELECT-only URL to MCWV BOT's database>
```

The optional connection supplies live war facts to War Bingo and database-backed
MCWV History Trivia. Without it, the service remains fully isolated, uses public
battle lookup where available, and History Trivia has a static fallback. If live
integration is wanted, create a restricted PostgreSQL login using
`docs/optional-readonly-war-integration.sql`; never use MCWV BOT's full-write URL
for this variable.

Expected health response at `https://YOUR-SERVICE/health`:

```json
{
  "service": "MCWV Games",
  "status": "ok",
  "discord_ready": true,
  "database_ready": true,
  "event_loop_lag_ms": 0.0,
  "guilds": 1
}
```

The health check now runs a bounded real `SELECT 1` and returns HTTP 503 while
Discord or PostgreSQL is unavailable. Optional `DB_STATEMENT_TIMEOUT_MS` and
`DB_LOCK_TIMEOUT_MS` variables may override the runtime defaults (1800ms and
900ms); leave them unset unless troubleshooting with a database administrator.

Render Free services may sleep after inactivity. An HTTP monitor such as
UptimeRobot can check `/health` every five minutes during testing; a healthy
response requires both Discord and PostgreSQL to be ready.

## 5. Test privately before cutover

The migrated bot starts testing-only. Guild owners and configured testers can use
it while public members cannot.

1. Use `/gamesadmin tester add` if needed.
2. Use `/gamesadmin game staff roles` to choose operational game-staff roles.
3. Use `/gamesadmin sync pets+eggs`.
4. Configure one private spawn channel.
5. Test `/coins`, `/daily`, `/guess`, `/shop`, case role grants, duels, Tower,
   Trivia, Lottery, restarts, and `/health`.
6. With a normal tester, verify three free hatches per rolling 24 hours, followed
   by prepaid use or a 100-coin deduction for each extra hatch.
7. Confirm MCWV BOT's tickets, applications, clan commands, invites, giveaways,
   Render service, and token are unchanged.

## 6. Safe cutover

Do not allow both applications to settle public games simultaneously.

1. Turn games off on MCWV BOT and allow active sessions to finish.
2. Migrate game data once.
3. Deploy and privately verify MCWV Games.
4. Remove/disable game commands and game message handling in MCWV BOT in a
   separate reviewed deployment.
5. Sync MCWV BOT's command tree so its old game commands disappear.
6. Use MCWV Games `/gamesadmin toggle` to enable public games.

This order prevents duplicate rewards, duplicate random spawns, and command
confusion.

## Local validation

```bash
python -m py_compile games_bot.py migrate_game_data.py test_standalone.py
python -m pyflakes games_bot.py migrate_game_data.py test_standalone.py
python test_standalone.py
GUILD_ID=1501608673250640055 python -c \
  "import games_bot; print(len(games_bot.bot.tree.get_commands(guild=games_bot.guild_obj)))"
```

The expected command count is `30`. The standalone suite also enforces early
acknowledgement for all slash commands and every audited component/modal callback.
