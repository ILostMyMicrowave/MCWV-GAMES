"""One-time, allowlisted migration from MCWV BOT DB to the isolated Games DB.

Required environment variables:
  SOURCE_DATABASE_URL   Existing MCWV BOT PostgreSQL URL (read only is enough)
  DATABASE_URL          New MCWV Games PostgreSQL URL
  MIGRATE_CONFIRM       Must equal COPY_MCWV_GAMES

Only game-prefixed tables and settings keys beginning with ``games_`` are copied.
No clan, ticket, application, invite, giveaway, user, or war-history table is copied.
"""
import io
import os
import sys

import psycopg2
from psycopg2 import sql

SOURCE_URL = os.environ.get("SOURCE_DATABASE_URL", "").strip()
DESTINATION_URL = os.environ.get("DATABASE_URL", "").strip()
CONFIRM = os.environ.get("MIGRATE_CONFIRM", "").strip()

GAME_TABLES = [
    "mcwv_coins",
    "mcwv_coin_log",
    "mcwv_game_testers",
    "mcwv_cases",
    "mcwv_case_contents",
    "mcwv_case_rolls",
    "mcwv_pet_collections",
    "mcwv_duels",
    "mcwv_game_stats",
    "mcwv_bingo_cards",
    "mcwv_game_eggs",
    "mcwv_game_pets",
    "mcwv_user_stats",
    "mcwv_guess_profiles",
    "mcwv_lottery_tickets",
    "mcwv_tower_scores",
    "mcwv_game_cooldowns",
    "mcwv_petdle_progress",
    "mcwv_lottery_draws",
]


def connect(url):
    return psycopg2.connect(url, sslmode=os.environ.get("DB_SSLMODE", "require"), connect_timeout=10)


def table_exists(connection, table):
    with connection.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        return cur.fetchone()[0] is not None


def common_columns(source, destination, table):
    query = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """
    with source.cursor() as cur:
        cur.execute(query, (table,))
        source_columns = [row[0] for row in cur.fetchall()]
    with destination.cursor() as cur:
        cur.execute(query, (table,))
        destination_columns = {row[0] for row in cur.fetchall()}
    return [column for column in source_columns if column in destination_columns]


def copy_query(source, destination, select_query, target_table, columns):
    buffer = io.StringIO()
    with source.cursor() as cur:
        cur.copy_expert(
            sql.SQL("COPY ({}) TO STDOUT WITH (FORMAT CSV, HEADER FALSE)").format(select_query).as_string(source),
            buffer,
        )
    buffer.seek(0)
    with destination.cursor() as cur:
        statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)").format(
            sql.Identifier(target_table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )
        cur.copy_expert(statement.as_string(destination), buffer)


def reset_serial_sequence(destination, table):
    with destination.cursor() as cur:
        cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table,))
        row = cur.fetchone()
        sequence = row[0] if row else None
        if not sequence:
            return
        cur.execute(sql.SQL("SELECT COALESCE(MAX(id), 0) FROM {}").format(sql.Identifier(table)))
        maximum = int(cur.fetchone()[0] or 0)
        if maximum > 0:
            cur.execute("SELECT setval(%s, %s, TRUE)", (sequence, maximum))
        else:
            cur.execute("SELECT setval(%s, 1, FALSE)", (sequence,))


def initialize_destination():
    # Importing registers commands but does not connect to Discord or start the web server.
    os.environ.setdefault("GUILD_ID", "1")
    import games_bot

    games_bot.DATABASE_URL = DESTINATION_URL
    games_bot.conn = connect(DESTINATION_URL)
    games_bot.conn.autocommit = True
    games_bot.init_base_schema()
    games_bot.init_games_tables()
    games_bot.conn.close()
    games_bot.conn = None


def main():
    if not SOURCE_URL or not DESTINATION_URL:
        raise SystemExit("SOURCE_DATABASE_URL and DATABASE_URL are required")
    if SOURCE_URL == DESTINATION_URL:
        raise SystemExit("Refusing migration: source and destination URLs are identical")
    if CONFIRM != "COPY_MCWV_GAMES":
        raise SystemExit("Set MIGRATE_CONFIRM=COPY_MCWV_GAMES to authorize the one-time copy")

    initialize_destination()
    source = connect(SOURCE_URL)
    destination = connect(DESTINATION_URL)
    source.set_session(readonly=True, autocommit=True)
    destination.autocommit = False

    try:
        existing = [table for table in GAME_TABLES if table_exists(source, table)]
        with destination.cursor() as cur:
            if existing:
                cur.execute(
                    sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(
                        sql.SQL(", ").join(map(sql.Identifier, existing))
                    )
                )
            cur.execute("DELETE FROM settings WHERE key LIKE 'games_%'")

        results = []
        for table in existing:
            columns = common_columns(source, destination, table)
            if not columns:
                continue
            select_query = sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.Identifier(table),
            )
            copy_query(source, destination, select_query, table, columns)
            with destination.cursor() as cur:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
                count = int(cur.fetchone()[0])
            reset_serial_sequence(destination, table)
            results.append((table, count))

        if table_exists(source, "settings"):
            columns = ["key", "value"]
            copy_query(
                source,
                destination,
                sql.SQL("SELECT key, value FROM settings WHERE key LIKE 'games_%' AND value IS NOT NULL"),
                "settings",
                columns,
            )

        # A migrated deployment always starts testing-only. The owner must
        # deliberately enable it after Discord role/channel verification.
        with destination.cursor() as cur:
            cur.execute(
                """INSERT INTO settings (key, value) VALUES ('games_enabled', '0')
                   ON CONFLICT (key) DO UPDATE SET value = '0'"""
            )

        destination.commit()
        print("Migration committed successfully. Copied:")
        for table, count in results:
            print(f"  {table}: {count:,} row(s)")
        print("  settings: game-prefixed keys only")
    except Exception:
        destination.rollback()
        raise
    finally:
        source.close()
        destination.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # psycopg2 can include malformed DSN tokens (including passwords) in
        # parser errors, so never echo raw exception text from this credentialed tool.
        print(f"Migration failed and was rolled back: {type(exc).__name__}", file=sys.stderr)
        raise
