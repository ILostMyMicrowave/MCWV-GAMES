-- OPTIONAL: run as a database administrator on MCWV BOT's PostgreSQL database.
-- Replace the password before execution and store the resulting connection URL
-- only in Northflank as MCWV_READONLY_DATABASE_URL.
--
-- This role cannot INSERT, UPDATE, DELETE, CREATE, ALTER, or DROP.

CREATE ROLE mcwv_games_reader
    LOGIN
    PASSWORD 'REPLACE_WITH_A_LONG_RANDOM_PASSWORD';

ALTER ROLE mcwv_games_reader SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE postgres TO mcwv_games_reader;
GRANT USAGE ON SCHEMA public TO mcwv_games_reader;
GRANT SELECT ON TABLE
    public.battles,
    public.war_snapshots,
    public.player_leaderboard_history,
    public.cross_clan_player_history,
    public.users
TO mcwv_games_reader;

-- To remove it later:
-- REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM mcwv_games_reader;
-- REVOKE USAGE ON SCHEMA public FROM mcwv_games_reader;
-- REVOKE CONNECT ON DATABASE postgres FROM mcwv_games_reader;
-- DROP ROLE mcwv_games_reader;
