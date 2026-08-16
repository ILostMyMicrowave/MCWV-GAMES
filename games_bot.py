"""MCWV Games — standalone Discord games/economy bot.

This service intentionally contains no clan, ticket, application, invite,
giveaway, war-tracking, or MCWV admin-web functionality.
"""
import asyncio
import difflib
import hashlib
import json
import math
import os
import re
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from flask import Flask, jsonify
from werkzeug.serving import make_server
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageOps
import psycopg2
from psycopg2.extras import execute_values

APP_NAME = "MCWV Games"
TOKEN = os.environ.get("DISCORD_TOKEN", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
GUILD_ID = int(os.environ.get("GUILD_ID", "0") or 0)
OWNER_IDS = {
    int(value.strip()) for value in os.environ.get("OWNER_IDS", os.environ.get("OWNER_ID", "")).split(",")
    if value.strip().isdigit()
}
PORT = int(os.environ.get("PORT", "10000") or 10000)
CLAN_NAME = os.environ.get("CLAN_NAME", "MCWV").strip() or "MCWV"
PS99_API = "https://ps99.biggamesapi.io"
MCWV_READONLY_DATABASE_URL = os.environ.get("MCWV_READONLY_DATABASE_URL", "").strip()
# Standalone games staff are configured through /gamesadmin. No ticket/staff
# roles from MCWV BOT are imported into this service.
ALLOWED_ROLE_ID = 0
MCWV_TICKET_STAFF_ROLE_IDS = ()

if not GUILD_ID:
    raise RuntimeError("Missing required GUILD_ID environment variable")

guild_obj = discord.Object(id=GUILD_ID)
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True


class MCWVGamesBot(commands.Bot):
    async def close(self):
        if "games_housekeeping_loop" in globals() and games_housekeeping_loop.is_running():
            games_housekeeping_loop.cancel()
        if session is not None and not session.closed:
            await session.close()
        if conn is not None and conn.closed == 0:
            conn.close()
        await super().close()


bot = MCWVGamesBot(command_prefix="!!", intents=intents)

conn = None
session = None
_db_lock = threading.RLock()


def _connect_database():
    global conn
    if not DATABASE_URL:
        return None
    with _db_lock:
        if conn is not None and conn.closed == 0:
            return conn
        conn = psycopg2.connect(
            DATABASE_URL,
            sslmode=os.environ.get("DB_SSLMODE", "require"),
            connect_timeout=8,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
        )
        conn.autocommit = True
        return conn


def ensure_db_connection():
    try:
        return _connect_database()
    except Exception as exc:
        print(f"[database] connection failed: {type(exc).__name__}: {exc}")
        return None


async def ensure_db_connection_async():
    return await asyncio.to_thread(ensure_db_connection)


_db_heal_scheduled = False


async def _heal_database():
    global _db_heal_scheduled
    try:
        await ensure_db_connection_async()
    finally:
        _db_heal_scheduled = False


def db_enabled():
    """Fail fast and heal a dropped connection without blocking Discord's loop."""
    global _db_heal_scheduled
    if conn is not None and conn.closed == 0:
        return True
    if DATABASE_URL and not _db_heal_scheduled:
        try:
            loop = asyncio.get_running_loop()
            _db_heal_scheduled = True
            loop.create_task(_heal_database())
        except RuntimeError:
            return ensure_db_connection() is not None
    return False


def init_base_schema():
    if not db_enabled():
        raise RuntimeError("Database is unavailable")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)


def db_get_setting(key, default=None):
    if not db_enabled():
        return default
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s", (str(key),))
            row = cur.fetchone()
        return row[0] if row else default
    except Exception as exc:
        print(f"[database] setting read failed: {exc}")
        return default


def db_set_setting(key, value):
    if not db_enabled():
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO settings (key, value) VALUES (%s, %s)
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""",
                (str(key), str(value)),
            )
        return True
    except Exception as exc:
        print(f"[database] setting write failed: {exc}")
        return False


def db_is_owner_discord(discord_id):
    return int(discord_id) in OWNER_IDS


def _friendly_battle_name(battle_id):
    text = str(battle_id or "Unknown Battle")
    return re.sub(r'(\d+)', r' \1', re.sub(r'([a-z])([A-Z])', r'\1 \2', text)).strip()


def _readonly_connection():
    """Optional SELECT-only connection to MCWV BOT's war-history database."""
    if not MCWV_READONLY_DATABASE_URL:
        return None
    try:
        return psycopg2.connect(
            MCWV_READONLY_DATABASE_URL,
            sslmode=os.environ.get("DB_SSLMODE", "require"),
            connect_timeout=5,
        )
    except Exception as exc:
        print(f"[mcwv-readonly] connection failed: {exc}")
        return None


def get_battles_war_state():
    """Read the latest scheduled war without ever writing to MCWV BOT's DB."""
    worker = _readonly_connection()
    if worker is None:
        return None
    try:
        worker.set_session(readonly=True, autocommit=True)
        with worker.cursor() as cur:
            cur.execute("""
                SELECT battle_id, battle_name,
                       EXTRACT(EPOCH FROM start_time)::bigint,
                       EXTRACT(EPOCH FROM end_time)::bigint,
                       COALESCE(manually_edited, FALSE)
                FROM battles
                WHERE start_time IS NOT NULL OR end_time IS NOT NULL
                ORDER BY COALESCE(end_time, start_time) DESC LIMIT 1
            """)
            row = cur.fetchone()
        if not row:
            return None
        return {
            "battle_id": row[0], "battle_name": row[1],
            "start": int(row[2]) if row[2] is not None else None,
            "finish": int(row[3]) if row[3] is not None else None,
            "manually_edited": bool(row[4]),
        }
    except Exception as exc:
        print(f"[mcwv-readonly] war state failed: {exc}")
        return None
    finally:
        worker.close()


async def get_active_battle_id_for_placement():
    """Prefer the read-only MCWV schedule, then use the public active-war API."""
    state = await asyncio.to_thread(get_battles_war_state)
    now = time.time()
    if state and (not state.get("start") or state["start"] <= now) and (
            not state.get("finish") or state["finish"] > now):
        return str(state["battle_id"])
    try:
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.get(
                f"{PS99_API}/api/activeClanBattle",
                headers={"User-Agent": "MCWV-Games/1.0"},
            ) as response:
                if response.status != 200:
                    return None
                payload = await response.json(content_type=None)
        data = payload.get("data", payload) if isinstance(payload, dict) else {}
        if not isinstance(data, dict):
            return None
        config = data.get("configData") or data.get("ConfigData") or {}
        candidates = (data, config if isinstance(config, dict) else {})
        for item in candidates:
            for key in ("BattleID", "battleId", "battle_id", "_id", "Id"):
                if item.get(key):
                    return str(item[key])
    except Exception as exc:
        print(f"[games] active battle lookup failed: {exc}")
    return None


app = Flask(__name__)


@app.get("/")
@app.get("/health")
def health():
    return jsonify({
        "service": APP_NAME,
        "status": "ok",
        "discord_ready": bool(bot.is_ready()),
        "database_ready": db_enabled(),
        "guilds": len(bot.guilds),
    }), 200


def run_health_server():
    make_server("0.0.0.0", PORT, app, threaded=True).serve_forever()


# ---------------- MCWV GAMES & ECONOMY (TESTING) ----------------
# Stage-gated: games are hidden until the owner flips games_enabled on.
# Allowed during testing: owner (infinite balance) + listed testers.

GAMES_SETTING_ENABLED = "games_enabled"
GAMES_SETTING_SPAWN_CHANCE = "games_spawn_chance"       # percent, e.g. "1" = 1%
GAMES_SETTING_SPAWN_CHANNELS = "games_spawn_channels"   # JSON list of channel ids
GAMES_SETTING_JACKPOT = "games_jackpot"
GAMES_SETTING_JACKPOT_SEED = "games_jackpot_seed"
GAMES_SETTING_LOTTERY_POOL = "games_lottery_pool"
GAMES_SETTING_LOTTERY_END = "games_lottery_end_ts"
GAMES_SETTING_INTEREST_RATE = "games_interest_rate_pct"
GAMES_SETTING_INTEREST_CAP = "games_interest_cap"
GAMES_SETTING_STAFF_ROLES = "games_staff_role_ids"       # JSON list, owner-managed

GAMES_DEFAULT_CHANCE_PCT = 1
GAMES_SPAWN_CHANNEL_COOLDOWN = 300      # seconds between spawns in one channel
GAMES_SPAWN_GLOBAL_COOLDOWN = 1200      # seconds between spawns server-wide (3/hr)
GAMES_ROUND_TIMEOUT = 120               # generic timed-round limit (Guess uses its own clock)
GAMES_DUEL_TIMEOUT = 45                 # seconds to answer a duel round
GAMES_INTEREST_RATE_PCT_DEFAULT = 1     # 1% per day
GAMES_INTEREST_CAP_DEFAULT = 100000     # interest only on first 100k banked
GAMES_DAILY_BASE = 100
GAMES_DAILY_STREAK_BONUS = 10
GAMES_DAILY_STREAK_REWARD_CAP = 30      # streak keeps growing; payout tops out at day 30
GAMES_HATCH_FREE_PER_DAY = 3
GAMES_HATCH_COST = 100
GAMES_SPIN_COST_EXTRA = 250
GAMES_LOTTERY_TICKET_COST = 50
GAMES_PAY_MIN = 10
GAMES_MAX_TRANSACTION = 1_000_000_000   # protects commands/embeds from absurd values
GAMES_MAX_ANSWER_ATTEMPTS = 8           # valid pet-name guesses per round per user
GAMES_GUESS_TIMEOUT = 90                 # quicker rounds with exact 30s/60s hint drops
GAMES_GUESS_HINT_TIMES = (30, 60)
GAMES_GUESS_BASE_REWARD = 250
GAMES_GUESS_MIN_REWARD = 125
GAMES_GUESS_MAX_REWARD = 500
GAMES_SCRAMBLE_MAX_ATTEMPTS = 5
GAMES_HANGMAN_MAX_FULL_GUESSES = 3
GAMES_TRIVIA_COOLDOWN = 15 * 60         # repeatable, but not an infinite coin printer
GAMES_HISTORY_TRIVIA_COOLDOWN = 30 * 60
GAMES_SCRAMBLE_CHANNEL_COOLDOWN = 3 * 60
GAMES_HANGMAN_CHANNEL_COOLDOWN = 3 * 60
GAMES_TOWER_FLOOR_TIMEOUT = 60
GAMES_TOWER_MAX_FLOOR = 25               # bounded economy; reaching the roof is a win
GAMES_TRIVIA_CORRECT_REWARD = 25
GAMES_TRIVIA_PERFECT_BONUS = 100
GAMES_PETDLE_MAX_GUESSES = 6
GAMES_SCRATCH_PITY_MISSES = 4           # the next losing card is upgraded to a pair
GAMES_SHOP_CASE_LIMIT = 10               # keeps the embed under Discord's 6,000-char cap

# Rarity odds (fun-first, locked): titanic 0.5, huge 2, exclusive 6, epic 15, rare 25, common rest
GAMES_HATCH_TIERS = (
    ("titanic", 0.5),
    ("huge", 2.0),
    ("exclusive", 6.0),
    ("epic", 15.0),
    ("rare", 25.0),
    ("common", None),  # remainder
)

# Real pet seed (name -> rbxassetid), harvested from public db.biggames.io pages.
# The daily item sync can extend this later.
GAMES_PET_SEED = {
    "Huge Basket Bunny": "75145545226238",
    "Huge Blurred Axolotl": "89262001520583",
    "Huge Corrupt Butterfly": "18882974863",
    "Huge Leprechaun Kitsune": "100650487607408",
    "Huge Lucki Angelus": "139803624702732",
    "Huge Mining Monkey": "104634632776696",
    "Huge Player Fox": "101605254258375",
    "Huge Rogue Squid": "82176106497018",
    "Huge Shuriken Corgi": "18978050941",
    "Huge Temporal Owl": "18882992626",
    "Titanic Angry Yeti": "131125477985744",
    "Titanic Arcane Halo Cat": "131125477985744",
    "Titanic Arcane Void Cat": "131125477985744",
    "Titanic Axolotl": "131125477985744",
    "Titanic Bread Shiba": "131125477985744",
    "Titanic Calico Cat": "131125477985744",
    "Titanic Captain Octopus": "131125477985744",
    "Titanic Cheerful Yeti": "131125477985744",
    "Titanic Chest Mimic": "131125477985744",
    "Titanic Clover Butterfly": "131125477985744",
    "Titanic Disco Ball Agony": "131125477985744",
    "Titanic Dot Matrix Kitsune": "131125477985744",
    "Titanic Helicopter Corgi": "131125477985744",
    "Titanic Irish Wolfhound": "125815340060379",
    "Titanic Lucki Chest Mimic": "82078892883300",
    "Titanic Mucki": "131125477985744",
    "Titanic Nyan Cat": "131125477985744",
    "Titanic Pink Lucky Block": "131125477985744",
    "Titanic Prickly Panda": "131125477985744",
    "Titanic Sandcastle Kraken": "90627425215733",
    "Titanic Smiley Penguin": "131125477985744",
    "Titanic Super Coral Stingray": "131125477985744",
    "Gargantuan Capybara": "131125477985744",
    "Gargantuan Fluffy Cat": "131125477985744",
}

GAMES_EGG_SEED = [
    {
        "name": "Clan Egg",
        "emoji": "\U0001f95a",
        "description": "The MCWV test egg — full rarity ladder.",
        "tiers": [
            ("titanic", 0.5, [n for n in GAMES_PET_SEED if n.startswith("Titanic")]),
            ("huge", 2.0, [n for n in GAMES_PET_SEED if n.startswith("Huge") or n.startswith("Gargantuan")]),
            ("exclusive", 6.0, ["Exclusive Dragon", "Exclusive Phoenix"]),
            ("epic", 15.0, ["Epic Unicorn", "Epic Griffin"]),
            ("rare", 25.0, ["Rare Fox", "Rare Panda"]),
            ("common", None, ["Clan Kitten", "Clan Puppy", "Clan Bird"]),
        ],
    },
]

GAMES_TRIVIA_SEED = [
    ("How many Huges can you hatch from the Clan Egg test pool?", ["5", "10", "11", "20"], 2),
    ("What is the strongest pet rarity in PS99?", ["Huge", "Exclusive", "Titanic", "Gargantuan"], 3),
    ("Which of these is a real PS99 Titanic?", ["Titanic Nyan Cat", "Titanic Mega Dog", "Titanic Ultra Cat", "Titanic Bob"], 0),
    ("What does RAP stand for?", ["Recent Average Price", "Rare Active Pets", "Random Auction Price", "Rapid Auction Points"], 0),
    ("Which battle did MCWV finish #24 in?", ["Ninja Battle 2026", "Gummy Battle 2026", "Lunar Battle 2026", "Soccer Battle 2026"], 0),
    ("What's the fun hatch rate for a Titanic in the Clan Egg?", ["0.05%", "0.5%", "5%", "50%"], 1),
    ("What currency do clan war games use?", ["Coins", "Gems", "Diamonds", "Stars"], 0),
    ("Which pet starts with 'Gargantuan'?", ["Capybara", "Cat", "Dog", "Dragon"], 0),
    ("How many free hatches do you get per day?", ["1", "2", "3", "5"], 2),
    ("Where do rare hatch announcements go?", ["Nowhere", "The channel it was won in", "DMs", "Voice"], 1),
    ("What do you spend coins on in the shop?", ["Cases and game items", "Server roles", "Real money", "War points"], 0),
    ("How long is a /daily streak gap before it resets?", ["24 hours", "48 hours", "1 week", "Never"], 1),
]

# ============================================================
# GAMES UI KIT — shared design system (colors, bars, cards)
# ============================================================
GAMES_UI = {
    "gold": 0xF5C842,      # coins / economy
    "purple": 0x6C22F5,    # brand / guess / tower
    "violet": 0xA855F7,    # eggs / cases
    "pink": 0xF87171,      # duels
    "blue": 0x3B82F6,      # pixel / info
    "cyan": 0x0EA5E9,      # scratch extras / spins
    "green": 0x22C55E,     # wins
    "red": 0xEF4444,       # losses
    "slate": 0x64748B,     # neutral / nothing
    "amber": 0xF59E0B,     # jackpot / titanic
    "indigo": 0x6366F1,    # trivia
    "silver": 0xC0C5CE,    # 2nd place
    "bronze": 0xCD7F32,    # 3rd place
}


def games_color(name, fallback="purple"):
    return discord.Color(GAMES_UI.get(name, GAMES_UI.get(fallback, 0x6C22F5)))


def games_bar(value, total, length=10):
    """Unicode progress bar (clamped)."""
    if total <= 0:
        return "░" * length
    filled = int(round(min(max(float(value), 0.0), float(total)) / float(total) * length))
    return ("█" * filled) + ("░" * (length - filled))


def games_hearts(lives, total=3):
    return ("❤️" * max(lives, 0)) + ("🖤" * max(total - lives, 0))


# Full emoji sequences (NOT string slices — these are multi-codepoint emoji)
GAMES_ABCD = ["🅰️", "🅱️", "🅲️", "🅳️"]


def games_money(n):
    return f"🪙 **{int(n):,}**"


def games_tier_style(tier):
    """(emoji, label, rgb tuple) per pet tier."""
    return {
        "titanic": ("🌌", "TITANIC", (245, 158, 11)),
        "huge": ("💥", "HUGE", (168, 85, 247)),
        "gargantuan": ("🌠", "GARGANTUAN", (236, 72, 153)),
        "exclusive": ("💎", "Exclusive", (59, 130, 246)),
        "epic": ("💜", "Epic", (192, 132, 252)),
        "rare": ("💙", "Rare", (96, 165, 250)),
        "common": ("⚪", "Common", (156, 163, 175)),
    }.get(tier, ("❔", str(tier).capitalize(), (156, 163, 175)))


def games_pet_roll_tier(pet_name, chance=None, egg=None):
    """Classify a hatch by the pet itself, never just by a tiny probability."""
    name = str(pet_name or "").lower()
    if name.startswith("titanic "):
        return "titanic"
    if name.startswith("gargantuan "):
        return "gargantuan"
    if name.startswith("huge "):
        return "huge"
    egg_rarity = str((egg or {}).get("rarity") or "").lower()
    if "exclusive" in egg_rarity:
        return "exclusive"
    c = float(chance or 0)
    return "exclusive" if c < 10 else "epic" if c < 20 else "rare" if c < 30 else "common"


def games_tier_from_chance(chance, pet_name=None, egg=None):
    """(emoji, label) for an egg-content row."""
    tier = games_pet_roll_tier(pet_name, chance, egg)
    emoji, label, _rgb = games_tier_style(tier)
    return emoji, label


def games_coin_rank(user_id):
    """Leaderboard rank by cash+bank. None when unavailable."""
    try:
        if not db_enabled():
            return None
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(*) + 1 FROM mcwv_coins c2
                   WHERE (c2.balance + c2.bank) >
                         (SELECT COALESCE(balance,0) + COALESCE(bank,0) FROM mcwv_coins WHERE discord_id = %s)""",
                (int(user_id),),
            )
            return int(cur.fetchone()[0] or 1)
    except Exception:
        return None


def games_footer(embed, text):
    """Append a bit of text to the embed footer (never raises)."""
    try:
        base = str(embed.footer.text or "")
        embed.set_footer(text=(base + " · " + text) if base else text)
    except Exception:
        pass


async def games_animate(msg, frames, delay=0.45):
    """Edit a message through a list of content frames (best-effort)."""
    for f in frames:
        await asyncio.sleep(delay)
        try:
            await msg.edit(content=f)
        except Exception:
            return


def games_wallet_embed(target, title=None):
    """The wallet card used by /coins and quick-action views."""
    bal = games_coin_balance(target.id)
    total = bal["balance"] + bal["bank"]
    unlimited = " · ∞ unlimited (testing)" if games_is_unlimited(target.id) else ""
    embed = discord.Embed(
        title=title or f"🪙 {target.display_name}'s Wallet",
        color=games_color("gold"),
    )
    avatar = getattr(target, "display_avatar", None) or getattr(target, "avatar", None)
    if avatar is not None:
        embed.set_thumbnail(url=avatar.url)
    rank = games_coin_rank(target.id)
    if rank:
        embed.set_author(name=f"💰 Rank #{rank} on the coin leaderboard")
    embed.add_field(name="💵 Cash", value=f"**{bal['balance']:,}**{unlimited}", inline=True)
    embed.add_field(name="🏦 Bank", value=f"**{bal['bank']:,}**", inline=True)
    embed.add_field(name="💼 Net worth", value=f"**{total:,}**", inline=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(amount), 0) FROM mcwv_coin_log WHERE target_id = %s AND amount > 0", (int(target.id),))
            earned = int(cur.fetchone()[0] or 0)
            cur.execute("SELECT COALESCE(SUM(-amount), 0) FROM mcwv_coin_log WHERE target_id = %s AND amount < 0", (int(target.id),))
            spent = int(cur.fetchone()[0] or 0)
        lifetime_txt = f"📈 **{earned:,}** earned · 📉 **{spent:,}** spent"
    except Exception:
        lifetime_txt = "📈 lifetime stats unavailable"
    prepaid = {
        "🥚 Hatches": games_prepaid_get(target.id, "hatch"),
        "🎡 Spins": games_prepaid_get(target.id, "spin"),
        "🎴 Scratches": games_prepaid_get(target.id, "scratch"),
    }
    prepaid_txt = " · ".join(f"{k} ×{v}" for k, v in prepaid.items() if v) or "none banked"
    embed.add_field(name="📦 Prepaid items", value=prepaid_txt, inline=False)
    embed.add_field(name="📊 Lifetime", value=lifetime_txt, inline=False)
    games_footer(embed, "💳 /pay · 🏦 /deposit /withdraw · 1%/day bank interest (cap 100k)")
    return embed


class CoinsQuickView(discord.ui.View):
    """Quick actions attached to wallet cards."""

    def __init__(self, owner_id):
        super().__init__(timeout=180)
        self._owner = int(owner_id)
        self.message = None

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    async def _guard(self, interaction):
        if interaction.user.id != self._owner:
            await interaction.response.send_message("This wallet isn't yours — run `/coins`.", ephemeral=True)
            return False
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass
        return True

    @discord.ui.button(label="Daily", style=discord.ButtonStyle.success, emoji="📅", row=0)
    async def b_daily(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await interaction.followup.send(
            "📅 Claim your check-in with **`/daily`** — 100 coins + 10 per streak day.", ephemeral=True)

    @discord.ui.button(label="Shop", style=discord.ButtonStyle.primary, emoji="🛒", row=0)
    async def b_shop(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        view = ShopView()
        view._set_owner(interaction.user.id)
        view.message = await interaction.followup.send(
            embed=games_shop_embed(interaction.user.id), view=view, ephemeral=True, wait=True
        )

    @discord.ui.button(label="Games", style=discord.ButtonStyle.primary, emoji="🎮", row=0)
    async def b_games(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await interaction.followup.send(embed=await games_hub_embed(interaction.user), ephemeral=True)

    @discord.ui.button(label="Leaderboard", style=discord.ButtonStyle.secondary, emoji="🏆", row=0)
    async def b_top(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await interaction.followup.send(embed=await games_top_embed(interaction.user.id), ephemeral=True)


GAMES_SHOP_ITEMS = [
    ("hatch", "🥚 Extra Hatch", GAMES_HATCH_COST, "3 free daily"),
    ("spin", "🎡 Extra Spin", GAMES_SPIN_COST_EXTRA, "1 free daily"),
    ("scratch", "🎴 Extra Scratch", 100, "1 free daily"),
]


def games_shop_items(user_id):
    """Live shop data for the embed + button states."""
    bal = games_coin_balance(user_id)
    unlimited = games_is_unlimited(user_id)
    hatches_used, _, hatches_reset = games_free_status(user_id, "hatch")
    spins_used, _, spins_reset = games_free_status(user_id, "spin")
    scratch_used, _, scratch_reset = games_free_status(user_id, "scratch")
    owned = {
        "hatch": games_prepaid_get(user_id, "hatch"),
        "spin": games_prepaid_get(user_id, "spin"),
        "scratch": games_prepaid_get(user_id, "scratch"),
    }
    return {
        "balance": int(bal["balance"]),
        "bank": int(bal["bank"]),
        "unlimited": unlimited,
        "free": {
            "hatch": (hatches_used, hatches_reset),
            "spin": (spins_used, spins_reset),
            "scratch": (scratch_used, scratch_reset),
        },
        "owned": owned,
        "lottery_owned": games_lottery_owned(user_id),
        "pool": games_lottery_pool(),
    }


def games_case_catalog(enabled_only=True, limit=15):
    """Case definitions + role odds for the shop and staff dashboard."""
    if not db_enabled():
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, name, price, emoji, enabled
                   FROM mcwv_cases
                   WHERE (%s = FALSE OR enabled = TRUE)
                   ORDER BY enabled DESC, price ASC, LOWER(name) ASC
                   LIMIT %s""",
                (bool(enabled_only), max(1, min(int(limit), 100))),
            )
            cases = cur.fetchall()
            out = []
            for cid, name, price, emoji, enabled in cases:
                cur.execute(
                    "SELECT role_id, chance FROM mcwv_case_contents WHERE case_id = %s ORDER BY chance DESC, role_id",
                    (int(cid),),
                )
                contents = [(int(rid), float(chance)) for rid, chance in cur.fetchall()]
                out.append({
                    "id": int(cid), "name": str(name), "price": int(price),
                    "emoji": str(emoji or "🎁"), "enabled": bool(enabled), "contents": contents,
                })
            return out
    except Exception as exc:
        print(f"[games] case catalog failed: {exc}")
        return []


def games_shop_embed(user_id, page=0):
    d = games_shop_items(user_id)
    unlimited_txt = " · ♾️ testing balance" if d["unlimited"] else ""
    all_cases = games_case_catalog(enabled_only=True, limit=100)
    page_count = max(1, math.ceil(len(all_cases) / GAMES_SHOP_CASE_LIMIT))
    page = max(0, min(int(page or 0), page_count - 1))
    start = page * GAMES_SHOP_CASE_LIMIT
    cases = all_cases[start:start + GAMES_SHOP_CASE_LIMIT]
    embed = discord.Embed(
        title="[MCWV] Clan & Community!'s Shop",
        color=games_color("gold"),
        description=(
            f"**Your balance:** {d['balance']:,} credits{unlimited_txt}\n\n"
            "Click the buttons below to buy an item!\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "## Cases\n"
            "Open cases to win random roles! Each case contains different items with varying chances. "
            "The rarer the item, the lower the chance!"
            + (f"\n\nShowing cases **{start + 1}–{min(start + GAMES_SHOP_CASE_LIMIT, len(all_cases))}** "
               f"of **{len(all_cases)}**." if len(all_cases) > GAMES_SHOP_CASE_LIMIT else "")
        ),
    )
    if not cases:
        embed.add_field(name="No cases available", value="Staff can create one from `/caseadmin panel`.", inline=False)
    for case in cases:
        total = sum(chance for _rid, chance in case["contents"])
        lines = [f"<@&{rid}> (**{chance:.1f}%**)" for rid, chance in case["contents"][:8]]
        if len(case["contents"]) > 8:
            lines.append(f"*…and {len(case['contents']) - 8} more role(s)*")
        nothing = max(0.0, 100.0 - total)
        if nothing > 0.0001:
            lines.append(f"Nothing (**{nothing:.1f}%**)")
        inside = "\n".join(lines) if lines else "*No prizes configured yet.*"
        embed.add_field(
            name=f"{case['emoji']} {case['name']} (♾️ Unlimited)",
            value=(
                f"**Cost:** {case['price']:,} credits\n\n"
                f"**What's inside:**\n{inside}\n\n"
                f"Use the **Open {case['name']}** button below."
            )[:1024],
            inline=False,
        )
    extras = []
    for kind, label, price, free_desc in GAMES_SHOP_ITEMS:
        used, reset = d["free"][kind]
        reset_txt = f" · resets {discord.utils.format_dt(reset, 'R')}" if reset else ""
        extras.append(
            f"{label} — **{price:,} credits** · {used} used ({free_desc}){reset_txt} · banked **×{d['owned'][kind]}**"
        )
    embed.add_field(
        name="━━━━━━━━━━━━━━━━━━━━\nGame Extras",
        value="\n".join(extras),
        inline=False,
    )
    nxt = games_lottery_next_draw()
    embed.add_field(
        name=f"🎟 Lottery Ticket — {GAMES_LOTTERY_TICKET_COST} credits",
        value=(f"Pool: **{d['pool']:,} credits** · your tickets: **{d['lottery_owned']}**/"
               f"{GAMES_LOTTERY_WEEKLY_TICKET_CAP}\nNext draw {discord.utils.format_dt(nxt, 'R')} · winner takes 70%"),
        inline=False,
    )
    page_text = f" · Cases page {page + 1}/{page_count}" if page_count > 1 else ""
    games_footer(embed, "Discord places buttons directly below embeds; case buttons follow the same order as the cases above" + page_text)
    return embed


async def games_hub_embed(user):
    """The /games hub card (also reused by quick views)."""
    bal = games_coin_balance(user.id)
    featured_slug = games_featured_egg()
    featured = next((e for e in games_get_eggs() if e["slug"] == featured_slug), None)
    daily_state = "✅ claimed" if await games_daily_claimed(user.id) else "⚠️ not yet — run `/daily`"
    embed = discord.Embed(
        title="🎮 MCWV Games Hub",
        description=(
            f"**Your wallet:** 🪙 `{bal['balance']:,}` cash · 🏦 `{bal['bank']:,}` bank · "
            f"💼 `{bal['balance'] + bal['bank']:,}` total\n"
        ),
        color=games_color("purple"),
    )
    embed.add_field(name="📅 Daily", value=daily_state, inline=True)
    hatches_used, _, hatches_reset = games_free_status(user.id, "hatch")
    spins_used, _, spins_reset = games_free_status(user.id, "spin")
    hatches_txt = f"`{hatches_used}/{GAMES_HATCH_FREE_PER_DAY}` free"
    if hatches_reset:
        hatches_txt += f" · resets {discord.utils.format_dt(hatches_reset, 'R')}"
    spins_txt = f"`{spins_used}/1` free"
    if spins_reset:
        spins_txt += f" · resets {discord.utils.format_dt(spins_reset, 'R')}"
    embed.add_field(name="🥚 Hatches today", value=hatches_txt, inline=True)
    embed.add_field(name="🎡 Spins today", value=spins_txt, inline=True)
    scratch_used, _, _scratch_reset = games_free_status(user.id, "scratch")
    tower_used, _, tower_reset = games_free_status(user.id, "tower")
    petdle_state = "✅ solved" if games_petdle_solved_today(user.id) else "`/petdle`"
    embed.add_field(name="🐾 Petdle today", value=petdle_state, inline=True)
    embed.add_field(name="🎴 Scratch today", value=f"`{scratch_used}/1` free", inline=True)
    tower_txt = f"`{tower_used}/{GAMES_TOWER_RUNS_PER_DAY}` runs"
    if tower_reset:
        tower_txt += f" · resets {discord.utils.format_dt(tower_reset, 'R')}"
    embed.add_field(name="🏗 Tower today", value=tower_txt, inline=True)
    embed.add_field(
        name="✨ Featured egg",
        value=f"**{featured['name']}** — top-tier odds doubled!" if featured else "—",
        inline=False,
    )
    embed.add_field(
        name="🎰 Progressive jackpot",
        value=f"**{games_jackpot_get():,}** 🪙 — hit it on `/spin`!",
        inline=False,
    )
    embed.add_field(
        name="🎲 Play",
        value="`/hatch` · `/trivia` · `/scramble` · `/hangman` · `/petdle` · `/spin` · `/scratch` · `/tower` · `/bingo`",
        inline=False,
    )
    embed.add_field(
        name="🪙 Economy",
        value="`/daily` · `/coins` · `/pay` · `/deposit` · `/shop` · `/cases` · `/lottery` · `/top`",
        inline=False,
    )
    embed.add_field(
        name="⚔️ Compete",
        value="`/duel @player <wager>` · `/top games` · `/historytrivia`",
        inline=False,
    )
    games_footer(embed, "📖 /gamesguide · all rewards are fun coins")
    return embed


async def games_daily_claimed(user_id):
    """True when the user already claimed their daily in the last 24h."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_daily_at FROM mcwv_coins WHERE discord_id = %s AND last_daily_at > NOW() - INTERVAL '24 hours'",
                (int(user_id),),
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def games_petdle_solved_today(user_id):
    """True when today's UTC Petdle is solved (calendar day, not drifting 24h)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT solved FROM mcwv_petdle_progress WHERE discord_id = %s AND day = CURRENT_DATE",
                (int(user_id),),
            )
            row = cur.fetchone()
            return bool(row and row[0])
    except Exception:
        return False


async def games_top_embed(user_id, games=None):
    """Leaderboard embed (coins by default, or per-game wins)."""
    if games:
        if games == "tower":
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT discord_id, best_floor, best_score, runs FROM mcwv_tower_scores
                        ORDER BY best_floor DESC, best_score DESC LIMIT 10
                    """)
                    tower_rows = cur.fetchall()
            except Exception:
                tower_rows = []
            lines = [
                f"{['🥇','🥈','🥉'][i] if i < 3 else f'`#{i + 1}`'} <@{uid}> — floor **{floor}** · **{score:,}** score · {runs} runs"
                for i, (uid, floor, score, runs) in enumerate(tower_rows)
            ]
            embed = discord.Embed(
                title="🏗 Tower Champions",
                description="\n".join(lines) if lines else "No completed runs yet — start with `/tower`!",
                color=games_color("purple"),
            )
            games_footer(embed, "Ranked by best floor, then best score")
            return embed
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT discord_id, wins, plays FROM mcwv_user_stats
                    WHERE game = %s
                    ORDER BY wins DESC, plays DESC LIMIT 10
                """, (games,))
                rows = cur.fetchall()
        except Exception as exc:
            print(f"[games] top games fetch failed: {exc}")
            rows = []
        if not rows:
            embed = discord.Embed(title=f"🏆 Top — {games}", description="No plays yet — be the first!", color=games_color("purple"))
            return embed
        max_wins = max(int(r[1] or 0) for r in rows) or 1
        medals = ["🥇", "🥈", "🥉"] + ["4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        lines = []
        for i, (uid, wins, plays) in enumerate(rows):
            winrate = (int(wins or 0) / int(plays or 1)) * 100
            bar = games_bar(int(wins or 0), max_wins, 8)
            lines.append(
                f"{medals[i]} <@{uid}> — **{int(wins or 0)}** wins · {winrate:.0f}% rate\n`{bar}`")
        embed = discord.Embed(
            title=f"🏆 Top {games.title()} Players",
            description="\n".join(lines),
            color=games_color("purple"),
        )
        my_wins = games_user_wins(user_id, games)
        embed.set_footer(text=f"Your {games} wins: {my_wins}")
        return embed
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT discord_id, balance, bank FROM mcwv_coins WHERE balance + bank > 0 ORDER BY balance + bank DESC LIMIT 10")
            rows = cur.fetchall()
    except Exception as exc:
        print(f"[games] top fetch failed: {exc}")
        rows = []
    if not rows:
        embed = discord.Embed(title="🪙 Richest Members", description="No coins minted yet — `/daily` to start!", color=games_color("gold"))
        return embed
    top_total = int(rows[0][1] or 0) + int(rows[0][2] or 0) or 1
    medals = ["🥇", "🥈", "🥉"] + ["4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    lines = []
    for i, (uid, bal, bank) in enumerate(rows):
        total = int(bal or 0) + int(bank or 0)
        bar = games_bar(total, top_total, 8)
        lines.append(f"{medals[i]} <@{uid}> — 🪙 **{total:,}**\n`{bar}`")
    embed = discord.Embed(
        title="🪙 Richest Members",
        description="\n".join(lines),
        color=games_color("gold"),
    )
    rank = games_coin_rank(user_id)
    if rank:
        embed.set_footer(text=f"Your rank: #{rank} · /daily, games, duels and bank interest all pay")
    return embed


# ---------------- GATE + TESTERS ----------------

def games_enabled():
    return str(db_get_setting(GAMES_SETTING_ENABLED, "0")) == "1"


def games_owner_check(member):
    """Owner = Discord guild owner or an OWNER_ID/OWNER_IDS environment entry."""
    if member is None:
        return False
    g = getattr(member, "guild", None)
    if g is not None and g.owner_id == member.id:
        return True
    return db_is_owner_discord(member.id)


def games_staff_role_ids():
    """Configured game-staff roles; legacy staff roles are the first-run default."""
    fallback = {int(ALLOWED_ROLE_ID)} if ALLOWED_ROLE_ID else set()
    fallback.update(int(rid) for rid in (MCWV_TICKET_STAFF_ROLE_IDS or []) if rid)
    raw = db_get_setting(GAMES_SETTING_STAFF_ROLES, None)
    if raw is None:
        return fallback
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        return {int(rid) for rid in (parsed or []) if int(rid) > 0}
    except Exception:
        return fallback


def games_set_staff_role_ids(role_ids):
    """Persist the exact game-staff role set. Returns (ok, ids/error)."""
    ids = sorted({int(rid) for rid in (role_ids or []) if int(rid) > 0})[:25]
    if not db_enabled():
        return False, "Database is not available."
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO settings (key, value) VALUES (%s, %s)
                       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""",
                    (GAMES_SETTING_STAFF_ROLES, json.dumps(ids)),
                )
        return True, ids
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def games_staff_check(member):
    """Game staff can manage cases and manually start/stop Guess the Pet."""
    if games_owner_check(member):
        return True
    if member is None or not isinstance(member, discord.Member):
        return False
    member_roles = {int(getattr(role, "id", 0)) for role in getattr(member, "roles", [])}
    return bool(member_roles & games_staff_role_ids())


def games_case_staff_check(member):
    """Compatibility name used by the case manager UI."""
    return games_staff_check(member)


def games_is_tester(user_id):
    try:
        if not db_enabled():
            return False
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM mcwv_game_testers WHERE discord_id = %s", (int(user_id),))
            return cur.fetchone() is not None
    except Exception as exc:
        print(f"[games] tester check failed: {exc}")
        return False


def games_gate_allowed(interaction):
    """True when the user may use games (public on, or owner/tester during testing)."""
    if games_enabled():
        return True
    user = interaction.user
    if games_owner_check(user if isinstance(user, discord.Member) else None):
        return True
    if isinstance(user, discord.Member) and user.guild is not None and user.guild.owner_id == user.id:
        return True
    return games_is_tester(user.id)


def games_is_unlimited(user_id):
    """Virtual spending is testing-only.

    The old behaviour left the owner unlimited after public launch, which made
    `/pay`, duels and lottery tickets an accidental infinite coin mint.
    """
    if games_enabled() or not db_enabled():
        return False
    if db_is_owner_discord(user_id):
        return True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT is_unlimited FROM mcwv_coins WHERE discord_id = %s", (int(user_id),))
            row = cur.fetchone()
            return bool(row and row[0])
    except Exception:
        return False


def games_gate_message(user_id):
    if games_enabled():
        return True
    if db_is_owner_discord(user_id):
        return True
    return games_is_tester(user_id)


def games_new_db_connection():
    """Dedicated connection for worker-thread game jobs.

    The main bot connection must never be shared with `asyncio.to_thread` jobs;
    doing that can mix transactions from interest, bingo, lottery and commands.
    """
    if not DATABASE_URL:
        return None
    c = psycopg2.connect(
        DATABASE_URL,
        sslmode=os.environ.get("DB_SSLMODE", "require"),
        connect_timeout=5,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )
    c.autocommit = True
    return c


def games_cooldown_claim(subject_id, game, seconds):
    """Atomically claim a persistent cooldown. Returns (ok, retry_at)."""
    if not db_enabled():
        return False, None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO mcwv_game_cooldowns (subject_id, game, last_played_at)
                   VALUES (%s, %s, NOW())
                   ON CONFLICT (subject_id, game) DO UPDATE SET last_played_at = NOW()
                   WHERE mcwv_game_cooldowns.last_played_at <=
                         NOW() - (%s * INTERVAL '1 second')
                   RETURNING last_played_at""",
                (int(subject_id), str(game)[:40], max(0, int(seconds))),
            )
            row = cur.fetchone()
            if row:
                return True, row[0] + timedelta(seconds=max(0, int(seconds)))
            cur.execute(
                "SELECT last_played_at FROM mcwv_game_cooldowns WHERE subject_id = %s AND game = %s",
                (int(subject_id), str(game)[:40]),
            )
            old = cur.fetchone()
        retry_at = old[0] + timedelta(seconds=max(0, int(seconds))) if old and old[0] else None
        return False, retry_at
    except Exception as exc:
        print(f"[games] cooldown claim failed: {exc}")
        return False, None


def games_coin_transfer(sender_id, recipient_id, amount):
    """Move coins in one transaction so a crash cannot lose or duplicate them."""
    sender_id, recipient_id, amount = int(sender_id), int(recipient_id), int(amount)
    if not db_enabled():
        return False, "Database is not available."
    if amount <= 0 or amount > GAMES_MAX_TRANSACTION:
        return False, f"Amount must be between 1 and {GAMES_MAX_TRANSACTION:,}."
    virtual = games_is_unlimited(sender_id)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO mcwv_coins (discord_id) VALUES (%s), (%s) ON CONFLICT (discord_id) DO NOTHING",
                    (sender_id, recipient_id),
                )
                if virtual:
                    cur.execute("SELECT balance FROM mcwv_coins WHERE discord_id = %s", (sender_id,))
                    sender_after = int(cur.fetchone()[0] or 0)
                else:
                    cur.execute(
                        """UPDATE mcwv_coins
                           SET balance = balance - %s, total_spent = total_spent + %s
                           WHERE discord_id = %s AND balance >= %s
                           RETURNING balance""",
                        (amount, amount, sender_id, amount),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise ValueError("Not enough coins.")
                    sender_after = int(row[0])
                cur.execute(
                    """UPDATE mcwv_coins
                       SET balance = balance + %s, total_earned = total_earned + %s
                       WHERE discord_id = %s RETURNING balance""",
                    (amount, amount, recipient_id),
                )
                recipient_after = int(cur.fetchone()[0])
                cur.execute(
                    """INSERT INTO mcwv_coin_log
                       (actor_id, target_id, type, amount, balance_after, meta)
                       VALUES (%s,%s,%s,%s,%s,%s::jsonb),
                              (%s,%s,'pay_received',%s,%s,%s::jsonb)""",
                    (
                        sender_id, sender_id, "pay_sent_test" if virtual else "pay_sent",
                        0 if virtual else -amount, sender_after,
                        json.dumps({"to": recipient_id, "virtual": virtual}),
                        sender_id, recipient_id, amount, recipient_after,
                        json.dumps({"from": sender_id, "virtual_source": virtual}),
                    ),
                )
        return True, recipient_after
    except ValueError as exc:
        return False, str(exc)
    except Exception as exc:
        print(f"[games] transfer failed: {exc}")
        return False, f"{type(exc).__name__}: {exc}"


def games_bank_move(user_id, amount, direction):
    """Race-safe cash↔bank move. Returns (ok, balances_or_error)."""
    user_id, amount = int(user_id), int(amount)
    if amount <= 0 or amount > GAMES_MAX_TRANSACTION:
        return False, f"Amount must be between 1 and {GAMES_MAX_TRANSACTION:,}."
    if direction not in ("deposit", "withdraw"):
        return False, "Invalid bank direction."
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                    (user_id,),
                )
                if direction == "deposit":
                    cur.execute(
                        """UPDATE mcwv_coins SET balance = balance - %s, bank = bank + %s
                           WHERE discord_id = %s AND balance >= %s
                           RETURNING balance, bank""",
                        (amount, amount, user_id, amount),
                    )
                else:
                    cur.execute(
                        """UPDATE mcwv_coins SET bank = bank - %s, balance = balance + %s
                           WHERE discord_id = %s AND bank >= %s
                           RETURNING balance, bank""",
                        (amount, amount, user_id, amount),
                    )
                row = cur.fetchone()
                if row is None:
                    raise ValueError("Not enough cash." if direction == "deposit" else "Not enough banked coins.")
                cash, bank = int(row[0]), int(row[1])
                cur.execute(
                    """INSERT INTO mcwv_coin_log
                       (actor_id, target_id, type, amount, balance_after, meta)
                       VALUES (%s,%s,%s,0,%s,%s::jsonb)""",
                    (user_id, user_id, direction, cash, json.dumps({"moved": amount, "cash_after": cash, "bank_after": bank})),
                )
        return True, {"balance": cash, "bank": bank}
    except ValueError as exc:
        return False, str(exc)
    except Exception as exc:
        print(f"[games] bank move failed: {exc}")
        return False, f"{type(exc).__name__}: {exc}"


# ---------------- COIN ENGINE (integer + atomic) ----------------

def games_coin_log_zero(target_id, kind, meta=None):
    """Log a zero-amount event (free usage tracking). ALWAYS logs so daily
    limits can't be bypassed by zero-amount paths."""
    try:
        if not db_enabled():
            return
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                (int(target_id),),
            )
            cur.execute(
                """INSERT INTO mcwv_coin_log (actor_id, target_id, type, amount, balance_after, meta)
                   VALUES (%s, %s, %s, 0, (SELECT balance FROM mcwv_coins WHERE discord_id = %s), %s::jsonb)""",
                (int(target_id), int(target_id), str(kind)[:40], int(target_id), json.dumps(meta or {})),
            )
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[games] coin_log_zero failed: {exc}")


def games_prepaid_get(user_id, kind):
    try:
        if not db_enabled():
            return 0
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                (int(user_id),),
            )
            col = {"hatch": "prepaid_hatches", "spin": "prepaid_spins", "scratch": "prepaid_scratches"}.get(kind)
            if not col:
                return 0
            cur.execute(f"SELECT {col} FROM mcwv_coins WHERE discord_id = %s", (int(user_id),))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0
    except Exception as exc:
        print(f"[games] prepaid get failed: {exc}")
        return 0


def games_prepaid_consume(user_id, kind):
    """Use one prepaid item if available. Returns True if consumed."""
    try:
        if not db_enabled():
            return False
        with conn.cursor() as cur:
            col = {"hatch": "prepaid_hatches", "spin": "prepaid_spins", "scratch": "prepaid_scratches"}.get(kind)
            if not col:
                return False
            cur.execute(
                f"UPDATE mcwv_coins SET {col} = {col} - 1 WHERE discord_id = %s AND {col} > 0 RETURNING {col}",
                (int(user_id),),
            )
            row = cur.fetchone()
        conn.commit()
        if row is None:
            return False
        games_coin_log_zero(user_id, f"prepaid_{kind}_used", meta={"kind": kind})
        return True
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[games] prepaid consume failed: {exc}")
        return False


def games_coin_adjust(target_id, amount, kind, actor_id=None, meta=None):
    """Atomic balance change. Returns (ok, new_balance_or_error)."""
    if not db_enabled():
        return False, "Database is not available."
    amount = int(amount)
    target_id = int(target_id)
    if amount == 0:
        return True, 0
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                    (target_id,),
                )
                cur.execute(
                    """UPDATE mcwv_coins
                       SET balance = balance + %s,
                           total_earned = total_earned + CASE WHEN %s > 0 THEN %s ELSE 0 END,
                           total_spent = total_spent + CASE WHEN %s < 0 THEN -%s ELSE 0 END
                       WHERE discord_id = %s AND balance + %s >= 0
                       RETURNING balance""",
                    (amount, amount, amount, amount, amount, target_id, amount),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError("Not enough coins.")
                new_balance = int(row[0])
                cur.execute(
                    """INSERT INTO mcwv_coin_log (actor_id, target_id, type, amount, balance_after, meta)
                       VALUES (%s, %s, %s, %s, %s, %s::jsonb)""",
                    (int(actor_id) if actor_id else target_id, target_id, str(kind)[:40], amount, new_balance,
                     json.dumps(meta or {})),
                )
        return True, new_balance
    except ValueError as exc:
        return False, str(exc)
    except Exception as exc:
        print(f"[games] coin_adjust error: {exc}")
        return False, f"{type(exc).__name__}: {exc}"


def games_coin_spend(user_id, amount, kind, actor_id=None, meta=None):
    """Spend coins; unlimited users skip the deduction but still log (type gets '_test' suffix)."""
    user_id = int(user_id)
    if games_is_unlimited(user_id):
        return games_coin_log_test(user_id, amount, kind, meta)
    return games_coin_adjust(user_id, -int(amount), kind, actor_id=actor_id, meta=meta)


def games_coin_log_test(user_id, amount, kind, meta=None):
    try:
        if not db_enabled():
            return False, "Database is not available."
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                (int(user_id),),
            )
            cur.execute(
                """INSERT INTO mcwv_coin_log (actor_id, target_id, type, amount, balance_after, meta)
                   VALUES (%s, %s, %s, 0, (SELECT balance FROM mcwv_coins WHERE discord_id = %s), %s::jsonb)""",
                (int(user_id), int(user_id), f"{kind}_test"[:40], int(user_id), json.dumps(meta or {"test": True})),
            )
        conn.commit()
        return True, 0
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"{type(exc).__name__}: {exc}"


def games_coin_balance(user_id):
    try:
        if not db_enabled():
            return {"balance": 0, "bank": 0}
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                (int(user_id),),
            )
            cur.execute("SELECT balance, bank FROM mcwv_coins WHERE discord_id = %s", (int(user_id),))
            row = cur.fetchone()
            return {"balance": int(row[0] or 0), "bank": int(row[1] or 0)}
    except Exception as exc:
        print(f"[games] balance fetch failed: {exc}")
        return {"balance": 0, "bank": 0}


# ---------------- ATOMIC FREE-USE LIMITS (race-safe) ----------------
# Each free limit is a 24h window starting at the user's first free use.
# Consumed with a SINGLE conditional UPDATE, so even rapid/parallel commands
# can never double-spend a free use (check-then-insert patterns could).

GAMES_FREE_COLUMNS = {
    "hatch": ("free_hatch_window", "free_hatch_used"),
    "spin": ("free_spin_window", "free_spin_used"),
    "scratch": ("free_scratch_window", "free_scratch_used"),
    "tower": ("tower_window", "tower_used"),
}


def games_free_use(user_id, kind, limit=None):
    """Consume one free use of `kind` atomically. Returns (is_free, used_now)."""
    cols = GAMES_FREE_COLUMNS.get(kind)
    if not cols:
        return False, 0
    win_col, used_col = cols
    if limit is None:
        limit = {"hatch": GAMES_HATCH_FREE_PER_DAY, "spin": 1, "scratch": 1,
                 "tower": GAMES_TOWER_RUNS_PER_DAY}.get(kind, 1)
    try:
        if not db_enabled():
            return False, 0
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                (int(user_id),),
            )
            cur.execute(
                f"""UPDATE mcwv_coins SET
                        {used_col} = CASE WHEN {win_col} IS NULL
                                          OR {win_col} < NOW() - INTERVAL '24 hours'
                                     THEN 1 ELSE {used_col} + 1 END,
                        {win_col} = CASE WHEN {win_col} IS NULL
                                          OR {win_col} < NOW() - INTERVAL '24 hours'
                                     THEN NOW() ELSE {win_col} END
                    WHERE discord_id = %s
                      AND ({win_col} IS NULL
                           OR {win_col} < NOW() - INTERVAL '24 hours'
                           OR {used_col} < %s)
                    RETURNING {used_col}""",
                (int(user_id), int(limit)),
            )
            row = cur.fetchone()
        conn.commit()
        if row is None:
            return False, int(limit)
        used = int(row[0] or 0)
        return True, used
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[games] free_use {kind} failed: {exc}")
        return False, 0


def games_free_status(user_id, kind, limit=None):
    """(used_now, limit, reset_ts) WITHOUT consuming — for displays."""
    cols = GAMES_FREE_COLUMNS.get(kind)
    if not cols:
        return 0, limit or 1, None
    win_col, used_col = cols
    if limit is None:
        limit = {"hatch": GAMES_HATCH_FREE_PER_DAY, "spin": 1, "scratch": 1,
                 "tower": GAMES_TOWER_RUNS_PER_DAY}.get(kind, 1)
    try:
        if not db_enabled():
            return 0, limit, None
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                (int(user_id),),
            )
            cur.execute(f"SELECT {used_col}, {win_col} FROM mcwv_coins WHERE discord_id = %s", (int(user_id),))
            row = cur.fetchone()
        conn.commit()
        if not row:
            return 0, int(limit), None
        win_start = row[1]
        now_dt = datetime.now(timezone.utc)
        if win_start is None or (now_dt - win_start).total_seconds() >= 86400:
            return 0, int(limit), None
        return min(int(row[0] or 0), int(limit)), int(limit), win_start + timedelta(hours=24)
    except Exception as exc:
        print(f"[games] free_status {kind} failed: {exc}")
        return 0, int(limit or 1), None


# ---------------- GAME RNG + NAMES ----------------

def games_roll_chance(pct):
    """True with pct% probability using crypto-grade RNG."""
    pct = max(0.0, min(100.0, float(pct)))
    return secrets.randbelow(1000000) < int(pct * 10000)


def games_weighted_choice(items, weights):
    if not items:
        return None
    # Integer weights avoid floating-point boundary bias and preserve tiny odds.
    scaled = [max(1, int(round(float(w) * 1_000_000))) if float(w) > 0 else 0 for w in weights]
    total = sum(scaled)
    if total <= 0:
        return items[0]
    roll = secrets.randbelow(total)
    acc = 0
    for item, weight in zip(items, scaled):
        acc += weight
        if roll < acc:
            return item
    return items[-1]


def normalize_answer(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def games_answers_match(raw_answer, pet_name):
    ans = normalize_answer(raw_answer)
    name = normalize_answer(pet_name)
    if not ans or not name:
        return False
    if ans == name:
        return True
    # allow dropping leading "the"
    if name.startswith("the") and ans == name[3:]:
        return True
    if ans.startswith("the") and name == ans[3:]:
        return True
    return False


def games_guess_short_name(pet_name):
    """Drop the rarity prefix for a natural alias (when that alias is unique)."""
    words = str(pet_name or "").strip().split()
    while words and words[0].lower() in {"huge", "titanic", "gargantuan"}:
        words.pop(0)
    return " ".join(words)


def games_edit_distance_at_most_one(a, b):
    """Fast one-edit check used only for long Guess-the-Pet answers."""
    a, b = str(a), str(b)
    if a == b:
        return True
    if abs(len(a) - len(b)) > 1:
        return False
    if len(a) > len(b):
        a, b = b, a
    i = j = edits = 0
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            i += 1
            j += 1
            continue
        edits += 1
        if edits > 1:
            return False
        if len(a) == len(b):
            i += 1
        j += 1
    return edits + (j < len(b)) <= 1


def games_guess_aliases(target, pool):
    aliases = {normalize_answer(target)}
    short = normalize_answer(games_guess_short_name(target))
    if len(short) >= 4:
        same = sum(1 for name in pool if normalize_answer(games_guess_short_name(name)) == short)
        if same == 1:
            aliases.add(short)
    return aliases


def games_guess_answer_result(raw_answer, target, aliases):
    """Return (correct, similarity); one typo is accepted only on long answers."""
    answer = normalize_answer(raw_answer)
    target_norm = normalize_answer(target)
    if not answer:
        return False, 0.0
    alias_set = set(aliases or ())
    correct = answer in alias_set or games_answers_match(answer, target)
    if not correct and len(answer) >= 7:
        typo_targets = {candidate for candidate in {target_norm, *alias_set} if len(candidate) >= 8}
        correct = any(games_edit_distance_at_most_one(answer, candidate) for candidate in typo_targets)
    similarity = difflib.SequenceMatcher(None, answer, target_norm).ratio()
    return correct, similarity


def games_guess_typo_index(valid_names):
    """Deletion index for recognising catalogue names with one conservative typo."""
    index = {}
    for name in valid_names:
        if len(name) < 8:
            continue
        for pos in range(len(name)):
            index.setdefault(name[:pos] + name[pos + 1:], set()).add(name)
    return index


def games_guess_is_catalogue_attempt(answer, valid_names, typo_index):
    """True for an exact catalogue name/alias or a long answer one edit from one."""
    if answer in valid_names:
        return True
    if not (7 <= len(answer) <= 80):
        return False
    candidates = set(typo_index.get(answer, ()))  # one missing character
    for pos in range(len(answer)):
        shortened = answer[:pos] + answer[pos + 1:]
        if shortened in valid_names:               # one extra character
            candidates.add(shortened)
        candidates.update(typo_index.get(shortened, ()))  # one substitution
    return any(games_edit_distance_at_most_one(answer, candidate) for candidate in candidates)


def games_safe_name(pet_name):
    """Display name for round messages."""
    return str(pet_name or "")[:60]


# ---------------- ACTIVE ROUNDS + DUELS + SESSIONS (memory) ----------------

ACTIVE_GUESS_ROUNDS = {}      # channel_id -> round dict
ACTIVE_GUESS_TASKS = {}       # channel_id -> exact hint/timeout clock
ACTIVE_GUESS_START_TASKS = {} # channel_id -> background round-build task
ACTIVE_GUESS_STARTING = set() # channels currently fetching/building a round
ACTIVE_GUESS_CANCEL_REQUESTS = set()
ACTIVE_DUELS = {}             # duel_id -> duel dict (persisted in mcwv_duels too)
ACTIVE_DUEL_TIMEOUT_TASKS = {}  # duel_id -> scheduled refund task
ACTIVE_TRIVIA = {}            # user_id -> trivia session
ACTIVE_HANGMAN = {}           # channel_id -> hangman session
ACTIVE_SCRAMBLE = {}          # channel_id -> scramble round
ACTIVE_TOWER = {}             # user_id -> tower session
ACTIVE_TOWER_TIMEOUT_TASKS = {}  # user_id -> exact per-floor deadline task
_spawn_last_channel = {}
_spawn_last_global = {"ts": 0.0}


def games_track(game, channel_id, sessions=1, minted=0, burned=0):
    try:
        if not db_enabled():
            return
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_game_stats (game, sessions, coins_minted, coins_burned, last_played)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (game) DO UPDATE SET
                    sessions = mcwv_game_stats.sessions + EXCLUDED.sessions,
                    coins_minted = mcwv_game_stats.coins_minted + EXCLUDED.coins_minted,
                    coins_burned = mcwv_game_stats.coins_burned + EXCLUDED.coins_burned,
                    last_played = NOW()
            """, (str(game)[:40], int(sessions), int(minted), int(burned)))
        conn.commit()
    except Exception as exc:
        print(f"[games] stat track failed: {exc}")


def init_games_tables():
    if not db_enabled():
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_coins (
                discord_id BIGINT PRIMARY KEY,
                balance BIGINT NOT NULL DEFAULT 0,
                bank BIGINT NOT NULL DEFAULT 0,
                is_unlimited BOOLEAN DEFAULT FALSE,
                last_daily_at TIMESTAMPTZ,
                daily_streak INTEGER DEFAULT 0,
                last_interest_at TIMESTAMPTZ,
                total_earned BIGINT DEFAULT 0,
                total_spent BIGINT DEFAULT 0,
                prepaid_hatches INTEGER DEFAULT 0,
                prepaid_spins INTEGER DEFAULT 0,
                prepaid_scratches INTEGER DEFAULT 0
            )""")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS prepaid_hatches INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS prepaid_spins INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS prepaid_scratches INTEGER DEFAULT 0")
            # atomic 24h free-use windows (race-safe daily limits)
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS free_hatch_window TIMESTAMPTZ")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS free_hatch_used INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS free_spin_window TIMESTAMPTZ")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS free_spin_used INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS free_scratch_window TIMESTAMPTZ")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS free_scratch_used INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS tower_window TIMESTAMPTZ")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS tower_used INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS last_petdle_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE mcwv_coins ADD COLUMN IF NOT EXISTS scratch_pity INTEGER NOT NULL DEFAULT 0")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_coin_log (
                id BIGSERIAL PRIMARY KEY,
                actor_id BIGINT, target_id BIGINT, type TEXT, amount BIGINT,
                balance_after BIGINT, meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            cur.execute("CREATE INDEX IF NOT EXISTS mcwv_coin_log_target_idx ON mcwv_coin_log (target_id, created_at DESC)")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_game_testers (
                discord_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_cases (
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                price BIGINT NOT NULL DEFAULT 100,
                emoji TEXT DEFAULT '\\U0001f381',
                enabled BOOLEAN DEFAULT TRUE,
                created_by BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_case_contents (
                id BIGSERIAL PRIMARY KEY,
                case_id BIGINT NOT NULL,
                role_id BIGINT NOT NULL,
                chance NUMERIC(5,2) NOT NULL,
                UNIQUE (case_id, role_id)
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_case_rolls (
                id BIGSERIAL PRIMARY KEY,
                case_id BIGINT, user_id BIGINT, won_role_id BIGINT,
                price_paid BIGINT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_pet_collections (
                discord_id BIGINT NOT NULL,
                pet_key TEXT NOT NULL,
                count INTEGER DEFAULT 0,
                first_hatched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (discord_id, pet_key)
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_duels (
                id BIGSERIAL PRIMARY KEY,
                challenger BIGINT, target BIGINT, wager BIGINT,
                game_type TEXT, state TEXT, winner BIGINT,
                challenger_paid BIGINT NOT NULL DEFAULT 0,
                target_paid BIGINT NOT NULL DEFAULT 0,
                escrow_version SMALLINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            cur.execute("ALTER TABLE mcwv_duels ADD COLUMN IF NOT EXISTS challenger_paid BIGINT NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE mcwv_duels ADD COLUMN IF NOT EXISTS target_paid BIGINT NOT NULL DEFAULT 0")
            cur.execute("ALTER TABLE mcwv_duels ADD COLUMN IF NOT EXISTS escrow_version SMALLINT NOT NULL DEFAULT 0")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_game_stats (
                game TEXT PRIMARY KEY,
                sessions BIGINT DEFAULT 0,
                coins_minted BIGINT DEFAULT 0,
                coins_burned BIGINT DEFAULT 0,
                last_played TIMESTAMPTZ
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_bingo_cards (
                id BIGSERIAL PRIMARY KEY,
                discord_id BIGINT, battle_id TEXT,
                card JSONB NOT NULL DEFAULT '[]'::jsonb,
                marked JSONB NOT NULL DEFAULT '[]'::jsonb,
                bingo_paid JSONB NOT NULL DEFAULT '[]'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (discord_id, battle_id)
            )""")
            # This ALTER must come after CREATE on a clean database.
            cur.execute("ALTER TABLE mcwv_bingo_cards ADD COLUMN IF NOT EXISTS bingo_paid JSONB NOT NULL DEFAULT '[]'::jsonb")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_game_eggs (
                slug TEXT PRIMARY KEY,
                name TEXT,
                icon_asset TEXT,
                rarity TEXT,
                rap BIGINT DEFAULT 0,
                contents JSONB DEFAULT '[]'::jsonb,
                synced_at TIMESTAMPTZ
            )""")
            cur.execute("ALTER TABLE mcwv_game_eggs ADD COLUMN IF NOT EXISTS contents JSONB DEFAULT '[]'::jsonb")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_game_pets (
                slug TEXT PRIMARY KEY,
                name TEXT,
                category TEXT,
                asset TEXT,
                exist_count BIGINT DEFAULT 0,
                synced_at TIMESTAMPTZ
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_user_stats (
                discord_id BIGINT,
                game TEXT,
                wins INTEGER DEFAULT 0,
                plays INTEGER DEFAULT 0,
                PRIMARY KEY (discord_id, game)
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_guess_profiles (
                discord_id BIGINT PRIMARY KEY,
                wins INTEGER NOT NULL DEFAULT 0,
                rounds INTEGER NOT NULL DEFAULT 0,
                current_streak INTEGER NOT NULL DEFAULT 0,
                best_streak INTEGER NOT NULL DEFAULT 0,
                fastest_ms INTEGER,
                total_reward BIGINT NOT NULL DEFAULT 0,
                valid_guesses BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            # Preserve pre-upgrade Guess history on the first deploy.
            cur.execute("""
                INSERT INTO mcwv_guess_profiles (discord_id, wins, rounds, total_reward)
                SELECT s.discord_id, s.wins, s.plays, COALESCE(l.reward, 0)
                FROM mcwv_user_stats s
                LEFT JOIN (
                    SELECT target_id, SUM(amount) AS reward FROM mcwv_coin_log
                    WHERE type = 'guess_win' GROUP BY target_id
                ) l ON l.target_id = s.discord_id
                WHERE s.game = 'guess'
                ON CONFLICT (discord_id) DO NOTHING
            """)
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_lottery_tickets (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                week TEXT,
                tickets INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, week)
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_tower_scores (
                discord_id BIGINT PRIMARY KEY,
                best_floor INTEGER DEFAULT 0,
                best_score BIGINT DEFAULT 0,
                runs INTEGER DEFAULT 0,
                reached_at TIMESTAMPTZ
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_game_cooldowns (
                subject_id BIGINT NOT NULL,
                game TEXT NOT NULL,
                last_played_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (subject_id, game)
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_petdle_progress (
                discord_id BIGINT NOT NULL,
                day DATE NOT NULL,
                guesses JSONB NOT NULL DEFAULT '[]'::jsonb,
                solved BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (discord_id, day)
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS mcwv_lottery_draws (
                round_key TEXT PRIMARY KEY,
                pool BIGINT NOT NULL,
                winner_id BIGINT,
                prize BIGINT NOT NULL DEFAULT 0,
                drawn_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
        conn.commit()
        recovered = games_recover_stale_duels()
        print("🎮 Games tables ready" + (f" · refunded {recovered} interrupted duel(s)" if recovered else ""))
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"🎮 Games table init failed: {exc}")



# ---------------- ECONOMY COMMANDS ----------------

@bot.tree.command(name="coins", description="Check your (or someone's) coin balance", guild=guild_obj)
@app_commands.describe(user="Whose balance to check")
async def games_coins(interaction: discord.Interaction, user: discord.User = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    target = user or interaction.user
    embed = games_wallet_embed(target)
    view = CoinsQuickView(interaction.user.id)
    view.message = await interaction.followup.send(embed=embed, view=view, ephemeral=True, wait=True)


@bot.tree.command(name="daily", description="Daily check-in — 100 coins + 10 per streak day", guild=guild_obj)
async def games_daily(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    uid = interaction.user.id
    now = datetime.now(timezone.utc)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING", (uid,))
            # ATOMIC claim: single guarded UPDATE — concurrent/rapid claims can
            # never both pass the 24h guard (old check-then-update could).
            cur.execute(
                """
                UPDATE mcwv_coins
                SET daily_streak = CASE
                        WHEN last_daily_at IS NULL OR last_daily_at < NOW() - INTERVAL '48 hours'
                        THEN 1 ELSE daily_streak + 1 END,
                    last_daily_at = NOW(),
                    balance = balance + %(base)s + %(bonus)s * (
                        CASE WHEN last_daily_at IS NULL OR last_daily_at < NOW() - INTERVAL '48 hours'
                        THEN 0 ELSE LEAST(daily_streak, %(cap_index)s) END),
                    total_earned = total_earned + %(base)s + %(bonus)s * (
                        CASE WHEN last_daily_at IS NULL OR last_daily_at < NOW() - INTERVAL '48 hours'
                        THEN 0 ELSE LEAST(daily_streak, %(cap_index)s) END)
                WHERE discord_id = %(uid)s
                  AND (last_daily_at IS NULL OR last_daily_at < NOW() - INTERVAL '24 hours')
                RETURNING daily_streak, balance
                """,
                {"uid": uid, "base": GAMES_DAILY_BASE, "bonus": GAMES_DAILY_STREAK_BONUS,
                 "cap_index": GAMES_DAILY_STREAK_REWARD_CAP - 1},
            )
            row = cur.fetchone()
        conn.commit()
        if row is None:
            with conn.cursor() as cur:
                cur.execute("SELECT last_daily_at FROM mcwv_coins WHERE discord_id = %s", (uid,))
                last_row = cur.fetchone()
            conn.commit()
            if last_row and last_row[0]:
                nxt = last_row[0] + timedelta(hours=24)
                return await interaction.followup.send(
                    f"⏳ Already claimed! Next check-in {discord.utils.format_dt(nxt, 'R')}.", ephemeral=True)
            return await interaction.followup.send("⏳ Already claimed today!", ephemeral=True)
        streak = int(row[0] or 1)
        award = GAMES_DAILY_BASE + GAMES_DAILY_STREAK_BONUS * min(streak - 1, GAMES_DAILY_STREAK_REWARD_CAP - 1)
        new_balance = int(row[1] or 0)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO mcwv_coin_log (actor_id, target_id, type, amount, balance_after, meta) "
                    "VALUES (%s,%s,'daily',%s,%s,%s::jsonb)",
                    (uid, uid, award, new_balance, json.dumps({"streak": streak})),
                )
            conn.commit()
        except Exception as log_exc:
            print(f"[games] daily log failed: {log_exc}")
        games_track("daily", interaction.channel_id, minted=award)
        flames = "\U0001f525" * min(streak, 10)
        bar_filled = games_bar(min(streak, 10), 10)
        milestone = {7: " 🎖 1 week!", 14: " 🏅 2 weeks!", 30: " 👑 30 days!", 100: " 💎 100 days!"}.get(streak, "")
        embed = discord.Embed(
            title="\U0001f4c5 Daily Check-in",
            description=f"+{games_money(award)} added to your balance",
            color=games_color("green"),
        )
        embed.add_field(
            name=f"Streak \u00b7 {streak} day{'s' if streak != 1 else ''}{milestone}",
            value=f"{flames}\n`{bar_filled}`",
            inline=True,
        )
        embed.add_field(name="Next check-in", value=discord.utils.format_dt(now + timedelta(hours=24), "R"), inline=True)
        embed.add_field(
            name="Next reward",
            value=games_money(
                GAMES_DAILY_BASE + GAMES_DAILY_STREAK_BONUS *
                min(streak, GAMES_DAILY_STREAK_REWARD_CAP - 1)
            ),
            inline=True,
        )
        games_footer(embed, f"Streak resets after 48h · reward caps at day {GAMES_DAILY_STREAK_REWARD_CAP}")
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[games] daily failed: {exc}")
        await interaction.followup.send("❌ Something went wrong claiming your daily.", ephemeral=True)


@bot.tree.command(name="pay", description="Send coins to another member", guild=guild_obj)
@app_commands.describe(user="Who to pay", amount="Amount (min 10)")
async def games_pay(interaction: discord.Interaction, user: discord.User, amount: int):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    amount = int(amount)
    if amount < GAMES_PAY_MIN:
        return await interaction.followup.send(f"❌ Minimum transfer is **{GAMES_PAY_MIN}** coins.", ephemeral=True)
    if amount > GAMES_MAX_TRANSACTION:
        return await interaction.followup.send(f"❌ Maximum transfer is **{GAMES_MAX_TRANSACTION:,}** coins.", ephemeral=True)
    if user.id == interaction.user.id:
        return await interaction.followup.send("❌ You can't pay yourself!", ephemeral=True)
    if getattr(user, "bot", False):
        return await interaction.followup.send("❌ Bots can't receive game coins.", ephemeral=True)
    ok, res = games_coin_transfer(interaction.user.id, user.id, amount)
    if not ok:
        return await interaction.followup.send(f"❌ {res}", ephemeral=True)
    embed = discord.Embed(title="💸 Payment Sent", description=f"{games_money(amount)} → {getattr(user, 'mention', '<@%s>' % user.id)}", color=games_color("green"))
    games_footer(embed, "/coins to check your balance")
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="deposit", description="Move cash into your bank (earns daily interest)", guild=guild_obj)
@app_commands.describe(amount="Amount to deposit, or 'all'")
async def games_deposit(interaction: discord.Interaction, amount: str):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    uid = interaction.user.id
    bal = games_coin_balance(uid)
    try:
        amt = bal["balance"] if str(amount).strip().lower() == "all" else int(amount)
    except (ValueError, TypeError):
        return await interaction.followup.send("❌ Amount must be a whole number or `all`.", ephemeral=True)
    if amt <= 0 or amt > GAMES_MAX_TRANSACTION:
        return await interaction.followup.send(f"❌ Amount must be between 1 and {GAMES_MAX_TRANSACTION:,}.", ephemeral=True)
    ok, result = games_bank_move(uid, amt, "deposit")
    if not ok:
        return await interaction.followup.send(f"❌ {result}", ephemeral=True)
    embed = discord.Embed(title="🏦 Deposit", description=f"{games_money(amt)} moved into your bank", color=games_color("green"))
    embed.add_field(name="New bank balance", value=f"**{result['bank']:,}** 🪙", inline=True)
    embed.add_field(name="Interest", value="+1%/day (capped at 100k)", inline=True)
    games_footer(embed, "/withdraw to take it back out")
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="withdraw", description="Move banked coins back to cash", guild=guild_obj)
@app_commands.describe(amount="Amount to withdraw, or 'all'")
async def games_withdraw(interaction: discord.Interaction, amount: str):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    uid = interaction.user.id
    bal = games_coin_balance(uid)
    try:
        amt = bal["bank"] if str(amount).strip().lower() == "all" else int(amount)
    except (ValueError, TypeError):
        return await interaction.followup.send("❌ Amount must be a whole number or `all`.", ephemeral=True)
    if amt <= 0 or amt > GAMES_MAX_TRANSACTION:
        return await interaction.followup.send(f"❌ Amount must be between 1 and {GAMES_MAX_TRANSACTION:,}.", ephemeral=True)
    ok, result = games_bank_move(uid, amt, "withdraw")
    if not ok:
        return await interaction.followup.send(f"❌ {result}", ephemeral=True)
    embed = discord.Embed(title="💵 Withdrawal", description=f"{games_money(amt)} moved to cash", color=games_color("green"))
    embed.add_field(name="New cash balance", value=f"**{result['balance']:,}** 🪙", inline=True)
    games_footer(embed, "Banked coins earn 1%/day interest")
    await interaction.followup.send(embed=embed, ephemeral=True)


class ShopCaseButton(discord.ui.Button):
    """One-click bridge from the shop catalogue to the normal case confirmation."""

    def __init__(self, case, index):
        self.case_data = case
        label = f"Open {case['name']} · {case['price']:,}"
        super().__init__(
            label=label[:80],
            style=discord.ButtonStyle.success,
            emoji=case["emoji"] if games_emoji_ok(case.get("emoji")) else "🎁",
            row=int(index) // 5,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, ShopView) or view._owner is None:
            return await interaction.response.send_message("This shop is closed — run `/shop`.", ephemeral=True)
        if interaction.user.id != view._owner:
            return await interaction.response.send_message("This shop isn't yours — run `/shop`.", ephemeral=True)
        if view._busy:
            return await interaction.response.send_message("Processing your last purchase…", ephemeral=True)
        await games_case_open.callback(interaction, self.case_data["name"])


class ShopPageButton(discord.ui.Button):
    def __init__(self, direction, disabled=False):
        self.direction = -1 if int(direction) < 0 else 1
        super().__init__(
            label="Previous Cases" if self.direction < 0 else "More Cases",
            style=discord.ButtonStyle.secondary,
            emoji="⬅️" if self.direction < 0 else "➡️",
            row=2,
            disabled=bool(disabled),
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, ShopView):
            return await interaction.response.send_message("This shop is closed — run `/shop`.", ephemeral=True)
        if not await view._guard(interaction):
            return
        view._page = max(0, min(view._page + self.direction, view._page_count - 1))
        view._build_case_page()
        view.refresh_buttons(interaction.user.id)
        await interaction.edit_original_response(
            embed=games_shop_embed(interaction.user.id, view._page), view=view
        )


class ShopView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=600)
        self._owner = None
        self._busy = False
        self.message = None
        self._all_cases = games_case_catalog(enabled_only=True, limit=100)
        self._page_count = max(1, math.ceil(len(self._all_cases) / GAMES_SHOP_CASE_LIMIT))
        self._page = 0
        self._build_case_page()

    def _build_case_page(self):
        for child in list(self.children):
            if isinstance(child, (ShopCaseButton, ShopPageButton)):
                self.remove_item(child)
        self._page = max(0, min(self._page, self._page_count - 1))
        start = self._page * GAMES_SHOP_CASE_LIMIT
        cases = self._all_cases[start:start + GAMES_SHOP_CASE_LIMIT]
        for index, case in enumerate(cases):
            self.add_item(ShopCaseButton(case, index))
        if self._page_count > 1:
            self.add_item(ShopPageButton(-1, disabled=self._page <= 0))
            self.add_item(ShopPageButton(1, disabled=self._page >= self._page_count - 1))

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    def _set_owner(self, user_id):
        self._owner = int(user_id)
        self.refresh_buttons(user_id)

    def refresh_buttons(self, user_id):
        d = games_shop_items(user_id)
        unlimited = d["unlimited"]
        balance = d["balance"]
        labels = {
            "hatch": f"🥚 Hatch · {GAMES_HATCH_COST} 🪙 ×{d['owned']['hatch']}",
            "spin": f"🎡 Spin · {GAMES_SPIN_COST_EXTRA} 🪙 ×{d['owned']['spin']}",
            "scratch": f"🎴 Scratch · 100 🪙 ×{d['owned']['scratch']}",
        }
        for child, (kind, _label, price, _desc) in zip(self.children[:3], GAMES_SHOP_ITEMS):
            child.label = labels[kind]
            child.disabled = self._busy or (not unlimited and balance < price)
        lotto = self.children[3]
        lotto.label = f"🎫 Lottery · {GAMES_LOTTERY_TICKET_COST} credits ×{d['lottery_owned']}"
        lotto.disabled = self._busy or (not unlimited and (balance < GAMES_LOTTERY_TICKET_COST
                                                          or d['lottery_owned'] >= GAMES_LOTTERY_WEEKLY_TICKET_CAP))
        for child in self.children[4:]:
            if isinstance(child, ShopCaseButton):
                case = child.case_data
                child.label = f"Open {case['name']} · {case['price']:,}"[:80]
                child.disabled = self._busy or not case.get("contents") or (
                    not unlimited and balance < int(case["price"])
                )
            elif isinstance(child, ShopPageButton):
                at_edge = self._page <= 0 if child.direction < 0 else self._page >= self._page_count - 1
                child.disabled = self._busy or at_edge

    async def _guard(self, interaction):
        if self._owner is None:
            await interaction.response.send_message("This shop is closed — run `/shop`.", ephemeral=True)
            return False
        if interaction.user.id != self._owner:
            await interaction.response.send_message("This shop isn't yours — run `/shop`.", ephemeral=True)
            return False
        if self._busy:
            await interaction.response.send_message("Processing your last purchase…", ephemeral=True)
            return False
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass
        return True

    async def _refresh_ui(self, interaction, user_id, confirm=None):
        # rebuild the shop card + button states on the ORIGINAL message
        try:
            await interaction.edit_original_response(embed=games_shop_embed(user_id, self._page), view=self)
        except Exception as exc:
            print(f"[games] shop refresh failed: {exc}")
        if confirm:
            await interaction.followup.send(confirm, ephemeral=True)

    async def _buy(self, interaction, prepaid_kind):
        if not await self._guard(interaction):
            return
        item_map = {
            "hatch": ("Extra Hatch", GAMES_HATCH_COST),
            "spin": ("Extra Spin", GAMES_SPIN_COST_EXTRA),
            "scratch": ("Extra Scratch", 100),
        }
        item, price = item_map[prepaid_kind]
        self._busy = True
        self.refresh_buttons(interaction.user.id)
        try:
            await interaction.edit_original_response(view=self)
        except Exception:
            pass
        ok, res = games_coin_spend(interaction.user.id, price, f"shop_{prepaid_kind}", meta={"item": item})
        if not ok:
            self._busy = False
            self.refresh_buttons(interaction.user.id)
            await self._refresh_ui(interaction, interaction.user.id)
            return await interaction.followup.send(f"❌ {res}", ephemeral=True)
        try:
            with conn.cursor() as cur:
                col = {"hatch": "prepaid_hatches", "spin": "prepaid_spins", "scratch": "prepaid_scratches"}[prepaid_kind]
                cur.execute(
                    f"UPDATE mcwv_coins SET {col} = {col} + 1 WHERE discord_id = %s RETURNING {col}",
                    (interaction.user.id,),
                )
                new_count = int(cur.fetchone()[0])
            conn.commit()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            games_coin_adjust(interaction.user.id, price, "shop_refund")
            self._busy = False
            self.refresh_buttons(interaction.user.id)
            await self._refresh_ui(interaction, interaction.user.id)
            print(f"[games] shop purchase failed: {exc}")
            return await interaction.followup.send("❌ Purchase failed — coins returned.", ephemeral=True)
        self._busy = False
        self.refresh_buttons(interaction.user.id)
        bal = games_coin_balance(interaction.user.id)
        await self._refresh_ui(
            interaction, interaction.user.id,
            confirm=(
                f"✅ Bought **{item}** for **{price:,}** 🪙 — banked ×{new_count} "
                f"(cash now **{bal['balance']:,}** 🪙).\n"
                f"Used automatically once your free dailies run out."
            ),
        )

    @discord.ui.button(label="🥚 Hatch", style=discord.ButtonStyle.primary, row=3)
    async def buy_hatch(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._buy(interaction, "hatch")

    @discord.ui.button(label="🎡 Spin", style=discord.ButtonStyle.primary, row=3)
    async def buy_spin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._buy(interaction, "spin")

    @discord.ui.button(label="🎴 Scratch", style=discord.ButtonStyle.primary, row=3)
    async def buy_scratch(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._buy(interaction, "scratch")

    @discord.ui.button(label="🎫 Lottery", style=discord.ButtonStyle.success, row=4)
    async def buy_lottery(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        self._busy = True
        self.refresh_buttons(interaction.user.id)
        try:
            await interaction.edit_original_response(view=self)
        except Exception:
            pass
        ok, result = games_lottery_purchase(interaction.user.id, 1)
        if not ok:
            self._busy = False
            self.refresh_buttons(interaction.user.id)
            await self._refresh_ui(interaction, interaction.user.id)
            return await interaction.followup.send(f"❌ {result}", ephemeral=True)
        self._busy = False
        self.refresh_buttons(interaction.user.id)
        await self._refresh_ui(
            interaction, interaction.user.id,
            confirm=(f"🎟 Ticket bought — you hold **{result['owned']}** for the {result['round']} draw. "
                     f"Pool: **{result['pool']:,}** 🪙."),
        )


@bot.tree.command(name="shop", description="Buy game items with coins", guild=guild_obj)
async def games_shop(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    view = ShopView()
    view._set_owner(interaction.user.id)
    view.message = await interaction.followup.send(
        embed=games_shop_embed(interaction.user.id), view=view, ephemeral=True, wait=True
    )


# ---------------- ROLE CASES ----------------

@bot.tree.command(name="cases", description="Browse available role cases", guild=guild_obj)
async def games_cases(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mcwv_cases WHERE enabled = TRUE")
            total_cases = int(cur.fetchone()[0] or 0)
            cur.execute(
                "SELECT id, name, price, emoji FROM mcwv_cases WHERE enabled = TRUE ORDER BY price ASC LIMIT 25"
            )
            rows = cur.fetchall()
        if not rows:
            return await interaction.followup.send("No cases yet — staff will add some soon!", ephemeral=True)
        embed = discord.Embed(title="🎁 Role Cases", color=games_color("violet"),
                              description="**Loot boxes with real Discord role prizes** — contents and odds are always shown before you roll.")
        for cid, name, price, emoji in rows:
            embed.add_field(
                name=f"{emoji} {name}",
                value=f"`{price:,}` credits per roll · open with `/case {name}`",
                inline=False,
            )
        footer = f"{total_cases} case{'s' if total_cases != 1 else ''} available · the price is consumed on every roll"
        if total_cases > len(rows):
            footer += " · use /shop to browse every page"
        embed.set_footer(text=footer)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"❌ Could not list cases: `{type(exc).__name__}`", ephemeral=True)


class CaseConfirmView(discord.ui.View):
    """Shows a case's contents + odds, then rolls on confirm."""

    def __init__(self, case_id, case_name, price, emoji, roles, weights, owner_id):
        super().__init__(timeout=120)
        self.case_id = int(case_id)
        self.case_name = case_name
        self.price = int(price)
        self.owner_id = int(owner_id)
        self.emoji = emoji
        self.roles = roles          # list of discord.Role or None (nothing)
        self.weights = weights      # float % list, aligned
        self.rolled = False
        self.message = None
        for child in self.children:
            child.label = f"Open · {self.price:,} 🪙"

    async def on_timeout(self):
        if self.rolled:
            return
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    @discord.ui.button(label="Open", style=discord.ButtonStyle.success, emoji="🎁")
    async def open_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.owner_id:
            return await interaction.response.send_message("This case isn't yours — run `/case`.", ephemeral=True)
        if self.rolled:
            return await interaction.response.send_message("This case was already rolled.", ephemeral=True)
        self.rolled = True
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        roll_msg = await interaction.followup.send("🎲 **Rolling…**", ephemeral=True)
        # hype frames: cycle through random possible prizes
        cycle = [r for r in self.roles if r is not None]
        frames = []
        for _ in range(4):
            pick = secrets.choice(cycle) if cycle else None
            frames.append(f"🎁 **{pick.mention if pick else '…'}**…")
        try:
            await games_animate(roll_msg, frames, delay=0.45)
        except Exception:
            pass
        # Never honour a stale confirmation after staff disabled/repriced it.
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT price, enabled FROM mcwv_cases WHERE id = %s", (self.case_id,))
                live_case = cur.fetchone()
            if not live_case or not live_case[1] or int(live_case[0]) != self.price:
                self.rolled = False
                return await roll_msg.edit(content="❌ This case changed or was disabled — run `/case` again.")
        except Exception:
            self.rolled = False
            return await roll_msg.edit(content="❌ Couldn't verify this case — you were not charged.")
        ok, res = games_coin_spend(interaction.user.id, self.price, "case_open", meta={"case": self.case_name})
        if not ok:
            self.rolled = False
            return await interaction.followup.send(f"❌ {res}", ephemeral=True)
        won = games_weighted_choice(self.roles, self.weights)
        if won is None:
            try:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO mcwv_case_rolls (case_id, user_id, price_paid) VALUES (%s,%s,%s)",
                                (self.case_id, interaction.user.id, self.price))
                conn.commit()
            except Exception:
                pass
            games_track("case", interaction.channel_id, burned=self.price)
            games_track_user("case", interaction.user.id, win=False)
            reveal = discord.Embed(
                title=f"{self.emoji} {self.case_name}",
                description="The case rattles… and **nothing** falls out. 😔",
                color=games_color("slate"),
            )
            reveal.add_field(name="Price paid", value=f"`{self.price:,}` 🪙", inline=True)
            reveal.set_footer(text="Better luck next roll!")
            await roll_msg.edit(content=None, embed=reveal)
            return

        duplicate_credit = 0
        role_granted = False
        try:
            member = interaction.user
            if isinstance(member, discord.Member) and won in member.roles:
                # Duplicate protection keeps a paid roll from feeling completely dead.
                duplicate_credit = max(1, self.price // 2)
                games_coin_adjust(member.id, duplicate_credit, "case_duplicate", meta={"case": self.case_name, "role": won.id})
            elif isinstance(member, discord.Member):
                await member.add_roles(won, reason=f"Unboxed from case '{self.case_name}'")
                role_granted = True
            with conn.cursor() as cur:
                cur.execute("INSERT INTO mcwv_case_rolls (case_id, user_id, won_role_id, price_paid) VALUES (%s,%s,%s,%s)",
                            (self.case_id, interaction.user.id, won.id, self.price))
            conn.commit()
        except Exception as exc:
            print(f"[games] case role grant failed: {exc}")
            if not duplicate_credit and not role_granted:
                # A permission/API failure should not eat the player's roll.
                games_coin_adjust(interaction.user.id, self.price, "case_grant_refund", meta={"case": self.case_name})
                return await roll_msg.edit(content="❌ I couldn't grant that role — your coins were refunded.", embed=None)
            # If Discord granted the role but only the audit insert failed, never
            # refund as well: that would turn a transient DB error into a free role.
            print(f"[games] case reward granted but roll audit failed: case={self.case_id} user={interaction.user.id}")
        games_track("case", interaction.channel_id, burned=max(0, self.price - duplicate_credit))
        games_track_user("case", interaction.user.id, win=role_granted)
        chance = float(self.weights[self.roles.index(won)])
        is_rare = chance <= 5
        reveal = discord.Embed(
            title=f"{self.emoji} {self.case_name} — {'✨ RARE DROP!' if is_rare else '🎉 Unboxed!'}",
            description=f"{interaction.user.mention} unboxed **{won.mention}**!",
            color=games_color("amber") if is_rare else games_color("violet"),
        )
        reveal.add_field(name="Roll odds", value=f"**{chance:g}%**", inline=True)
        reveal.add_field(name="Price paid", value=f"`{self.price:,}` 🪙", inline=True)
        reveal.add_field(
            name="Duplicate protection" if duplicate_credit else "New role",
            value=f"Already owned · **+{duplicate_credit:,}** 🪙 back" if duplicate_credit else won.mention,
            inline=True,
        )
        reveal.set_footer(text=f"Case: {self.case_name} · /case {self.case_name} to roll again")
        await roll_msg.edit(content=None, embed=reveal)


async def games_case_autocomplete(interaction: discord.Interaction, current: str):
    cur = str(current or "").strip().lower()
    cases = games_case_catalog(enabled_only=True, limit=25)
    return [
        app_commands.Choice(name=f"{case['emoji']} {case['name']} — {case['price']:,} credits"[:100], value=case["name"])
        for case in cases if not cur or cur in case["name"].lower()
    ][:25]


@bot.tree.command(name="case", description="Open a role case (contents + chances shown)", guild=guild_obj)
@app_commands.describe(name="Case name")
@app_commands.autocomplete(name=games_case_autocomplete)
async def games_case_open(interaction: discord.Interaction, name: str):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, price, emoji FROM mcwv_cases WHERE enabled = TRUE AND LOWER(name) = LOWER(%s)", (name.strip(),))
            case = cur.fetchone()
            if not case:
                return await interaction.followup.send("❌ Case not found.", ephemeral=True)
            cid, cname, price, emoji = case
            cur.execute("SELECT role_id, chance FROM mcwv_case_contents WHERE case_id = %s ORDER BY chance DESC", (cid,))
            contents = cur.fetchall()
        if not contents:
            return await interaction.followup.send("❌ That case has no contents yet.", ephemeral=True)

        guild = interaction.guild
        roles = []
        weights = []
        lines = []
        total = 0.0
        for role_id, chance in contents:
            role = guild.get_role(int(role_id))
            if role is None or role.managed:
                continue
            if role.permissions.administrator or role.permissions.manage_guild or role.permissions.manage_roles:
                continue
            roles.append(role)
            weights.append(float(chance))
            total += float(chance)
            rarity = "\u2728" if float(chance) <= 5 else ("\U0001f49c" if float(chance) <= 15 else "\U0001f4e6")
            lines.append(f"{rarity} {role.mention} \u2014 **{float(chance):g}%** `{games_bar(float(chance), 100, 8)}`")
        if not roles:
            return await interaction.followup.send("❌ Case contents are invalid (roles deleted?).", ephemeral=True)
        if total > 100.0001:
            return await interaction.followup.send(
                f"❌ This case's configured odds total **{total:g}%**. Staff must reduce them to 100% or less.",
                ephemeral=True,
            )
        filler_weight = max(0.0, 100.0 - total)
        if filler_weight > 0:
            roles.append(None)
            weights.append(filler_weight)
            lines.append(f"\u2b1c Nothing \u2014 **{filler_weight:g}%** `{games_bar(filler_weight, 100, 8)}`")

        bal = games_coin_balance(interaction.user.id)
        can_afford = games_is_unlimited(interaction.user.id) or bal["balance"] >= int(price)
        embed = discord.Embed(
            title=f"{emoji} {cname}",
            description="\n".join(lines) or "No contents.",
            color=games_color("violet"),
        )
        embed.add_field(name="Price", value=f"**{int(price):,}** 🪙" + (" \u00b7 \u221e unlimited" if games_is_unlimited(interaction.user.id) else ""), inline=True)
        embed.add_field(name="Your cash", value=f"**{bal['balance']:,}**" + ("" if can_afford else " \u2014 not enough!"), inline=True)
        if getattr(guild, "icon", None):
            embed.set_thumbnail(url=guild.icon.url)
        embed.set_footer(text="The price is fully consumed on every roll.")
        view = CaseConfirmView(cid, cname, int(price), emoji, roles, weights, interaction.user.id)
        if not can_afford:
            for child in view.children:
                child.disabled = True
        view.message = await interaction.followup.send(
            embed=embed, view=view, ephemeral=True, wait=True
        )
    except Exception as exc:
        print(f"[games] case open failed: {exc}")
        await interaction.followup.send(f"❌ Case open failed: `{type(exc).__name__}`", ephemeral=True)


# ---------------- OWNER / STAFF ADMIN ----------------

@bot.tree.command(name="coinsadmin", description="Owner coin admin (add/remove/set/ledger/audit)", guild=guild_obj)
@app_commands.describe(action="add / remove / set / ledger / audit", user="Target user", amount="Amount")
@app_commands.choices(action=[
    app_commands.Choice(name="add", value="add"),
    app_commands.Choice(name="remove", value="remove"),
    app_commands.Choice(name="set", value="set"),
    app_commands.Choice(name="ledger", value="ledger"),
    app_commands.Choice(name="audit", value="audit"),
])
async def coins_admin(interaction: discord.Interaction, action: str, user: discord.User = None, amount: int = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    if not games_owner_check(interaction.user):
        return await interaction.followup.send("❌ Owner only.", ephemeral=True)

    if action == "ledger":
        try:
            with conn.cursor() as cur:
                cur.execute("""SELECT id, actor_id, target_id, type, amount, balance_after, created_at
                               FROM mcwv_coin_log ORDER BY id DESC LIMIT 15""")
                rows = cur.fetchall()
            lines = [f"`#{r[0]}` <@{r[2]}> **{r[3]}** {r[4]:+,} (bal {r[5]:,})" for r in rows] or ["Ledger empty."]
            embed = discord.Embed(title="🧾 Recent Coin Ledger", description="\n".join(lines), color=discord.Color.greyple())
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ Ledger failed: `{type(exc).__name__}`", ephemeral=True)
        return

    if action == "audit":
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(SUM(balance + bank), 0) FROM mcwv_coins")
                total_bal = int(cur.fetchone()[0])
                cur.execute("SELECT COALESCE(SUM(amount), 0) FROM mcwv_coin_log")
                total_flow = int(cur.fetchone()[0])
            difference = total_bal - total_flow
            await interaction.followup.send(
                f"📊 **Audit**\nTotal cash + bank: **{total_bal:,}**\nNet ledger flow: **{total_flow:,}**\n"
                f"Difference: **{difference:+,}**\n"
                f"{'✅ Balanced' if difference == 0 else '⚠️ Difference found — inspect legacy/manual entries.'}",
                ephemeral=True,
            )
        except Exception as exc:
            await interaction.followup.send(f"❌ Audit failed: `{type(exc).__name__}`", ephemeral=True)
        return

    if user is None or amount is None:
        return await interaction.followup.send("Usage: `/coinsadmin <action> @user <amount>`.", ephemeral=True)
    amount = int(amount)
    if amount < 0 or amount > GAMES_MAX_TRANSACTION:
        return await interaction.followup.send(
            f"❌ Amount must be between 0 and {GAMES_MAX_TRANSACTION:,}.", ephemeral=True)
    if action == "add":
        ok, res = games_coin_adjust(user.id, int(amount), "admin_add", actor_id=interaction.user.id)
        return await interaction.followup.send(f"✅ +{amount:,} to {user.mention}." if ok else f"❌ {res}", ephemeral=True)
    if action == "remove":
        ok, res = games_coin_adjust(user.id, -int(amount), "admin_remove", actor_id=interaction.user.id)
        return await interaction.followup.send(f"✅ −{amount:,} from {user.mention}." if ok else f"❌ {res}", ephemeral=True)
    if action == "set":
        bal = games_coin_balance(user.id)
        delta = int(amount) - bal["balance"]
        ok, res = games_coin_adjust(user.id, delta, "admin_set", actor_id=interaction.user.id)
        return await interaction.followup.send(f"✅ {user.mention} balance set to {amount:,}." if ok else f"❌ {res}", ephemeral=True)
    return await interaction.followup.send("Unknown action.", ephemeral=True)



# ---------------- GUESS THE PET ENGINE ----------------

async def games_fetch_pet_icon(asset_id):
    """Download a pet/egg icon — cached, with dual-host fallback. Returns bytes or None."""
    if not asset_id:
        return None
    asset_id = str(asset_id)
    cached = games_icon_from_cache(asset_id)
    if cached is not None:
        return cached
    global session
    try:
        cur_loop = asyncio.get_running_loop()
    except Exception:
        cur_loop = None
    if session is None or session.closed or (
            cur_loop is not None and getattr(session, "_loop", None) is not cur_loop):
        # recreate when bound to a dead/other loop (e.g. after a reconnect)
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
    for host in (PS99_API, GAMES_PUBLIC_API):
        try:
            async with session.get(
                f"{host}/image/{asset_id}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=12),
            ) as res:
                if res.status != 200:
                    continue
                data = await res.read()
                if len(data) > 64:
                    games_icon_cache_put(asset_id, data)
                    return data
        except Exception as exc:
            print(f"[games] pet icon fetch failed {asset_id} @ {host}: {exc}")
            continue
    return None


# ---------------- ICON LAYER: cache + drawn placeholders ----------------
# Every visual falls back to a locally rendered placeholder, so nothing
# ever shows as a broken/blank image when an asset is missing.

_ICON_CACHE = {}      # asset_id -> (fetched_ts, bytes)
_ICON_CACHE_MAX = 400
_ICON_CACHE_TTL = 6 * 3600


def games_icon_from_cache(asset_id):
    hit = _ICON_CACHE.get(str(asset_id))
    if hit and time.time() - hit[0] < _ICON_CACHE_TTL:
        return hit[1]
    return None


def games_icon_cache_put(asset_id, data):
    _ICON_CACHE[str(asset_id)] = (time.time(), data)
    if len(_ICON_CACHE) > _ICON_CACHE_MAX:
        oldest = sorted(_ICON_CACHE.items(), key=lambda kv: kv[1][0])[:len(_ICON_CACHE) - _ICON_CACHE_MAX]
        for k, _v in oldest:
            _ICON_CACHE.pop(k, None)


def _games_font(size, bold=True):
    try:
        path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold \
            else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


def games_build_pet_placeholder(pet_name, tier="common", size=128):
    """Rendered rarity card used when a pet has no icon (never a broken image)."""
    try:
        _emoji, label, rgb = games_tier_style(tier)
        W = H = size
        img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        d.rounded_rectangle((6, 6, W - 6, H - 6), radius=18, fill=rgb + (255,))
        d.rounded_rectangle((6, 6, W - 6, H - 6), radius=18, outline=(255, 255, 255, 210), width=3)
        big = _games_font(44, True)
        small = _games_font(13, True)
        lbl = str(label).upper()
        bb = d.textbbox((0, 0), lbl, font=big)
        d.text(((W - (bb[2] - bb[0])) // 2, (H - (bb[3] - bb[1])) // 2 - 34), lbl, font=big, fill=(255, 255, 255, 255))
        words = str(pet_name).split(" ")
        lines, cur = [], ""
        for w in words:
            t = (cur + " " + w).strip()
            if d.textlength(t, font=small) <= W - 24:
                cur = t
            else:
                if cur:
                    lines.append(cur)
                cur = w
        if cur:
            lines.append(cur)
        lines = lines[:3]
        ty = H - 24 - 15 * len(lines)
        for ln in lines:
            d.text((W // 2 - d.textlength(ln, font=small) // 2, ty), ln, font=small, fill=(255, 255, 255, 255))
            ty += 15
        buf = BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf
    except Exception as exc:
        print(f"[games] pet placeholder failed: {exc}")
        return None


def games_build_egg_placeholder(size=64):
    """Small drawn egg tile used when an egg has no icon asset."""
    try:
        S = size
        img = Image.new("RGBA", (S, S), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        # egg body (ellipse) + shine + base
        d.ellipse((14, 6, S - 14, S - 10), fill=(168, 130, 255, 255), outline=(255, 255, 255, 220), width=3)
        d.ellipse((24, 16, 34, 26), fill=(255, 255, 255, 120))
        d.rounded_rectangle((24, S - 16, S - 24, S - 8), radius=4, fill=(108, 34, 245, 255))
        buf = BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf
    except Exception as exc:
        print(f"[games] egg placeholder failed: {exc}")
        return None


def games_icon_file(icon_bytes, filename, size=None):
    """Bytes -> discord.File, resized to size (square) when given. None on failure."""
    try:
        img = Image.open(BytesIO(icon_bytes)).convert("RGBA")
        if size:
            img = img.resize((size, size), Image.Resampling.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return discord.File(buf, filename=filename)
    except Exception:
        return None


def games_emoji_ok(s):
    """True when the string is a safe, single emoji (unicode or custom)."""
    s = (s or "").strip()
    if not s:
        return False
    if re.fullmatch(r"<a?:[A-Za-z0-9_]{1,32}:\d{15,}>", s):
        return True
    if len(s) > 16:
        return False
    # flag: exactly two regional indicators
    if re.fullmatch(r"[\U0001F1E6-\U0001F1FF]{2}", s):
        return True
    # keycap: digit/#/* + U+20E3
    if re.fullmatch(r"[0-9#*]\uFE0F?\u20E3", s):
        return True
    # single base emoji (+ variation selectors, skintones, ZWJ sequences)
    base = re.sub(r"[\uFE0F\u200D\uFE0E]", "", s)
    base = re.sub(r"[\U0001F3FB-\U0001F3FF]", "", base)
    max_base = 7 if "\u200D" in s else 1
    if 0 < len(base) <= max_base and re.fullmatch(
            r"[\U0001F000-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF\u2190-\u21FF\u2B50\u00A9\u00AE]+",
            base):
        return True
    return False


def games_build_round_image(icon_bytes, mode="zoom"):
    """Transform a pet icon into a guessable puzzle image. Returns BytesIO."""
    try:
        img = Image.open(BytesIO(icon_bytes)).convert("RGBA")
        if mode == "silhouette":
            # black fill on the pet's alpha shape
            alpha = img.split()[3]
            solid = Image.new("RGBA", img.size, (5, 6, 12, 255))
            solid.putalpha(alpha.point(lambda a: 255 if a > 40 else 0))
            img = solid
        elif mode == "blur":
            img = img.filter(ImageFilter.GaussianBlur(radius=max(5, min(img.size) // 18)))
        elif mode == "monochrome":
            alpha = img.split()[3]
            img = ImageOps.grayscale(img.convert("RGB")).convert("RGBA")
            img.putalpha(alpha)
        elif mode == "negative":
            alpha = img.split()[3]
            rgb = ImageOps.invert(img.convert("RGB"))
            img = rgb.convert("RGBA")
            img.putalpha(alpha)
        elif mode == "reveal":
            img = img.filter(ImageFilter.GaussianBlur(radius=1.2))
        elif mode == "pixel":
            small = img.resize((12, 12), Image.Resampling.BILINEAR)
            img = small.resize((img.width, img.height), Image.Resampling.NEAREST)
        elif mode == "scrambled":
            w, h = img.size
            tw, th = w // 4, h // 4
            tiles = []
            for ty in range(4):
                for tx in range(4):
                    tiles.append(img.crop((tx * tw, ty * th, (tx + 1) * tw, (ty + 1) * th)))
            order = list(range(16))
            secrets.SystemRandom().shuffle(order)
            canvas = Image.new("RGBA", (w, h), (10, 12, 22, 255))
            for i, idx in enumerate(order):
                tx, ty = i % 4, i // 4
                canvas.paste(tiles[idx], (tx * tw, ty * th))
            img = canvas
        else:  # zoom
            w, h = img.size
            cw, ch = max(24, w // 3), max(24, h // 3)
            x = secrets.randbelow(max(1, w - cw))
            y = secrets.randbelow(max(1, h - ch))
            crop = img.crop((x, y, x + cw, y + ch))
            img = crop.resize((w, h), Image.Resampling.NEAREST)
        buf = BytesIO()
        img.convert("RGB").save(buf, format="PNG", optimize=True)
        buf.seek(0)
        return buf
    except Exception as exc:
        print(f"[games] round image build failed: {exc}")
        return None


GAMES_GUESS_MODE_INFO = {
    "zoom": ("🔎 Extreme Zoom", "A tiny detail is all you get.", "purple", 1.00),
    "silhouette": ("🌑 Silhouette", "Only the outline survived.", "slate", 1.20),
    "pixel": ("👾 Pixel Panic", "The pet has gone full retro.", "blue", 1.05),
    "scrambled": ("🧩 Tile Scramble", "Its picture has been shuffled into pieces.", "amber", 1.15),
    "blur": ("🌫️ Blur Blast", "The camera refused to focus.", "cyan", 0.95),
    "monochrome": ("🎞️ Monochrome", "All the colour has vanished.", "slate", 0.90),
    "negative": ("👽 Negative", "Every colour has been inverted.", "pink", 1.10),
    "letters": ("🔤 Letter Mixer", "Unscramble the letters in its name.", "cyan", 0.80),
}


def games_guess_profile(user_id):
    empty = {"wins": 0, "rounds": 0, "current_streak": 0, "best_streak": 0,
             "fastest_ms": None, "total_reward": 0, "valid_guesses": 0}
    if not db_enabled():
        return empty
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT wins, rounds, current_streak, best_streak, fastest_ms,
                          total_reward, valid_guesses
                   FROM mcwv_guess_profiles WHERE discord_id = %s""",
                (int(user_id),),
            )
            row = cur.fetchone()
        if not row:
            return empty
        return {
            "wins": int(row[0] or 0), "rounds": int(row[1] or 0),
            "current_streak": int(row[2] or 0), "best_streak": int(row[3] or 0),
            "fastest_ms": int(row[4]) if row[4] is not None else None,
            "total_reward": int(row[5] or 0), "valid_guesses": int(row[6] or 0),
        }
    except Exception as exc:
        print(f"[games] guess profile read failed: {exc}")
        return empty


def games_guess_record_result(round_info, winner_id=None, reward=0, elapsed_ms=None):
    """Persist per-player rounds/streaks. A timeout resets participating streaks."""
    participants = sorted({int(uid) for uid in round_info.get("participants", set()) if uid})
    attempts = round_info.get("attempts", {})
    if winner_id is not None and int(winner_id) not in participants:
        participants.append(int(winner_id))
    if not participants or not db_enabled():
        return
    try:
        with conn:
            with conn.cursor() as cur:
                for uid in participants:
                    valid = int(attempts.get(uid, 0))
                    if winner_id is not None and uid == int(winner_id):
                        cur.execute(
                            """INSERT INTO mcwv_guess_profiles
                               (discord_id, wins, rounds, current_streak, best_streak,
                                fastest_ms, total_reward, valid_guesses)
                               VALUES (%s,1,1,1,1,%s,%s,%s)
                               ON CONFLICT (discord_id) DO UPDATE SET
                                 wins = mcwv_guess_profiles.wins + 1,
                                 rounds = mcwv_guess_profiles.rounds + 1,
                                 current_streak = mcwv_guess_profiles.current_streak + 1,
                                 best_streak = GREATEST(mcwv_guess_profiles.best_streak,
                                                        mcwv_guess_profiles.current_streak + 1),
                                 fastest_ms = CASE
                                   WHEN mcwv_guess_profiles.fastest_ms IS NULL THEN EXCLUDED.fastest_ms
                                   ELSE LEAST(mcwv_guess_profiles.fastest_ms, EXCLUDED.fastest_ms) END,
                                 total_reward = mcwv_guess_profiles.total_reward + EXCLUDED.total_reward,
                                 valid_guesses = mcwv_guess_profiles.valid_guesses + EXCLUDED.valid_guesses,
                                 updated_at = NOW()""",
                            (uid, int(elapsed_ms or 0), int(reward), valid),
                        )
                    else:
                        cur.execute(
                            """INSERT INTO mcwv_guess_profiles
                               (discord_id, rounds, current_streak, valid_guesses)
                               VALUES (%s,1,0,%s)
                               ON CONFLICT (discord_id) DO UPDATE SET
                                 rounds = mcwv_guess_profiles.rounds + 1,
                                 current_streak = 0,
                                 valid_guesses = mcwv_guess_profiles.valid_guesses + EXCLUDED.valid_guesses,
                                 updated_at = NOW()""",
                            (uid, valid),
                        )
    except Exception as exc:
        print(f"[games] guess profile write failed: {exc}")


def games_guess_reward(round_info, elapsed, prior_streak=0):
    if not round_info.get("rewarded", True):
        return 0
    mode = round_info.get("mode", "zoom")
    multiplier = GAMES_GUESS_MODE_INFO.get(mode, (None, None, None, 1.0))[3]
    reward = int(round(GAMES_GUESS_BASE_REWARD * multiplier))
    reward += max(0, int((GAMES_GUESS_TIMEOUT - float(elapsed)) * 1.25))
    reward -= int(round_info.get("hint_step", 0)) * 45
    reward += min(max(int(prior_streak), 0), 5) * 15
    if str(round_info.get("pet_name", "")).startswith(("Titanic", "Gargantuan")):
        reward += 35
    reward = max(GAMES_GUESS_MIN_REWARD, min(GAMES_GUESS_MAX_REWARD, reward))
    return int(round(reward / 5.0) * 5)


def games_guess_cancel_clock(channel_id):
    task = ACTIVE_GUESS_TASKS.pop(int(channel_id), None)
    if task is not None and task is not asyncio.current_task():
        task.cancel()


async def games_guess_timeout(channel_id, started):
    channel_id = int(channel_id)
    round_info = ACTIVE_GUESS_ROUNDS.get(channel_id)
    if not round_info or float(round_info.get("started", 0)) != float(started):
        return
    ACTIVE_GUESS_ROUNDS.pop(channel_id, None)
    games_guess_cancel_clock(channel_id)
    if round_info.get("rewarded", True):
        games_guess_record_result(round_info)
        games_track_participants("guess", round_info.get("participants"), winner_id=None)
    channel = bot.get_channel(channel_id)
    if channel is None:
        return
    embed = discord.Embed(
        title="⌛ Nobody got it!",
        description=f"The mystery pet was **{round_info.get('pet_name', '?')}**.",
        color=games_color("slate"),
    )
    embed.add_field(name="Mode", value=GAMES_GUESS_MODE_INFO.get(round_info.get("mode"), (round_info.get("mode"),))[0], inline=True)
    embed.add_field(name="Players", value=str(len(round_info.get("participants", set()))), inline=True)
    embed.add_field(name="Valid guesses", value=str(sum(round_info.get("attempts", {}).values())), inline=True)
    if not round_info.get("rewarded", True):
        embed.add_field(name="Round type", value="Staff practice", inline=True)
    games_footer(embed, "Only real pet-name guesses consume an attempt")
    file = None
    if round_info.get("icon"):
        file = games_icon_file(round_info["icon"], "answer_pet.png", size=128)
    if file:
        embed.set_thumbnail(url="attachment://answer_pet.png")
    try:
        await channel.send(embed=embed, file=file)
    except Exception:
        pass


async def games_guess_clock(channel_id, started):
    """Exact hints and deadline; housekeeping is only a fallback."""
    try:
        for due in GAMES_GUESS_HINT_TIMES:
            await asyncio.sleep(max(0.0, due - (time.time() - float(started))))
            round_info = ACTIVE_GUESS_ROUNDS.get(int(channel_id))
            if not round_info or float(round_info.get("started", 0)) != float(started):
                return
            channel = bot.get_channel(int(channel_id))
            if channel:
                await games_post_hint(channel, round_info)
        await asyncio.sleep(max(0.0, GAMES_GUESS_TIMEOUT - (time.time() - float(started))))
        await games_guess_timeout(channel_id, started)
    except asyncio.CancelledError:
        return
    except Exception as exc:
        print(f"[games] guess clock failed: {exc}")
    finally:
        task = ACTIVE_GUESS_TASKS.get(int(channel_id))
        if task is asyncio.current_task():
            ACTIVE_GUESS_TASKS.pop(int(channel_id), None)


async def games_start_guess_round(channel, pet_key=None, mode=None, rewarded=True, source="spawn"):
    """Post a polished Guess-the-Pet round with one active/start lock per channel."""
    channel_id = int(channel.id)
    if channel_id in ACTIVE_GUESS_ROUNDS or channel_id in ACTIVE_GUESS_STARTING:
        return None
    ACTIVE_GUESS_CANCEL_REQUESTS.discard(channel_id)
    ACTIVE_GUESS_STARTING.add(channel_id)
    try:
        pets = games_guess_pet_pool()
        if not pets:
            return None
        if pet_key is not None:
            wanted = normalize_answer(pet_key)
            pet_key = next((name for name in pets if normalize_answer(name) == wanted), None)
            if pet_key is None:
                return None
        else:
            pet_key = games_pick_random(pets, scope=f"guess_pet:{channel_id}", max_recent=12)

        all_modes = list(GAMES_GUESS_MODE_INFO)
        if mode not in all_modes:
            recent_modes = _RECENT_SCOPE.setdefault(f"guess_mode:{channel_id}", [])
            candidates = [m for m in all_modes if m not in recent_modes] or all_modes
            mode = secrets.choice(candidates)
            recent_modes.append(mode)
            if len(recent_modes) > 5:
                recent_modes.pop(0)

        asset_id = games_pet_asset(pet_key)
        icon = await games_fetch_pet_icon(asset_id) if asset_id else None
        image_buf = None
        if icon and mode != "letters":
            image_buf = games_build_round_image(icon, mode)
        if image_buf is None and mode != "letters":
            mode = "letters"

        letters_txt = None
        if mode == "letters":
            letters = list(re.sub(r"[^A-Za-z]", "", str(pet_key)).lower())
            shuffled = letters[:]
            while len(shuffled) > 1 and shuffled == letters:
                secrets.SystemRandom().shuffle(shuffled)
            letters_txt = " ".join(shuffled).upper()

        mode_label, mode_flavor, mode_color, _multiplier = GAMES_GUESS_MODE_INFO[mode]
        short_counts = {}
        for name in pets:
            short = normalize_answer(games_guess_short_name(name))
            if short:
                short_counts[short] = short_counts.get(short, 0) + 1
        valid_names = {normalize_answer(name) for name in pets}
        valid_names.update(short for short, count in short_counts.items() if count == 1 and len(short) >= 4)
        started = time.time()
        round_info = {
            "pet_key": pet_key, "pet_name": pet_key, "mode": mode, "icon": icon,
            "started": started, "attempts": {}, "participants": set(),
            "channel_id": channel_id, "hint_step": 0, "rewarded": bool(rewarded),
            "source": source, "aliases": games_guess_aliases(pet_key, pets),
            "valid_names": valid_names, "typo_index": games_guess_typo_index(valid_names),
        }
        starting_reward = games_guess_reward(round_info, 0, 0)
        round_embed = discord.Embed(
            title=f"🐾 Guess the Pet — {mode_label}",
            description=(
                f"{mode_flavor}\n\n"
                + (f"Reward meter starts at **🪙 {starting_reward:,}** — streak bonuses can reach **{GAMES_GUESS_MAX_REWARD:,}**!" if rewarded
                   else "**Staff practice round** — no coins, just bragging rights!")
                + "\nType a real pet name in chat. Normal conversation does not use attempts."
            ),
            color=games_color(mode_color),
        )
        if letters_txt:
            round_embed.add_field(name="Mixed letters", value=f"`{letters_txt}`", inline=False)
        round_embed.add_field(name="Attempts", value=f"**{GAMES_MAX_ANSWER_ATTEMPTS}** valid guesses each", inline=True)
        round_embed.add_field(name="Timer", value=f"**{GAMES_GUESS_TIMEOUT}s**", inline=True)
        round_embed.add_field(name="Hints", value="At **30s** and **60s**", inline=True)
        games_footer(round_embed, "🔥 = close · 🟡 = warm · ❌ = cold · one typo allowed on long names")

        if channel_id in ACTIVE_GUESS_CANCEL_REQUESTS:
            ACTIVE_GUESS_CANCEL_REQUESTS.discard(channel_id)
            return False
        if image_buf:
            round_message = await channel.send(
                embed=round_embed, file=discord.File(image_buf, filename="guess_pet.png")
            )
        else:
            round_message = await channel.send(embed=round_embed)
        round_info["message_id"] = getattr(round_message, "id", None)
        ACTIVE_GUESS_ROUNDS[channel_id] = round_info
        games_track("guess", channel_id, sessions=1)
        clock = asyncio.create_task(games_guess_clock(channel_id, started))
        ACTIVE_GUESS_TASKS[channel_id] = clock
        return round_info
    except Exception as exc:
        print(f"[games] guess round start failed: {exc}")
        return None
    finally:
        ACTIVE_GUESS_STARTING.discard(channel_id)
        ACTIVE_GUESS_CANCEL_REQUESTS.discard(channel_id)



def games_spawn_allowed(channel_id):
    raw = db_get_setting(GAMES_SETTING_SPAWN_CHANNELS, "[]")
    try:
        chans = json.loads(raw or "[]")
    except Exception:
        chans = []
    return int(channel_id) in chans


async def games_maybe_spawn(message):
    """1% random spawn — called from on_message."""
    if not message.guild or message.author.bot:
        return
    if not games_spawn_allowed(message.channel.id):
        return
    if not games_gate_message(message.author.id):
        return
    if message.channel.id in ACTIVE_GUESS_ROUNDS or message.channel.id in ACTIVE_GUESS_STARTING:
        return
    now = time.time()
    if now - float(_spawn_last_channel.get(message.channel.id, 0)) < GAMES_SPAWN_CHANNEL_COOLDOWN:
        return
    if now - float(_spawn_last_global.get("ts", 0)) < GAMES_SPAWN_GLOBAL_COOLDOWN:
        return
    try:
        chance = float(db_get_setting(GAMES_SETTING_SPAWN_CHANCE, str(GAMES_DEFAULT_CHANCE_PCT)) or GAMES_DEFAULT_CHANCE_PCT)
    except Exception:
        chance = GAMES_DEFAULT_CHANCE_PCT
    if not games_roll_chance(chance):
        return
    _spawn_last_channel[message.channel.id] = now
    _spawn_last_global["ts"] = now
    try:
        # Keep a strong task reference while the icon is fetched and puzzle built.
        channel_id = int(message.channel.id)
        task = asyncio.create_task(games_start_guess_round(message.channel, rewarded=True, source="spawn"))
        ACTIVE_GUESS_START_TASKS[channel_id] = task
        task.add_done_callback(
            lambda done, cid=channel_id: ACTIVE_GUESS_START_TASKS.pop(cid, None)
            if ACTIVE_GUESS_START_TASKS.get(cid) is done else None
        )
    except Exception as exc:
        print(f"[games] random spawn failed: {exc}")


async def games_handle_answer(message):
    """Handle only pet-shaped guesses; ordinary conversation is never penalized."""
    round_info = ACTIVE_GUESS_ROUNDS.get(message.channel.id)
    if not round_info or message.author.bot:
        return False
    if time.time() - float(round_info["started"]) >= GAMES_GUESS_TIMEOUT:
        await games_guess_timeout(message.channel.id, round_info["started"])
        return False

    answer = normalize_answer(message.content)
    correct, similarity = games_guess_answer_result(
        message.content, round_info["pet_name"], round_info.get("aliases")
    )
    # Wrong guesses count only when they are catalogue names/unique aliases (or a
    # conservative one-edit typo of one). This also closes the unlimited-typo loophole.
    if not correct and not games_guess_is_catalogue_attempt(
            answer, round_info.get("valid_names", set()), round_info.get("typo_index", {})):
        return False

    attempts = int(round_info.setdefault("attempts", {}).get(message.author.id, 0))
    if attempts >= GAMES_MAX_ANSWER_ATTEMPTS:
        try:
            await message.add_reaction("🚫")
        except Exception:
            pass
        return True
    round_info["attempts"][message.author.id] = attempts + 1
    round_info.setdefault("participants", set()).add(message.author.id)

    if not correct:
        reaction = "🔥" if similarity >= 0.68 else ("🟡" if similarity >= 0.43 else "❌")
        if attempts + 1 >= GAMES_MAX_ANSWER_ATTEMPTS:
            reaction = "🚫"
        try:
            await message.add_reaction(reaction)
        except Exception:
            pass
        return True

    # Pop before the first await: two near-simultaneous correct messages cannot both win.
    ACTIVE_GUESS_ROUNDS.pop(message.channel.id, None)
    games_guess_cancel_clock(message.channel.id)
    elapsed = max(0.0, time.time() - float(round_info["started"]))
    rewarded_round = bool(round_info.get("rewarded", True))
    previous = games_guess_profile(message.author.id)
    reward = games_guess_reward(round_info, elapsed, previous.get("current_streak", 0))
    if reward > 0:
        paid, error = games_coin_adjust(
            message.author.id, reward, "guess_win",
            meta={"pet": round_info["pet_name"], "mode": round_info["mode"],
                  "seconds": round(elapsed, 3), "hints": round_info.get("hint_step", 0)},
        )
        if not paid:
            print(f"[games] guess reward failed for {message.author.id}: {error}")
            reward = 0
    if rewarded_round:
        elapsed_ms = int(round(elapsed * 1000))
        games_guess_record_result(round_info, message.author.id, reward, elapsed_ms)
        games_track("guess", message.channel.id, sessions=0, minted=reward)
        games_track_participants("guess", round_info.get("participants"), winner_id=message.author.id)
        profile = games_guess_profile(message.author.id)
    else:
        # Staff practice cannot farm coins, leaderboard wins, streaks or role progress.
        profile = previous

    try:
        speed_title = "⚡ LIGHTNING ANSWER!" if elapsed < 12 else ("🎯 Nailed it!" if not round_info.get("hint_step") else "🎉 Correct!")
        win_embed = discord.Embed(
            title=speed_title,
            description=f"{getattr(message.author, 'mention', '')} identified **{round_info['pet_name']}**!",
            color=games_color("gold" if elapsed < 12 else "green"),
        )
        win_embed.add_field(
            name="Reward",
            value=f"+**{reward:,}** 🪙" if reward else ("Practice round" if not round_info.get("rewarded") else "⚠️ Reward could not be credited"),
            inline=True,
        )
        win_embed.add_field(name="Speed", value=f"**{elapsed:.2f}s**", inline=True)
        win_embed.add_field(name="Attempts", value=f"**{attempts + 1}/{GAMES_MAX_ANSWER_ATTEMPTS}**", inline=True)
        win_embed.add_field(
            name="Win streak" if rewarded_round else "Practice",
            value=(f"🔥 **{profile.get('current_streak', previous.get('current_streak', 0) + 1)}** "
                   f"(best {profile.get('best_streak', 0)})"
                   if rewarded_round else "Coins and stats unchanged"),
            inline=True,
        )
        win_embed.add_field(
            name="Mode",
            value=GAMES_GUESS_MODE_INFO.get(round_info.get("mode"), (round_info.get("mode", "?"),))[0],
            inline=True,
        )
        win_embed.add_field(name="Hints used", value=f"**{round_info.get('hint_step', 0)}**", inline=True)
        games_footer(win_embed, "Fast answers, harder modes and streaks earn more · normal chat never costs attempts")
        pet_file = None
        icon_bytes = round_info.get("icon")
        if icon_bytes is None:
            asset = games_pet_asset(round_info["pet_name"])
            if asset:
                icon_bytes = await games_fetch_pet_icon(asset)
        if icon_bytes:
            pet_file = games_icon_file(icon_bytes, "win_pet.png", size=128)
        if pet_file is None:
            ph = games_build_pet_placeholder(round_info["pet_name"], "common", size=128)
            if ph:
                pet_file = discord.File(ph, filename="win_pet.png")
        if pet_file:
            win_embed.set_thumbnail(url="attachment://win_pet.png")
        await message.channel.send(embed=win_embed, file=pet_file)
    except Exception as exc:
        print(f"[games] guess win reveal failed: {exc}")
    return True



async def games_guess_pet_autocomplete(interaction: discord.Interaction, current: str):
    current = normalize_answer(current)
    matches = [name for name in games_guess_pet_pool() if not current or current in normalize_answer(name)]
    return [app_commands.Choice(name=name[:100], value=name) for name in matches[:25]]


@bot.tree.command(name="guess", description="Guess the Pet: stats or staff practice rounds", guild=guild_obj)
@app_commands.describe(
    action="Start/stop a practice round or view your stats",
    mode="Puzzle style (random rotates all modes)",
    pet="Optional real pet name for a staff practice round",
)
@app_commands.choices(action=[
    app_commands.Choice(name="start practice round (game staff)", value="start"),
    app_commands.Choice(name="my stats", value="stats"),
    app_commands.Choice(name="stop round (game staff)", value="stop"),
])
@app_commands.choices(mode=[
    app_commands.Choice(name="random rotation", value="random"),
    app_commands.Choice(name="extreme zoom", value="zoom"),
    app_commands.Choice(name="silhouette", value="silhouette"),
    app_commands.Choice(name="pixel panic", value="pixel"),
    app_commands.Choice(name="tile scramble", value="scrambled"),
    app_commands.Choice(name="blur blast", value="blur"),
    app_commands.Choice(name="monochrome", value="monochrome"),
    app_commands.Choice(name="negative colours", value="negative"),
    app_commands.Choice(name="mixed letters", value="letters"),
])
@app_commands.autocomplete(pet=games_guess_pet_autocomplete)
async def games_guess(interaction: discord.Interaction, action: str, mode: str = "random", pet: str = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    if action == "stats":
        profile = games_guess_profile(interaction.user.id)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT sessions, coins_minted FROM mcwv_game_stats WHERE game = 'guess'")
                global_row = cur.fetchone()
                cur.execute(
                    """SELECT discord_id, wins, best_streak FROM mcwv_guess_profiles
                       ORDER BY wins DESC, best_streak DESC LIMIT 5"""
                )
                leaders = cur.fetchall()
        except Exception:
            global_row, leaders = None, []
        rounds = profile["rounds"]
        accuracy = (profile["wins"] / rounds * 100.0) if rounds else 0.0
        fastest = f"{profile['fastest_ms'] / 1000:.2f}s" if profile["fastest_ms"] is not None else "—"
        embed = discord.Embed(
            title="🐾 Guess the Pet — Your Record",
            description=f"Stats for {interaction.user.mention}",
            color=games_color("purple"),
        )
        embed.add_field(name="Wins", value=f"**{profile['wins']:,}**", inline=True)
        embed.add_field(name="Rounds", value=f"**{rounds:,}**", inline=True)
        embed.add_field(name="Win rate", value=f"**{accuracy:.1f}%**", inline=True)
        embed.add_field(name="Current streak", value=f"🔥 **{profile['current_streak']}**", inline=True)
        embed.add_field(name="Best streak", value=f"🏆 **{profile['best_streak']}**", inline=True)
        embed.add_field(name="Fastest answer", value=f"⚡ **{fastest}**", inline=True)
        embed.add_field(name="Coins won", value=f"🪙 **{profile['total_reward']:,}**", inline=True)
        embed.add_field(name="Valid guesses", value=f"**{profile['valid_guesses']:,}**", inline=True)
        if global_row:
            embed.add_field(
                name="Server totals",
                value=f"**{int(global_row[0] or 0):,}** rounds · **{int(global_row[1] or 0):,}** coins awarded",
                inline=False,
            )
        if leaders:
            embed.add_field(
                name="Top detectives",
                value="\n".join(
                    f"{i + 1}. <@{uid}> — **{wins}** wins · best streak **{best}**"
                    for i, (uid, wins, best) in enumerate(leaders)
                ),
                inline=False,
            )
        games_footer(embed, "Rewarded rounds spawn automatically · staff-started rounds are practice")
        return await interaction.followup.send(embed=embed, ephemeral=True)

    if not games_staff_check(interaction.user):
        return await interaction.followup.send("❌ Game staff only.", ephemeral=True)

    if action == "stop":
        round_info = ACTIVE_GUESS_ROUNDS.pop(interaction.channel.id, None)
        if not round_info:
            if interaction.channel.id in ACTIVE_GUESS_STARTING:
                ACTIVE_GUESS_CANCEL_REQUESTS.add(interaction.channel.id)
                build_task = ACTIVE_GUESS_START_TASKS.pop(interaction.channel.id, None)
                if build_task:
                    build_task.cancel()
                return await interaction.followup.send("🛑 Round preparation cancelled.", ephemeral=True)
            return await interaction.followup.send("No active round in this channel.", ephemeral=True)
        games_guess_cancel_clock(interaction.channel.id)
        return await interaction.followup.send(
            f"🛑 {'Practice' if not round_info.get('rewarded', True) else 'Rewarded'} round stopped — "
            f"it was **{round_info.get('pet_name', '?')}**.", ephemeral=False
        )

    if action != "start":
        return await interaction.followup.send("❓ Unknown action.", ephemeral=True)
    if interaction.channel.id in ACTIVE_GUESS_ROUNDS or interaction.channel.id in ACTIVE_GUESS_STARTING:
        return await interaction.followup.send("❌ A Guess the Pet round is already active or loading here.", ephemeral=True)

    chosen_pet = None
    if pet:
        wanted = normalize_answer(pet)
        chosen_pet = next((name for name in games_guess_pet_pool() if normalize_answer(name) == wanted), None)
        if chosen_pet is None:
            return await interaction.followup.send("❌ That isn't a recognised pet. Choose one from autocomplete.", ephemeral=True)
    selected_mode = None if (mode or "random") == "random" else mode
    started = await games_start_guess_round(
        interaction.channel, pet_key=chosen_pet, mode=selected_mode,
        rewarded=False, source="staff_practice",
    )
    if started is False:
        return await interaction.followup.send("🛑 Round preparation was cancelled.", ephemeral=True)
    if not started:
        return await interaction.followup.send("❌ Could not start the round; check that another round isn't loading.", ephemeral=True)
    await interaction.followup.send(
        "✅ Practice round started above. Automatic random spawns remain the rewarded rounds.", ephemeral=True
    )



# ---------------- /hatch + /pets ----------------







@bot.tree.command(name="pets", description="Your hatched pet collection", guild=guild_obj)
@app_commands.describe(user="Whose collection to view")
async def games_pets(interaction: discord.Interaction, user: discord.User = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    target = user or interaction.user
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pet_key, count, first_hatched_at FROM mcwv_pet_collections WHERE discord_id = %s ORDER BY count DESC, pet_key ASC LIMIT 25", (target.id,))
            rows = cur.fetchall()
            cur.execute("SELECT COUNT(*), COALESCE(SUM(count), 0) FROM mcwv_pet_collections WHERE discord_id = %s", (target.id,))
            agg = cur.fetchone()
            total_unique, total_pets = int(agg[0] or 0), int(agg[1] or 0)
        if not rows:
            return await interaction.followup.send(f"{target.display_name} hasn't hatched anything yet — try `/hatch`!", ephemeral=True)
        lines = []
        for name, count, _ in rows:
            emoji = "🌌" if name.startswith("Titanic") else "🌠" if name.startswith("Gargantuan") \
                else "💥" if name.startswith("Huge") else "🐾"
            lines.append(f"{emoji} **{name}** ×{count}")
        # collection completion vs the real pet database
        total_in_db = len(games_get_pets())
        pct = (total_unique / total_in_db * 100) if total_in_db else 0
        embed = discord.Embed(
            title=f"🐾 {target.display_name}'s Pet Collection",
            description="\n".join(lines),
            color=games_color("purple"),
        )
        avatar = getattr(target, "display_avatar", None) or getattr(target, "avatar", None)
        if avatar is not None:
            embed.set_thumbnail(url=avatar.url)
        embed.add_field(name="Unique pets", value=f"**{total_unique}** / {total_in_db} real pets", inline=True)
        embed.add_field(name="Total hatches", value=f"**{total_pets}**", inline=True)
        embed.add_field(name="Collection book", value=f"`{games_bar(total_unique, max(total_in_db, 1), 12)}` {pct:.1f}%", inline=False)
        games_footer(embed, "/hatch to add more · real pets from the BIG Games database")
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"❌ Collection failed: `{type(exc).__name__}`", ephemeral=True)



# ---------------- DUELS (live 1v1) ----------------

def games_duel_escrow(duel):
    """Atomically reserve both wagers and persist the active duel."""
    duel_id = int(duel["id"])
    wager = int(duel["wager"])
    participants = (int(duel["challenger"]), int(duel["target"]))
    virtual = {uid: games_is_unlimited(uid) for uid in participants}
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT state FROM mcwv_duels WHERE id = %s FOR UPDATE", (duel_id,))
                row = cur.fetchone()
                if not row or row[0] != "pending":
                    raise ValueError("That duel is no longer pending.")
                paid = []
                balances = []
                for index, uid in enumerate(participants):
                    cur.execute(
                        "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                        (uid,),
                    )
                    if virtual[uid]:
                        cur.execute("SELECT balance FROM mcwv_coins WHERE discord_id = %s", (uid,))
                    else:
                        cur.execute(
                            """UPDATE mcwv_coins
                               SET balance = balance - %s, total_spent = total_spent + %s
                               WHERE discord_id = %s AND balance >= %s RETURNING balance""",
                            (wager, wager, uid, wager),
                        )
                    balance_row = cur.fetchone()
                    if balance_row is None:
                        who = "Challenger" if index == 0 else "Challenged player"
                        raise ValueError(f"{who} no longer has enough coins.")
                    paid.append(0 if virtual[uid] else wager)
                    balances.append(int(balance_row[0] or 0))
                cur.execute(
                    """UPDATE mcwv_duels
                       SET state = 'active', challenger_paid = %s, target_paid = %s, escrow_version = 1
                       WHERE id = %s AND state = 'pending' RETURNING id""",
                    (paid[0], paid[1], duel_id),
                )
                if cur.fetchone() is None:
                    raise ValueError("That duel is no longer pending.")
                for uid, amount, balance_after in zip(participants, paid, balances):
                    cur.execute(
                        """INSERT INTO mcwv_coin_log
                           (actor_id, target_id, type, amount, balance_after, meta)
                           VALUES (%s,%s,%s,%s,%s,%s::jsonb)""",
                        (uid, uid, "duel_escrow" if amount else "duel_escrow_test",
                         -amount, balance_after, json.dumps({"duel": duel_id, "wager": wager})),
                    )
        duel["paid"] = tuple(paid)
        return True, None
    except ValueError as exc:
        return False, str(exc)
    except Exception as exc:
        print(f"[games] duel escrow failed: {exc}")
        return False, f"{type(exc).__name__}: {exc}"


def games_duel_settle_db(duel, winner_id=None):
    """Atomically pay the winner or refund both players, exactly once."""
    duel_id = int(duel["id"])
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT state, challenger, target, wager, challenger_paid, target_paid, escrow_version
                       FROM mcwv_duels WHERE id = %s FOR UPDATE""",
                    (duel_id,),
                )
                row = cur.fetchone()
                if not row or row[0] != "active":
                    return False, "Duel was already settled."
                _state, challenger, target, wager, paid_a, paid_b, version = row
                wager = int(wager or 0)
                paid_a = int(paid_a or 0) if int(version or 0) >= 1 else wager
                paid_b = int(paid_b or 0) if int(version or 0) >= 1 else wager
                if winner_id is not None:
                    winner_id = int(winner_id)
                    if winner_id not in (int(challenger), int(target)):
                        raise ValueError("Invalid duel winner.")
                    pot = wager * 2
                    cur.execute(
                        "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                        (winner_id,),
                    )
                    cur.execute(
                        """UPDATE mcwv_coins SET balance = balance + %s, total_earned = total_earned + %s
                           WHERE discord_id = %s RETURNING balance""",
                        (pot, pot, winner_id),
                    )
                    balance_after = int(cur.fetchone()[0] or 0)
                    cur.execute(
                        """INSERT INTO mcwv_coin_log
                           (actor_id, target_id, type, amount, balance_after, meta)
                           VALUES (%s,%s,'duel_win',%s,%s,%s::jsonb)""",
                        (winner_id, winner_id, pot, balance_after, json.dumps({"duel": duel_id})),
                    )
                    cur.execute(
                        "UPDATE mcwv_duels SET state = 'settled', winner = %s WHERE id = %s",
                        (winner_id, duel_id),
                    )
                    return True, pot
                for uid, amount in ((int(challenger), paid_a), (int(target), paid_b)):
                    if amount <= 0:
                        continue
                    cur.execute(
                        "UPDATE mcwv_coins SET balance = balance + %s WHERE discord_id = %s RETURNING balance",
                        (amount, uid),
                    )
                    balance_after = int(cur.fetchone()[0] or 0)
                    cur.execute(
                        """INSERT INTO mcwv_coin_log
                           (actor_id, target_id, type, amount, balance_after, meta)
                           VALUES (%s,%s,'duel_refund',%s,%s,%s::jsonb)""",
                        (uid, uid, amount, balance_after, json.dumps({"duel": duel_id})),
                    )
                cur.execute("UPDATE mcwv_duels SET state = 'cancelled' WHERE id = %s", (duel_id,))
        return True, paid_a + paid_b
    except Exception as exc:
        print(f"[games] duel settlement transaction failed: {exc}")
        return False, f"{type(exc).__name__}: {exc}"


def games_recover_stale_duels():
    """Refund persisted active escrows after a restart; row locks make it exactly once."""
    if not db_enabled():
        return 0
    recovered = 0
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT id, challenger, target, wager, challenger_paid, target_paid, escrow_version
                       FROM mcwv_duels WHERE state = 'active' FOR UPDATE"""
                )
                for duel_id, challenger, target, wager, paid_a, paid_b, version in cur.fetchall():
                    wager = int(wager or 0)
                    amounts = (
                        int(paid_a or 0) if int(version or 0) >= 1 else wager,
                        int(paid_b or 0) if int(version or 0) >= 1 else wager,
                    )
                    for uid, amount in zip((int(challenger), int(target)), amounts):
                        if amount <= 0:
                            continue
                        cur.execute(
                            "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                            (uid,),
                        )
                        cur.execute(
                            "UPDATE mcwv_coins SET balance = balance + %s WHERE discord_id = %s RETURNING balance",
                            (amount, uid),
                        )
                        balance_after = int(cur.fetchone()[0] or 0)
                        cur.execute(
                            """INSERT INTO mcwv_coin_log
                               (actor_id, target_id, type, amount, balance_after, meta)
                               VALUES (%s,%s,'duel_restart_refund',%s,%s,%s::jsonb)""",
                            (uid, uid, amount, balance_after, json.dumps({"duel": int(duel_id)})),
                        )
                    cur.execute("UPDATE mcwv_duels SET state = 'recovered' WHERE id = %s", (int(duel_id),))
                    recovered += 1
        return recovered
    except Exception as exc:
        print(f"[games] stale duel recovery failed: {exc}")
        return 0


class DuelChallengeView(discord.ui.View):
    def __init__(self, duel_id, target_id, challenger_id, wager, game_type):
        super().__init__(timeout=45)
        self.duel_id = duel_id
        self.target_id = int(target_id)
        self.challenger_id = int(challenger_id)
        self.wager = int(wager)
        self.game_type = game_type
        self.message = None

    async def on_timeout(self):
        """Expire unanswered challenges so neither player remains blocked."""
        duel = ACTIVE_DUELS.get(self.duel_id)
        if not duel or duel.get("state") != "pending":
            return
        duel["state"] = "expired"
        ACTIVE_DUELS.pop(self.duel_id, None)
        if db_enabled():
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE mcwv_duels SET state = 'expired' WHERE id = %s AND state = 'pending'",
                        (int(self.duel_id),),
                    )
                conn.commit()
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"[games] pending duel expiry failed: {exc}")
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(content="⏰ Duel challenge expired — no coins were taken.", view=self)
            except Exception:
                pass

    @discord.ui.button(label="Accept Duel", style=discord.ButtonStyle.success, emoji="\u2694\ufe0f")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Defer FIRST: escrow spends are blocking DB work.
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass
        if interaction.user.id != self.target_id:
            return await interaction.followup.send("Only the challenged player can accept.", ephemeral=True)
        duel = ACTIVE_DUELS.get(self.duel_id)
        if not duel or duel["state"] != "pending":
            return await interaction.followup.send("That duel is no longer pending.", ephemeral=True)
        duel["state"] = "accepting"  # closes the double-click window before any DB work
        ok, error = games_duel_escrow(duel)
        if not ok:
            duel["state"] = "pending"
            return await interaction.followup.send(f"❌ Duel couldn't start: {error}", ephemeral=True)
        duel["state"] = "active"
        duel["escrowed"] = True
        self.stop()
        try:
            await interaction.edit_original_response(content="\u2694\ufe0f **DUEL ACCEPTED — good luck both!**", view=None)
        except Exception:
            await interaction.followup.send("\u2694\ufe0f **DUEL ACCEPTED — good luck both!**", ephemeral=True)
        try:
            await start_duel_round(interaction.channel, duel)
        except Exception as exc:
            print(f"[games] duel round start failed: {exc}")
            await settle_duel(self.duel_id, None, "start_failed")
            return await interaction.followup.send("❌ The round failed to start; both wagers were refunded.", ephemeral=True)
        task = asyncio.create_task(games_duel_timeout_after(self.duel_id, duel["round_started"]))
        ACTIVE_DUEL_TIMEOUT_TASKS[self.duel_id] = task

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass
        if interaction.user.id != self.target_id:
            return await interaction.followup.send("Only the challenged player can decline.", ephemeral=True)
        duel = ACTIVE_DUELS.pop(self.duel_id, None)
        if duel:
            duel["state"] = "declined"
        self.stop()
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE mcwv_duels SET state = 'declined' WHERE id = %s", (int(self.duel_id),))
            conn.commit()
        except Exception:
            pass
        try:
            await interaction.edit_original_response(content="\ud83d\ude45 Duel declined.", view=None)
        except Exception:
            await interaction.followup.send("\ud83d\ude45 Duel declined.", ephemeral=True)


async def start_duel_round(channel, duel):
    """Shared live challenge for both duelists — first correct answer wins the pot."""
    game_type = duel["game_type"]
    a_id, b_id = duel["challenger"], duel["target"]
    duel["answers"] = {a_id: 0, b_id: 0}
    duel["locked_until"] = {}
    duel["round_started"] = time.time()

    if game_type == "guess":
        pets = games_guess_pet_pool()
        pet = secrets.choice(pets)
        icon = await games_fetch_pet_icon(games_pet_asset(pet))
        if icon:
            buf = games_build_round_image(icon, "zoom")
            if buf:
                duel["answer"] = pet
                embed = discord.Embed(
                    title="⚔️ DUEL — Guess the Pet",
                    description=f"<@{a_id}> vs <@{b_id}> · first to name this pet wins **{duel['wager'] * 2:,}** 🪙!",
                    color=games_color("pink"),
                )
                embed.add_field(name="Pot", value=f"`{duel['wager']}` + `{duel['wager']}`", inline=True)
                embed.add_field(name="Time limit", value=f"{GAMES_DUEL_TIMEOUT}s", inline=True)
                games_footer(embed, "Just type the name in chat")
                await channel.send(embed=embed, file=discord.File(buf, filename="duel.png"))
                return
        # image unavailable → clean fallback to scramble (consistent game_type)
        duel["game_type"] = "scramble"
    elif game_type == "existcount":
        ep = games_random_exist_pet()
        pet = ep["name"] if ep else None
        exist_count = ep["exist_count"] if ep else 0
        if pet and exist_count > 0:
            duel["answer"] = pet
            duel["exist_count"] = exist_count
            duel["exist_guesses"] = {}
            icon = await games_fetch_pet_icon(games_pet_asset(pet))
            file = None
            if icon:
                buf = games_build_round_image(icon, "zoom")
                if buf:
                    file = discord.File(buf, filename="duel.png")
            embed = discord.Embed(
                title="🔢 DUEL — Exist Count",
                description=f"<@{a_id}> vs <@{b_id}> · closest guess to this pet's **real exist count** wins **{duel['wager'] * 2:,}** 🪙!",
                color=games_color("blue"),
            )
            embed.add_field(name="Pot", value=f"`{duel['wager']}` + `{duel['wager']}`", inline=True)
            embed.add_field(name="Time limit", value=f"{GAMES_DUEL_TIMEOUT}s", inline=True)
            games_footer(embed, "Type your number guess — e.g. 42000")
            await channel.send(embed=embed, file=file)
            return
    # fallback: scramble
    pool = games_random_word_pool()
    word = games_pick_random(pool, scope=f"duel_word:{duel.get('id')}", max_recent=3)
    letters = list(re.sub(r"[^A-Za-z]", "", word).lower())
    shuffled = letters[:]
    while len(shuffled) > 1 and shuffled == letters:
        secrets.SystemRandom().shuffle(shuffled)
    duel["answer"] = word
    embed = discord.Embed(
        title="🔀 DUEL — Scramble",
        description=f"<@{a_id}> vs <@{b_id}> · first to unscramble the pet name wins **{duel['wager'] * 2:,}** 🪙!",
        color=games_color("amber"),
    )
    embed.add_field(name="Letters", value=f"`{' '.join(shuffled).upper()}`", inline=False)
    embed.add_field(name="Pot", value=f"`{duel['wager']}` + `{duel['wager']}`", inline=True)
    embed.add_field(name="Time limit", value=f"{GAMES_DUEL_TIMEOUT}s", inline=True)
    await channel.send(embed=embed)


async def games_duel_timeout_after(duel_id, round_started):
    """Settle an unanswered active duel even if nobody sends another message."""
    try:
        await asyncio.sleep(GAMES_DUEL_TIMEOUT)
        duel = ACTIVE_DUELS.get(int(duel_id))
        if not duel or duel.get("state") != "active":
            return
        if float(duel.get("round_started", 0)) != float(round_started):
            return
        await settle_duel(int(duel_id), None, "timeout")
    except asyncio.CancelledError:
        return
    except Exception as exc:
        print(f"[games] scheduled duel timeout failed: {exc}")
    finally:
        task = ACTIVE_DUEL_TIMEOUT_TASKS.get(int(duel_id))
        if task is asyncio.current_task():
            ACTIVE_DUEL_TIMEOUT_TASKS.pop(int(duel_id), None)


async def games_handle_duel_answer(message):
    for duel_id, duel in list(ACTIVE_DUELS.items()):
        if duel.get("state") != "active":
            continue
        if message.channel.id != duel.get("channel_id"):
            continue
        if message.author.id not in (duel["challenger"], duel["target"]):
            continue
        if message.author.bot:
            continue
        if time.time() - float(duel.get("round_started", 0)) > GAMES_DUEL_TIMEOUT:
            await settle_duel(duel_id, None, "timeout")
            return True
        if duel.get("game_type") == "existcount":
            guesses = duel.setdefault("exist_guesses", {})
            if message.author.id in guesses:
                continue  # FIRST guess locks — later spam can't rewrite it
            try:
                guess = int(re.sub(r"[^0-9]", "", message.content or "") or -1)
            except Exception:
                guess = -1
            if guess < 0:
                continue
            guesses[message.author.id] = guess
            if len(duel["exist_guesses"]) >= 2:
                target = int(duel.get("exist_count", 0))
                g1 = duel["exist_guesses"].get(duel["challenger"], -1)
                g2 = duel["exist_guesses"].get(duel["target"], -1)
                if g1 < 0 and g2 < 0:
                    await settle_duel(duel_id, None, "timeout")
                elif g1 < 0:
                    await settle_duel(duel_id, duel["target"], "won")
                elif g2 < 0:
                    await settle_duel(duel_id, duel["challenger"], "won")
                else:
                    d1, d2 = abs(g1 - target), abs(g2 - target)
                    winner = secrets.choice([duel["challenger"], duel["target"]]) if d1 == d2 \
                        else (duel["challenger"] if d1 < d2 else duel["target"])
                    await settle_duel(duel_id, winner, "won")
            return True
        locked = duel.get("locked_until", {}).get(message.author.id, 0)
        if time.time() < locked:
            return True
        if not games_answers_match(message.content, duel.get("answer", "")):
            duel["locked_until"][message.author.id] = time.time() + 5
            return True
        await settle_duel(duel_id, message.author.id, "won")
        return True
    return False


async def settle_duel(duel_id, winner_id, reason):
    duel_id = int(duel_id)
    timeout_task = ACTIVE_DUEL_TIMEOUT_TASKS.pop(duel_id, None)
    if timeout_task is not None and timeout_task is not asyncio.current_task():
        timeout_task.cancel()
    duel = ACTIVE_DUELS.get(duel_id)
    if not duel or duel.get("state") not in ("active", "settling"):
        return
    duel["state"] = "settling"  # blocks two simultaneous correct messages
    ok, result = games_duel_settle_db(duel, winner_id if winner_id else None)
    if not ok:
        print(f"[games] duel {duel_id} settlement deferred: {result}")
        duel["state"] = "active"
        # A fallback retry becomes a refund rather than leaving funds locked.
        task = asyncio.create_task(games_duel_timeout_after(duel_id, duel.get("round_started", 0)))
        ACTIVE_DUEL_TIMEOUT_TASKS[duel_id] = task
        return
    ACTIVE_DUELS.pop(duel_id, None)
    channel = bot.get_channel(int(duel.get("channel_id", 0)))
    if winner_id:
        winner_id = int(winner_id)
        pot = int(result)
        # The pot was escrowed from both players: this is circulation, not minting.
        games_track("duel", duel.get("channel_id", 0))
        games_track_user("duel", winner_id, win=True)
        loser_id = duel["target"] if winner_id == int(duel["challenger"]) else duel["challenger"]
        games_track_user("duel", loser_id, win=False)
        try:
            guild = bot.get_guild(GUILD_ID)
            if guild:
                await games_check_duelist_role(guild, winner_id)
        except Exception as exc:
            print(f"[games] duelist role check failed: {exc}")
        if channel:
            if duel.get("game_type") == "existcount" and duel.get("answer"):
                answer_txt = f" — **{duel.get('answer')}** exists **{int(duel.get('exist_count', 0)):,}** times in game!"
            else:
                answer_txt = f" — it was **{duel.get('answer', '?')}**" if duel.get("answer") else ""
            embed = discord.Embed(
                title="🏆 Duel Won!",
                description=f"<@{winner_id}> takes the pot{answer_txt} — **+{pot:,}** 🪙!",
                color=games_color("gold"),
            )
            embed.add_field(name="Winner", value=f"<@{winner_id}>", inline=True)
            embed.add_field(name="Pot", value=f"`{pot:,}` 🪙", inline=True)
            await channel.send(embed=embed)
    elif channel:
        embed = discord.Embed(
            title="⏰ Duel Timed Out",
            description="Both wagers were refunded — rematch anytime with `/duel`.",
            color=games_color("slate"),
        )
        if duel.get("answer"):
            embed.add_field(name="Answer was", value=f"**{duel.get('answer')}**")
        await channel.send(embed=embed)


@bot.tree.command(name="duel", description="Challenge someone to a live 1v1 for a coin wager", guild=guild_obj)
@app_commands.describe(user="Who to challenge", wager="Coins on the line")
async def games_duel(interaction: discord.Interaction, user: discord.User, wager: int):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer()
    wager = int(wager)
    if wager < GAMES_DUEL_MIN_WAGER:
        return await interaction.followup.send(f"❌ Minimum wager is **{GAMES_DUEL_MIN_WAGER}** coins.", ephemeral=True)
    if wager > GAMES_MAX_TRANSACTION:
        return await interaction.followup.send(f"❌ Maximum wager is **{GAMES_MAX_TRANSACTION:,}** coins.", ephemeral=True)
    if user.id == interaction.user.id:
        return await interaction.followup.send("❌ You can't duel yourself!", ephemeral=True)
    if user.bot:
        return await interaction.followup.send("❌ Bots don't duel.", ephemeral=True)
    for d in ACTIVE_DUELS.values():
        if d.get("state") not in ("pending", "active"):
            continue
        participants = (d["challenger"], d["target"])
        if interaction.user.id in participants:
            return await interaction.followup.send("❌ You already have an active duel.", ephemeral=True)
        if user.id in participants:
            return await interaction.followup.send("❌ That player already has an active duel.", ephemeral=True)
    game_type = secrets.choice(["guess", "scramble", "existcount"])
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcwv_duels (challenger, target, wager, game_type, state) VALUES (%s,%s,%s,%s,'pending') RETURNING id",
                (interaction.user.id, user.id, wager, game_type))
            duel_id = int(cur.fetchone()[0])
        conn.commit()
    except Exception as exc:
        return await interaction.followup.send(f"❌ Could not create duel: `{type(exc).__name__}`", ephemeral=True)
    ACTIVE_DUELS[duel_id] = {
        "id": duel_id, "challenger": interaction.user.id, "target": user.id,
        "wager": wager, "game_type": game_type, "state": "pending", "escrowed": False,
        "channel_id": interaction.channel.id,
    }
    view = DuelChallengeView(duel_id, user.id, interaction.user.id, wager, game_type)
    game_label = {'guess': '🔍 Guess the Pet', 'existcount': '🔢 Exist Count', 'scramble': '🔀 Scramble'}.get(game_type, game_type)
    duel_embed = discord.Embed(
        title="⚔️ Duel Challenge",
        description=(
            f"**{interaction.user.mention}** challenges **{user.mention}**!\n\n"
            f"**Game:** {game_label} (bot-picked)\n"
            f"**Wager:** 🪙 **{wager:,}** each\n"
            f"**Pot:** 🪙 **{wager * 2:,}**\n\n"
            "First correct answer takes the pot. Accept to lock your wager!"
        ),
        color=games_color("pink"),
    )
    av1 = getattr(interaction.user, "display_avatar", None) or getattr(interaction.user, "avatar", None)
    if av1 is not None:
        duel_embed.set_thumbnail(url=av1.url)
    duel_embed.set_footer(text="Declining is free · no answer in 45s = full refund")
    view.message = await interaction.followup.send(embed=duel_embed, view=view, wait=True)


# ---------------- TRIVIA ----------------


# ---------------- SCRAMBLE ----------------

@bot.tree.command(name="scramble", description="Unscramble a pet name — first correct wins 100 coins", guild=guild_obj)
async def games_scramble(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer()
    if interaction.channel.id in ACTIVE_SCRAMBLE:
        return await interaction.followup.send("❌ A scramble is already active in this channel.", ephemeral=True)
    allowed, retry_at = (True, None) if games_is_unlimited(interaction.user.id) else games_cooldown_claim(
        interaction.channel.id, "scramble_channel", GAMES_SCRAMBLE_CHANNEL_COOLDOWN
    )
    if not allowed:
        retry = discord.utils.format_dt(retry_at, "R") if retry_at else "in a moment"
        return await interaction.followup.send(f"⏳ This channel can start another scramble {retry}.", ephemeral=True)
    pool = games_random_word_pool()
    word = games_pick_random(pool, scope=f"scramble:{interaction.channel.id}", max_recent=10)
    letters = list(re.sub(r"[^A-Za-z]", "", word).lower())
    shuffled = letters[:]
    while len(shuffled) > 1 and shuffled == letters:
        secrets.SystemRandom().shuffle(shuffled)
    ACTIVE_SCRAMBLE[interaction.channel.id] = {
        "answer": word, "started": time.time(), "attempts": {}, "participants": set()
    }
    embed = discord.Embed(
        title="🔀 Scramble",
        description="Unscramble this pet name — first correct wins **`100` 🪙**!",
        color=games_color("amber"),
    )
    embed.add_field(name="Letters", value=f"`{' '.join(shuffled).upper()}`", inline=False)
    embed.add_field(name="Prize", value="`100` 🪙", inline=True)
    embed.add_field(name="Timer", value=f"`{GAMES_ROUND_TIMEOUT // 60}` min", inline=True)
    games_footer(embed, "Type the answer in chat")
    await interaction.followup.send(embed=embed)


async def games_handle_scramble(message):
    s = ACTIVE_SCRAMBLE.get(message.channel.id)
    if not s or message.author.bot:
        return False
    if time.time() - float(s["started"]) > GAMES_ROUND_TIMEOUT:
        ACTIVE_SCRAMBLE.pop(message.channel.id, None)
        return False
    # Ignore normal channel chat; only same-letter candidate answers consume an attempt.
    candidate = normalize_answer(message.content)
    answer_letters = normalize_answer(s["answer"])
    if not candidate or sorted(candidate) != sorted(answer_letters):
        return False
    attempts = s.setdefault("attempts", {}).get(message.author.id, 0)
    if attempts >= GAMES_SCRAMBLE_MAX_ATTEMPTS:
        return False
    s["attempts"][message.author.id] = attempts + 1
    s.setdefault("participants", set()).add(message.author.id)
    if games_answers_match(message.content, s["answer"]):
        ACTIVE_SCRAMBLE.pop(message.channel.id, None)
        games_coin_adjust(message.author.id, 100, "scramble_win", meta={"word": s["answer"]})
        games_track("scramble", message.channel.id, minted=100)
        games_track_participants("scramble", s.get("participants"), winner_id=message.author.id)
        embed = discord.Embed(
            title="🎉 Scramble Solved!",
            description=f"**{getattr(message.author, 'mention', '')}** unscrambled it — **{s['answer']}**!",
            color=games_color("green"),
        )
        embed.add_field(name="Reward", value="+`100` 🪙", inline=True)
        embed.add_field(name="Attempts", value=f"**{attempts + 1}/{GAMES_SCRAMBLE_MAX_ATTEMPTS}**", inline=True)
        await message.channel.send(embed=embed)
        return True
    return False


# ---------------- HANGMAN ----------------

@bot.tree.command(name="hangman", description="Hangman with pet names — 6 lives", guild=guild_obj)
async def games_hangman(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer()
    if interaction.channel.id in ACTIVE_HANGMAN:
        return await interaction.followup.send("❌ A hangman game is already active here.", ephemeral=True)
    allowed, retry_at = (True, None) if games_is_unlimited(interaction.user.id) else games_cooldown_claim(
        interaction.channel.id, "hangman_channel", GAMES_HANGMAN_CHANNEL_COOLDOWN
    )
    if not allowed:
        retry = discord.utils.format_dt(retry_at, "R") if retry_at else "in a moment"
        return await interaction.followup.send(f"⏳ This channel can start another hangman game {retry}.", ephemeral=True)
    pool = games_random_word_pool()
    word = games_pick_random(pool, scope=f"hangman:{interaction.channel.id}", max_recent=8).lower()
    core = re.sub(r"[^a-z]", "", word)
    ACTIVE_HANGMAN[interaction.channel.id] = {
        "word": word, "core": core, "guessed": set(), "wrong": 0, "started": time.time(),
        "full_guesses": {}, "participants": set(),
    }
    msg = await interaction.followup.send(embed=render_hangman_embed(interaction.channel.id))
    ACTIVE_HANGMAN[interaction.channel.id]["msg"] = msg


GAMES_HANGMAN_ART = [
    "```\n\n   ┌───┐\n   │\n   │\n   │\n  ─┴────\n```",
    "```\n\n   ┌───┐\n   │   😬\n   │\n   │\n  ─┴────\n```",
    "```\n\n   ┌───┐\n   │   😟\n   │   │\n   │\n  ─┴────\n```",
    "```\n\n   ┌───┐\n   │   😰\n   │  ─│\n   │\n  ─┴────\n```",
    "```\n\n   ┌───┐\n   │   😱\n   │  ─│─\n   │\n  ─┴────\n```",
    "```\n\n   ┌───┐\n   │   😵\n   │  ─│─\n   │   ┌┐\n  ─┴────\n```",
    "```\n\n   ┌───┐\n   │   💀\n   │  ─│─\n   │  ─┴─\n  ─┴────\n```",
]


def render_hangman_embed(channel_id):
    g = ACTIVE_HANGMAN.get(channel_id)
    if not g:
        return discord.Embed(title="🎩 Hangman", description="No active game.", color=games_color("slate"))
    display = " ".join(
        ch.upper() if (not ch.isalpha() or ch in g["guessed"]) else "_"
        for ch in g["word"]
    )
    lives = 6 - g["wrong"]
    art = GAMES_HANGMAN_ART[min(g["wrong"], 6)]
    guessed_txt = " ".join(sorted(g["guessed"])) if g["guessed"] else "none yet"
    embed = discord.Embed(
        title=f"🎩 Hangman · {len(g['core'])} letters",
        description=art + f"\n`{display}`",
        color=games_color("green") if lives > 2 else games_color("red"),
    )
    embed.add_field(name="Lives", value=games_hearts(lives, 6), inline=True)
    embed.add_field(name="Guessed", value=f"`{guessed_txt[:40]}`", inline=True)
    games_footer(embed, "Type a letter or guess the full name · +150 🪙")
    return embed


async def _hangman_update(g, embed):
    """Edit the game message in place; falls back to a new message."""
    try:
        msg = g.get("msg")
        if msg is not None:
            await msg.edit(embed=embed)
            return
    except Exception as exc:
        print(f"[games] hangman edit failed: {exc}")
    g["fallback"] = True


async def games_handle_hangman(message):
    g = ACTIVE_HANGMAN.get(message.channel.id)
    if not g or message.author.bot:
        return False
    if time.time() - float(g["started"]) > 300:
        ACTIVE_HANGMAN.pop(message.channel.id, None)
        return False
    content = message.content.strip().lower()

    async def finish(winner=None):
        ACTIVE_HANGMAN.pop(message.channel.id, None)
        games_track_participants("hangman", g.get("participants"), winner_id=getattr(winner, "id", None))
        if winner is None:
            e = discord.Embed(
                title="💀 Hangman Lost!",
                description=f"The pet was **{g['word']}**.",
                color=games_color("red"),
            )
            e.set_footer(text="Run /hangman to try again")
        else:
            games_coin_adjust(winner.id, 150, "hangman_win", meta={"word": g["word"]})
            games_track("hangman", message.channel.id, minted=150)
            e = discord.Embed(
                title="🎉 Hangman Solved!",
                description=f"**{getattr(winner, 'mention', '')}** solved it — **{g['word']}**!",
                color=games_color("green"),
            )
            e.add_field(name="Reward", value="+`150` 🪙", inline=True)
        await _hangman_update(g, e)

    if len(content) == 1 and content.isalpha():
        g.setdefault("participants", set()).add(message.author.id)
        if content in g["guessed"]:
            return False
        g["guessed"].add(content)
        if content not in g["core"]:
            g["wrong"] += 1
            if g["wrong"] >= 6:
                await finish()
                return True
        elif all(ch in g["guessed"] for ch in g["core"]):
            await finish(message.author)
            return True
        await _hangman_update(g, render_hangman_embed(message.channel.id))
        return True

    if len(content) > 1:
        if games_answers_match(content, g["word"]):
            g.setdefault("participants", set()).add(message.author.id)
            await finish(message.author)
            return True
        # Only pet-name-shaped guesses count. Ordinary channel chat is ignored.
        known = {normalize_answer(p) for p in games_random_word_pool()}
        if normalize_answer(content) not in known:
            return False
        g.setdefault("participants", set()).add(message.author.id)
        used = g.setdefault("full_guesses", {}).get(message.author.id, 0)
        if used >= GAMES_HANGMAN_MAX_FULL_GUESSES:
            return False
        g["full_guesses"][message.author.id] = used + 1
        g["wrong"] += 1
        if g["wrong"] >= 6:
            await finish()
        else:
            await _hangman_update(g, render_hangman_embed(message.channel.id))
        return True
    return False


# ---------------- PETDLE (Wordle) ----------------

def games_petdle_target():
    """Return a stable target for the UTC day, even if the pet sync changes."""
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"games_petdle_target_{day}"
    pool = sorted({
        p["name"] for p in games_get_pets()
        if 4 <= len(normalize_answer(p.get("name"))) <= 16
    } or {
        name for name in GAMES_PET_SEED
        if 4 <= len(normalize_answer(name)) <= 16
    })
    digest = int(hashlib.sha256(f"MCWV:{day}".encode()).hexdigest(), 16)
    candidate = pool[digest % len(pool)]
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
                (key, candidate),
            )
            cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
            row = cur.fetchone()
        return str(row[0]) if row and row[0] else candidate
    except Exception:
        return candidate


def games_wordle_feedback(guess, target):
    """Wordle scoring with duplicate-letter accounting."""
    guess, target = normalize_answer(guess), normalize_answer(target)
    marks = ["⬛"] * len(guess)
    remaining = {}
    for i, ch in enumerate(target):
        if i < len(guess) and guess[i] == ch:
            marks[i] = "🟩"
        else:
            remaining[ch] = remaining.get(ch, 0) + 1
    for i, ch in enumerate(guess):
        if marks[i] == "🟩":
            continue
        if remaining.get(ch, 0) > 0:
            marks[i] = "🟨"
            remaining[ch] -= 1
    return "".join(marks)


def games_petdle_submit(user_id, guess, target):
    """Persist a guess and award a solve in the same transaction."""
    uid = int(user_id)
    day = datetime.now(timezone.utc).date()
    clean = normalize_answer(guess)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO mcwv_petdle_progress (discord_id, day)
                       VALUES (%s, %s) ON CONFLICT (discord_id, day) DO NOTHING""",
                    (uid, day),
                )
                cur.execute(
                    """SELECT guesses, solved FROM mcwv_petdle_progress
                       WHERE discord_id = %s AND day = %s FOR UPDATE""",
                    (uid, day),
                )
                row = cur.fetchone()
                guesses = row[0] if row and isinstance(row[0], list) else json.loads((row[0] if row else None) or "[]")
                solved = bool(row and row[1])
                if solved:
                    return {"status": "solved", "guesses": guesses, "awarded": False}
                if len(guesses) >= GAMES_PETDLE_MAX_GUESSES:
                    return {"status": "lost", "guesses": guesses, "awarded": False}
                guesses.append(clean)
                solved_now = clean == normalize_answer(target)
                cur.execute(
                    """UPDATE mcwv_petdle_progress SET guesses = %s::jsonb, solved = %s
                       WHERE discord_id = %s AND day = %s""",
                    (json.dumps(guesses), solved_now, uid, day),
                )
                if solved_now:
                    cur.execute(
                        "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                        (uid,),
                    )
                    cur.execute(
                        """UPDATE mcwv_coins
                           SET balance = balance + 200, total_earned = total_earned + 200,
                               last_petdle_at = NOW()
                           WHERE discord_id = %s RETURNING balance""",
                        (uid,),
                    )
                    balance_after = int(cur.fetchone()[0])
                    cur.execute(
                        """INSERT INTO mcwv_coin_log
                           (actor_id, target_id, type, amount, balance_after, meta)
                           VALUES (%s,%s,'petdle_win',200,%s,%s::jsonb)""",
                        (uid, uid, balance_after, json.dumps({"word": target, "day": str(day)})),
                    )
        status = "won" if solved_now else ("lost" if len(guesses) >= GAMES_PETDLE_MAX_GUESSES else "playing")
        return {"status": status, "guesses": guesses, "awarded": solved_now}
    except Exception as exc:
        print(f"[games] petdle submit failed: {exc}")
        return {"status": "error", "guesses": [], "awarded": False}


def games_petdle_board(guesses, target):
    rows = []
    for old_guess in guesses:
        rows.append(f"`{' '.join(str(old_guess).upper())}`\n{games_wordle_feedback(old_guess, target)}")
    return "\n".join(rows) if rows else "No guesses yet."


@bot.tree.command(name="petdle", description="Daily pet-name Wordle — six guesses", guild=guild_obj)
@app_commands.describe(guess="Your guess")
async def games_petdle(interaction: discord.Interaction, guess: str = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    target = games_petdle_target()
    target_core = normalize_answer(target)

    if guess is None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT guesses, solved FROM mcwv_petdle_progress WHERE discord_id = %s AND day = CURRENT_DATE",
                    (interaction.user.id,),
                )
                row = cur.fetchone()
            guesses = row[0] if row and isinstance(row[0], list) else json.loads((row[0] if row else None) or "[]")
            solved = bool(row and row[1])
        except Exception:
            guesses, solved = [], False
        embed = discord.Embed(
            title="🐾 Petdle — Daily Puzzle",
            description=games_petdle_board(guesses, target),
            color=games_color("green" if solved else "purple"),
        )
        embed.add_field(name="Target", value=f"**{len(target_core)} letters**", inline=True)
        embed.add_field(name="Attempts", value=f"**{len(guesses)}/{GAMES_PETDLE_MAX_GUESSES}**", inline=True)
        embed.add_field(name="How to play", value="`/petdle <guess>` · 🟩 exact · 🟨 misplaced · ⬛ absent", inline=False)
        if solved:
            embed.add_field(name="Solved", value=f"✅ **{target}**", inline=False)
        games_footer(embed, "Resets at 00:00 UTC · solve for +200 🪙")
        return await interaction.followup.send(embed=embed, ephemeral=True)

    guess_clean = normalize_answer(guess)
    if not guess_clean:
        return await interaction.followup.send("❌ Invalid guess.", ephemeral=True)
    if len(guess_clean) != len(target_core):
        return await interaction.followup.send(
            f"❌ Today's answer has **{len(target_core)} letters** — your guess has {len(guess_clean)}.",
            ephemeral=True,
        )

    result = games_petdle_submit(interaction.user.id, guess_clean, target)
    if result["status"] == "error":
        return await interaction.followup.send("❌ Petdle couldn't save that guess. Try again.", ephemeral=True)

    guesses = result["guesses"]
    status = result["status"]
    won = status in ("won", "solved")
    lost = status == "lost"
    embed = discord.Embed(
        title="🎉 Petdle Solved!" if won else ("💔 Petdle Over" if lost else "🐾 Petdle"),
        description=games_petdle_board(guesses, target),
        color=games_color("green" if won else "red" if lost else "purple"),
    )
    embed.add_field(name="Attempts", value=f"**{len(guesses)}/{GAMES_PETDLE_MAX_GUESSES}**", inline=True)
    if won:
        embed.add_field(name="Answer", value=f"**{target}**", inline=True)
        embed.add_field(name="Reward", value="+`200` 🪙" if result["awarded"] else "Already claimed ✅", inline=True)
        if result["awarded"]:
            games_track("petdle", interaction.channel_id, minted=200)
            games_track_user("petdle", interaction.user.id, win=True)
    elif lost:
        embed.add_field(name="Answer", value=f"**{target}**", inline=True)
        games_track_user("petdle", interaction.user.id, win=False)
    elif len(guesses) >= 3:
        embed.add_field(name="💡 Hint", value=f"Starts with **{target_core[0].upper()}**", inline=True)
    games_footer(embed, "New puzzle at 00:00 UTC")

    file = None
    if won or lost:
        asset = games_pet_asset(target)
        icon = await games_fetch_pet_icon(asset) if asset else None
        file = games_icon_file(icon, "petdle.png", size=128) if icon else None
        if file is None:
            ph = games_build_pet_placeholder(target, "common", size=128)
            if ph:
                file = discord.File(ph, filename="petdle.png")
        if file:
            embed.set_thumbnail(url="attachment://petdle.png")
    await interaction.followup.send(embed=embed, file=file, ephemeral=True)


# ---------------- SPIN WHEEL (progressive jackpot) ----------------



def games_jackpot_get():
    try:
        return int(db_get_setting(GAMES_SETTING_JACKPOT, db_get_setting(GAMES_SETTING_JACKPOT_SEED, "5000")) or 0)
    except Exception:
        return 5000


def games_jackpot_set(value):
    db_set_setting(GAMES_SETTING_JACKPOT, str(int(value)))


def games_spin_settle(user_id, label, fixed_amount, is_jackpot, jackpot_inc):
    """Settle a spin and mutate the progressive jackpot atomically."""
    uid = int(user_id)
    try:
        with conn:
            with conn.cursor() as cur:
                seed = int(db_get_setting(GAMES_SETTING_JACKPOT_SEED, "5000") or 5000)
                cur.execute(
                    "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
                    (GAMES_SETTING_JACKPOT, str(seed)),
                )
                cur.execute("SELECT value FROM settings WHERE key = %s FOR UPDATE", (GAMES_SETTING_JACKPOT,))
                row = cur.fetchone()
                try:
                    jackpot_before = max(0, int(row[0] if row else seed))
                except Exception:
                    jackpot_before = seed
                amount = jackpot_before if is_jackpot else max(0, int(fixed_amount or 0))
                item = "scratch" if label == "Free scratch" else None
                jackpot_after = seed if is_jackpot else jackpot_before + max(0, int(jackpot_inc))
                cur.execute("UPDATE settings SET value = %s WHERE key = %s", (str(jackpot_after), GAMES_SETTING_JACKPOT))
                cur.execute(
                    "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                    (uid,),
                )
                if amount > 0:
                    kind = "spin_jackpot" if is_jackpot else "spin_win"
                    cur.execute(
                        """UPDATE mcwv_coins
                           SET balance = balance + %s, total_earned = total_earned + %s
                           WHERE discord_id = %s RETURNING balance""",
                        (amount, amount, uid),
                    )
                    balance_after = int(cur.fetchone()[0])
                    cur.execute(
                        """INSERT INTO mcwv_coin_log
                           (actor_id, target_id, type, amount, balance_after, meta)
                           VALUES (%s,%s,%s,%s,%s,%s::jsonb)""",
                        (uid, uid, kind, amount, balance_after,
                         json.dumps({"label": label, "jackpot_before": jackpot_before})),
                    )
                elif item == "scratch":
                    cur.execute(
                        "UPDATE mcwv_coins SET prepaid_scratches = prepaid_scratches + 1 WHERE discord_id = %s RETURNING balance",
                        (uid,),
                    )
                    balance_after = int(cur.fetchone()[0])
                    cur.execute(
                        """INSERT INTO mcwv_coin_log
                           (actor_id, target_id, type, amount, balance_after, meta)
                           VALUES (%s,%s,'spin_item',0,%s,%s::jsonb)""",
                        (uid, uid, balance_after, json.dumps({"item": "scratch", "quantity": 1})),
                    )
        return {"ok": True, "amount": amount, "item": item, "jackpot": jackpot_after}
    except Exception as exc:
        print(f"[games] spin settlement failed: {exc}")
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def games_spin_sectors():
    """(label, fixed_amount, is_jackpot, color) — angles are weight-proportional."""
    return [
        ("100 coins", 100, False, (59, 130, 246)),
        ("250 coins", 250, False, (99, 102, 241)),
        ("500 coins", 500, False, (168, 85, 247)),
        ("1,000 coins", 1000, False, (139, 92, 246)),
        ("Free scratch", 0, False, (14, 165, 233)),
        ("Nothing", 0, False, (100, 116, 139)),
        ("JACKPOT", 0, True, (245, 158, 11)),
    ]


def games_spin_weights():
    return [30, 22, 12, 6, 10, 19, 1]


def games_spin_roll():
    """Returns (sector_idx, label, amount, is_jackpot)."""
    idx = games_weighted_choice(range(len(games_spin_sectors())), games_spin_weights())
    label, amount, is_jackpot, _ = games_spin_sectors()[idx]
    if is_jackpot:
        amount = games_jackpot_get()
    return idx, label, amount, is_jackpot


def _games_wheel_render(win_idx, spin_degrees=0.0):
    """Render the wheel at 2x resolution, then downscale (anti-aliased)."""
    import math as _math
    SIZE = 480
    S = SIZE * 2
    cx = cy = S // 2
    R = S // 2 - 36
    sectors = games_spin_sectors()
    weights = games_spin_weights()
    total_w = float(sum(weights))
    cum = 0.0
    win_mid = 0.0
    angles = []
    for i, w in enumerate(weights):
        start = cum
        sweep = w / total_w * 360.0
        angles.append((start, sweep))
        if i == win_idx:
            win_mid = start + sweep / 2.0
        cum += sweep
    rotation = 270.0 - win_mid + spin_degrees  # top = 270 deg in PIL clockwise convention

    # canvas + drop shadow
    img = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    shadow = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    ImageDraw.Draw(shadow).ellipse((cx - R + 10, cy - R + 14, cx + R + 10, cy + R + 14), fill=(0, 0, 0, 110))
    shadow = shadow.filter(ImageFilter.GaussianBlur(12))
    img.alpha_composite(shadow)

    d = ImageDraw.Draw(img)

    def shade(color, factor):
        return tuple(max(0, min(255, int(c * factor))) for c in color)

    # sectors: base fill + outer darker band + inner lighter band (depth)
    for i, (start, sweep) in enumerate(angles):
        _, _, _, color = sectors[i]
        a0 = start + rotation
        a1 = a0 + sweep
        d.pieslice((cx - R, cy - R, cx + R, cy + R), a0, a1, fill=color)
        # outer shade band
        band = Image.new("RGBA", (S, S), (0, 0, 0, 0))
        bd = ImageDraw.Draw(band)
        bd.pieslice((cx - R, cy - R, cx + R, cy + R), a0, a1, fill=shade(color, 0.72) + (255,))
        mask = Image.new("L", (S, S), 0)
        ImageDraw.Draw(mask).ellipse((cx - R, cy - R, cx + R, cy + R), fill=255)
        ring = Image.new("L", (S, S), 0)
        ImageDraw.Draw(ring).ellipse((cx - int(R * 0.72), cy - int(R * 0.72), cx + int(R * 0.72), cy + int(R * 0.72)), fill=255)
        band.putalpha(ImageChops.subtract(mask, ring))
        img.alpha_composite(band)

    # jackpot sector glow
    for i, (start, sweep) in enumerate(angles):
        if sectors[i][2]:
            glow = Image.new("RGBA", (S, S), (0, 0, 0, 0))
            gd = ImageDraw.Draw(glow)
            a0 = start + rotation
            a1 = a0 + sweep
            gd.pieslice((cx - R, cy - R, cx + R, cy + R), a0, a1, fill=(255, 213, 79, 90))
            glow = glow.filter(ImageFilter.GaussianBlur(10))
            img.alpha_composite(glow)

    # sector divider spokes
    for i, (start, _sweep) in enumerate(angles):
        a = _math.radians(start + rotation)
        d.line(
            (cx + _math.cos(a) * R * 0.30, cy + _math.sin(a) * R * 0.30,
             cx + _math.cos(a) * R, cy + _math.sin(a) * R),
            fill=(12, 14, 26, 150), width=4,
        )

    # labels (radial-out, rotated with the wheel — RGBA layers, composited)
    # PIL y-down coords: rotate(phi) maps the reading direction (1,0) -> (cos phi, -sin phi).
    # A label at wheel angle mid must read outward: (cos mid, sin mid) => phi = -mid.
    try:
        label_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 40)
    except Exception:
        label_font = ImageFont.load_default()
    for i, (start, sweep) in enumerate(angles):
        label = sectors[i][0]
        if i == win_idx and sectors[i][2]:
            label = "JACKPOT!"
        mid = start + sweep / 2.0 + rotation
        rad = _math.radians(mid)
        lx = cx + _math.cos(rad) * R * 0.60
        ly = cy + _math.sin(rad) * R * 0.60
        # size the layer to the actual text so nothing ever clips
        probe = Image.new("RGBA", (4, 4), (0, 0, 0, 0))
        pd = ImageDraw.Draw(probe)
        bb = pd.textbbox((0, 0), label, font=label_font)
        tw, th = bb[2] - bb[0], bb[3] - bb[1]
        pad = 14
        txt = Image.new("RGBA", (tw + pad * 2, th + pad * 2), (0, 0, 0, 0))
        td = ImageDraw.Draw(txt)
        ox, oy = pad - bb[0], pad - bb[1]
        # dark outline for contrast on any sector colour
        for dx, dy in ((2, 2), (-2, 2), (2, -2), (-2, -2), (2, 0), (-2, 0), (0, 2), (0, -2)):
            td.text((ox + dx, oy + dy), label, font=label_font, fill=(10, 10, 18, 150))
        td.text((ox, oy), label, font=label_font, fill=(255, 255, 255, 255))
        txt = txt.rotate(-mid, expand=True, resample=Image.Resampling.BICUBIC)
        img.alpha_composite(txt, (int(lx - txt.width / 2), int(ly - txt.height / 2)))

    # metallic rim
    d.ellipse((cx - R, cy - R, cx + R, cy + R), outline=(12, 14, 26, 255), width=18)
    d.ellipse((cx - R + 9, cy - R + 9, cx + R - 9, cy + R - 9), outline=(255, 214, 90, 255), width=6)
    # tick marks (every 12 degrees)
    for deg in range(0, 360, 12):
        a = _math.radians(deg)
        x1 = cx + _math.cos(a) * (R - 30)
        y1 = cy + _math.sin(a) * (R - 30)
        x2 = cx + _math.cos(a) * (R - 46)
        y2 = cy + _math.sin(a) * (R - 46)
        d.line((x1, y1, x2, y2), fill=(255, 255, 255, 200), width=3)

    # glossy highlight (soft arc top-left)
    gloss = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    gd2 = ImageDraw.Draw(gloss)
    gd2.ellipse((cx - R + 30, cy - R + 30, cx + R - 30, cy + R + 60), outline=(255, 255, 255, 46), width=16)
    gloss = gloss.filter(ImageFilter.GaussianBlur(6))
    img.alpha_composite(gloss)

    # center hub: gold ring + dark core + star
    hub_r = 52
    d.ellipse((cx - hub_r, cy - hub_r, cx + hub_r, cy + hub_r), fill=(20, 22, 38, 255), outline=(245, 200, 66, 255), width=8)
    d.ellipse((cx - hub_r + 14, cy - hub_r + 14, cx + hub_r - 14, cy + hub_r - 14), outline=(255, 255, 255, 90), width=3)
    try:
        star_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 56)
        star = Image.new("RGBA", (110, 90), (0, 0, 0, 0))
        ImageDraw.Draw(star).text((6, 6), "\u2605", font=star_font, fill=(255, 214, 90, 255))
        img.alpha_composite(star, (cx - star.width // 2, cy - star.height // 2 + 2))
    except Exception:
        pass

    # pointer (triangle at top) with shadow + outline
    px, py = cx, 26
    d.polygon([(px + 6, py + 10), (px - 30, py + 66), (px + 30, py + 66)], fill=(0, 0, 0, 90))
    d.polygon([(px, py), (px - 30, py + 56), (px + 30, py + 56)], fill=(245, 158, 11, 255), outline=(255, 255, 255, 255), width=5)

    # downscale for anti-aliasing (keep alpha so it sits clean on any theme)
    img = img.resize((SIZE, SIZE), Image.Resampling.LANCZOS)
    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf


def games_build_wheel_image(win_idx):
    """Draw the spin wheel with the winning sector at the top pointer."""
    return _games_wheel_render(win_idx, 0.0)


@bot.tree.command(name="spin", description="Spin the wheel once a day — progressive jackpot!", guild=guild_obj)
async def games_spin(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer()
    free, spins = games_free_use(interaction.user.id, "spin")
    if not free:
        if not games_prepaid_consume(interaction.user.id, "spin"):
            ok, res = games_coin_spend(interaction.user.id, GAMES_SPIN_COST_EXTRA, "spin_extra")
            if not ok:
                return await interaction.followup.send(f"❌ {res}", ephemeral=True)

    # ALWAYS log usage so the free spin can't be farmed on zero-prize outcomes
    games_coin_log_zero(interaction.user.id, "spin", meta={"free": free})
    spin_burned = 0 if free or games_is_unlimited(interaction.user.id) else GAMES_SPIN_COST_EXTRA

    idx, label, rolled_amount, is_jackpot = games_spin_roll()
    jackpot_inc = secrets.randbelow(41) + 10  # jackpot grows 10-50 per spin
    settlement = games_spin_settle(
        interaction.user.id, label, rolled_amount, is_jackpot, jackpot_inc
    )
    if not settlement.get("ok"):
        return await interaction.followup.send("❌ The spin couldn't be settled. Please contact staff.", ephemeral=True)
    amount = int(settlement.get("amount", 0))
    item = settlement.get("item")
    wheel_buf = games_build_wheel_image(idx)

    embed = discord.Embed(title="\U0001f3a1 Spin the Wheel", color=games_color("purple"))
    if is_jackpot:
        games_track("spin", interaction.channel.id, minted=amount, burned=spin_burned)
        games_track_user("spin", interaction.user.id, win=True)
        embed.color = games_color("gold")
        embed.title = "\U0001f3b0 JACKPOT!"
        embed.description = f"{interaction.user.mention} wins the whole **{amount:,}** coin jackpot! 🎆"
        embed.add_field(name="You won", value=games_money(amount), inline=True)
    elif amount > 0:
        games_track("spin", interaction.channel.id, minted=amount, burned=spin_burned)
        games_track_user("spin", interaction.user.id, win=True)
        embed.color = games_color("green")
        embed.description = f"\u2728 You landed on **{label}**!"
        embed.add_field(name="You won", value=games_money(amount), inline=True)
    elif item == "scratch":
        games_track("spin", interaction.channel.id, burned=spin_burned)
        games_track_user("spin", interaction.user.id, win=True)
        embed.color = games_color("cyan")
        embed.description = "🎴 You won a **free scratch card**!"
        embed.add_field(name="You won", value="**Extra Scratch ×1**", inline=True)
    else:
        games_track("spin", interaction.channel.id, burned=spin_burned)
        games_track_user("spin", interaction.user.id, win=False)
        embed.color = games_color("slate")
        embed.description = f"You landed on **{label}** \u2014 better luck next time!"
        embed.add_field(name="You won", value="nothing 😔", inline=True)
    embed.add_field(name="\U0001f3b0 Jackpot", value=f"**{int(settlement['jackpot']):,}** 🪙", inline=True)
    embed.add_field(name="\U0001f5d3 Spins today", value=f"{spins}/1", inline=True)
    games_footer(embed, "1 free spin per 24h window · extra spins from /shop")
    frame1 = games_build_wheel_frame(idx, -32) if wheel_buf else None
    frame2 = games_build_wheel_frame(idx, -10) if wheel_buf else None
    spin_embed = discord.Embed(
        title="🎡 Spinning…",
        description="The wheel is in motion — where will it land?",
        color=games_color("purple"),
    )
    spin_embed.set_footer(text="Good luck!")
    msg = await interaction.followup.send(
        embed=spin_embed,
        file=discord.File(frame1, filename="wheel.png") if frame1 else None,
        ephemeral=not is_jackpot,
    )
    if frame1 and frame2 and wheel_buf:
        try:
            await asyncio.sleep(0.5)
            await msg.edit(attachments=[discord.File(frame2, filename="wheel.png")])
            await asyncio.sleep(0.5)
            await msg.edit(embed=embed, attachments=[discord.File(wheel_buf, filename="wheel.png")])
        except Exception as anim_exc:
            print(f"[games] wheel animation failed: {anim_exc}")
            try:
                await msg.edit(embed=embed, attachments=[discord.File(wheel_buf, filename="wheel.png")] if wheel_buf else None)
            except Exception:
                pass
    else:
        try:
            await msg.edit(embed=embed, attachments=[discord.File(wheel_buf, filename="wheel.png")] if wheel_buf else None)
        except Exception:
            pass


# ---------------- SCRATCH ----------------

def games_scratch_apply_pity(user_id, picks):
    """Guarantee a pair after several misses and persist the miss counter."""
    picks = list(picks)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                    (int(user_id),),
                )
                cur.execute("SELECT scratch_pity FROM mcwv_coins WHERE discord_id = %s FOR UPDATE", (int(user_id),))
                pity = int(cur.fetchone()[0] or 0)
                counts = {p: picks.count(p) for p in set(picks)}
                natural_win = max(counts.values(), default=0) >= 2
                triggered = False
                if not natural_win and pity >= GAMES_SCRATCH_PITY_MISSES:
                    picks[2] = picks[1]
                    natural_win = True
                    triggered = True
                cur.execute(
                    "UPDATE mcwv_coins SET scratch_pity = %s WHERE discord_id = %s",
                    (0 if natural_win else pity + 1, int(user_id)),
                )
        return picks, triggered, (0 if natural_win else pity + 1)
    except Exception as exc:
        print(f"[games] scratch pity failed: {exc}")
        return picks, False, 0


@bot.tree.command(name="scratch", description="Scratch 3 pets — daily free, match for prizes", guild=guild_obj)
async def games_scratch(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer()
    # 1 free daily (atomic 24h window)
    free, used = games_free_use(interaction.user.id, "scratch")
    if not free:
        if not games_prepaid_consume(interaction.user.id, "scratch"):
            ok, res = games_coin_spend(interaction.user.id, 100, "scratch_extra")
            if not ok:
                return await interaction.followup.send(f"❌ {res}", ephemeral=True)
    games_coin_log_zero(interaction.user.id, "scratch", meta={"free": free})
    scratch_burned = 0 if free or games_is_unlimited(interaction.user.id) else 100
    pool = [p for p in GAMES_PET_SEED]
    tier_weights = []
    for p in pool:
        if p.startswith("Titanic"):
            tier_weights.append(5)
        elif p.startswith("Huge") or p.startswith("Gargantuan"):
            tier_weights.append(20)
        else:
            tier_weights.append(100)
    picks = [games_weighted_choice(pool, tier_weights) for _ in range(3)]
    picks, pity_triggered, pity_misses = games_scratch_apply_pity(interaction.user.id, picks)
    count_map = {}
    for p in picks:
        count_map[p] = count_map.get(p, 0) + 1
    best = max(count_map.values())
    award = 0
    if best == 3:
        if picks[0].startswith("Titanic"):
            award = 10000
        elif picks[0].startswith(("Huge", "Gargantuan")):
            award = 5000
        else:
            award = 500
    elif best == 2:
        award = 100
    if award:
        games_coin_adjust(interaction.user.id, award, "scratch_win", meta={"picks": picks, "pity": pity_triggered})
        games_track("scratch", interaction.channel_id, minted=award, burned=scratch_burned)
    else:
        games_track("scratch", interaction.channel_id, burned=scratch_burned)
    games_track_user("scratch", interaction.user.id, win=award > 0)
    if best == 3:
        result_txt = f"🎆 **TRIPLE MATCH! +{award:,}** 🪙"
        result_color = games_color("gold")
    elif best == 2:
        result_txt = f"✨ **Pair! +{award:,}** 🪙"
        result_color = games_color("green")
    else:
        result_txt = "😔 No match — better luck tomorrow!"
        result_color = games_color("slate")
    embed = discord.Embed(title="🎴 Scratch Card", color=result_color)
    embed.add_field(name="Your three pets", value=f"1️⃣ {picks[0]}\n2️⃣ {picks[1]}\n3️⃣ {picks[2]}", inline=False)
    embed.add_field(name="Result", value=result_txt, inline=False)
    if pity_triggered:
        embed.add_field(name="🛡️ Lucky save", value="Miss protection upgraded this card to a pair!", inline=False)
    elif not award:
        embed.add_field(
            name="🛡️ Pair protection",
            value=f"**{pity_misses}/{GAMES_SCRATCH_PITY_MISSES}** misses — next miss after the bar fills becomes a pair.\n"
                  f"`{games_bar(pity_misses, GAMES_SCRATCH_PITY_MISSES, 8)}`",
            inline=False,
        )
    games_footer(embed, "1 free daily · extras from /shop · miss protection included")
    strip = await games_build_scratch_strip(picks)
    covered = await games_build_scratch_strip(picks, covered=True) if strip else None
    # hidden state first — names + result only appear after the scratch reveal
    hidden = discord.Embed(title="🎴 Scratch Card", color=games_color("amber"))
    hidden.add_field(name="Your three pets", value="1️⃣ ???\n2️⃣ ???\n3️⃣ ???", inline=False)
    hidden.add_field(name="Result", value="*scratching…*", inline=False)
    games_footer(hidden, "1 free daily · extras from /shop")
    file = discord.File(covered if covered else strip, filename="scratch.png") if (covered or strip) else None
    msg = await interaction.followup.send(
        embed=hidden if covered else embed,
        file=file,
        ephemeral=award < 1000,
    )
    if covered and strip:
        try:
            await asyncio.sleep(0.8)
            await msg.edit(content="🎴 *scratching…*")
            await asyncio.sleep(0.8)
            await msg.edit(content=None, embed=embed, attachments=[discord.File(strip, filename="scratch.png")])
        except Exception as exc:
            print(f"[games] scratch reveal failed: {exc}")
            try:
                await msg.edit(content=None, embed=embed, attachments=[discord.File(strip, filename="scratch.png")])
            except Exception:
                pass




# ---------------- WEEKLY LOTTERY ----------------

# ---------------- WAR BINGO ----------------

GAMES_BINGO_EVENTS = [
    ("MCWV reaches top 25", lambda ctx: ctx.get("rank") and ctx["rank"] <= 25),
    ("MCWV reaches top 10", lambda ctx: ctx.get("rank") and ctx["rank"] <= 10),
    ("MCWV reaches top 5", lambda ctx: ctx.get("rank") and ctx["rank"] <= 5),
    ("MCWV passes 100M points", lambda ctx: ctx.get("points", 0) >= 100_000_000),
    ("MCWV passes 250M points", lambda ctx: ctx.get("points", 0) >= 250_000_000),
    ("MCWV passes 500M points", lambda ctx: ctx.get("points", 0) >= 500_000_000),
    ("MCWV passes 750M points", lambda ctx: ctx.get("points", 0) >= 750_000_000),
    ("MCWV passes 1B points", lambda ctx: ctx.get("points", 0) >= 1_000_000_000),
    ("A member passes 1M points", lambda ctx: bool(ctx.get("top_member_points", 0) >= 1_000_000)),
    ("A member passes 2M points", lambda ctx: bool(ctx.get("top_member_points", 0) >= 2_000_000)),
    ("A member passes 5M points", lambda ctx: bool(ctx.get("top_member_points", 0) >= 5_000_000)),
    ("A member passes 10M points", lambda ctx: bool(ctx.get("top_member_points", 0) >= 10_000_000)),
    ("A member passes 25M points", lambda ctx: bool(ctx.get("top_member_points", 0) >= 25_000_000)),
    ("10 members score points", lambda ctx: ctx.get("scorers", 0) >= 10),
    ("20 members score points", lambda ctx: ctx.get("scorers", 0) >= 20),
    ("25 members score points", lambda ctx: ctx.get("scorers", 0) >= 25),
    ("40 members score points", lambda ctx: ctx.get("scorers", 0) >= 40),
    ("50 members score points", lambda ctx: ctx.get("scorers", 0) >= 50),
    ("War timer passes 25%", lambda ctx: ctx.get("progress_pct", 0) >= 25),
    ("War timer passes 50%", lambda ctx: ctx.get("progress_pct", 0) >= 50),
    ("War timer passes 75%", lambda ctx: ctx.get("progress_pct", 0) >= 75),
    ("Final 24 hours", lambda ctx: ctx.get("hours_left", 999) <= 24),
    ("Final 12 hours", lambda ctx: ctx.get("hours_left", 999) <= 12),
    ("Final 6 hours", lambda ctx: ctx.get("hours_left", 999) <= 6),
]


@bot.tree.command(name="bingo", description="War Bingo — free card, auto-marked live during wars", guild=guild_obj)
async def games_bingo(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    battle_id = await get_active_battle_id_for_placement()
    if not battle_id:
        st = get_battles_war_state()
        battle_id = str(st["battle_id"]) if st and st.get("battle_id") else None
    if not battle_id:
        return await interaction.followup.send("❌ No active or scheduled war found.", ephemeral=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT card, marked FROM mcwv_bingo_cards WHERE discord_id = %s AND battle_id = %s", (interaction.user.id, str(battle_id)))
            row = cur.fetchone()
        if row:
            card = row[0] if isinstance(row[0], list) else json.loads(row[0] or "[]")
            marked = row[1] if isinstance(row[1], list) else json.loads(row[1] or "[]")
            img_buf = games_build_bingo_card(card, marked, str(battle_id))
            embed = discord.Embed(title=f"🎯 War Bingo — {str(battle_id)}", color=games_color("gold"))
            embed.add_field(name="Progress", value=f"**{len(marked)}/24** marked `{games_bar(len(marked), 24, 12)}`", inline=True)
            games_footer(embed, "Auto-updates live during the war · check /bingo anytime")
            file = discord.File(img_buf, filename="bingo.png") if img_buf else None
            return await interaction.followup.send(embed=embed, file=file, ephemeral=True)
        pool = GAMES_BINGO_EVENTS[:]
        secrets.SystemRandom().shuffle(pool)
        card = [ev[0] for ev in pool[:24]]
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO mcwv_bingo_cards (discord_id, battle_id, card)
                   VALUES (%s,%s,%s::jsonb)
                   ON CONFLICT (discord_id, battle_id) DO NOTHING
                   RETURNING card, marked""",
                (interaction.user.id, str(battle_id), json.dumps(card)))
            inserted = cur.fetchone()
            if inserted is None:
                cur.execute(
                    "SELECT card, marked FROM mcwv_bingo_cards WHERE discord_id = %s AND battle_id = %s",
                    (interaction.user.id, str(battle_id)),
                )
                inserted = cur.fetchone()
        conn.commit()
        if inserted:
            card = inserted[0] if isinstance(inserted[0], list) else json.loads(inserted[0] or "[]")
            marked = inserted[1] if isinstance(inserted[1], list) else json.loads(inserted[1] or "[]")
        else:
            marked = []
        img_buf = games_build_bingo_card(card, marked, str(battle_id))
        embed = discord.Embed(
            title=f"🎯 War Bingo — {str(battle_id)}",
            description="Your free card is ready! It **marks itself live** during the war.",
            color=games_color("gold"),
        )
        games_footer(embed, "Check /bingo anytime to see your progress")
        file = discord.File(img_buf, filename="bingo.png") if img_buf else None
        await interaction.followup.send(embed=embed, file=file, ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"❌ Bingo failed: `{type(exc).__name__}`", ephemeral=True)


def _games_wrap_text(draw, text, font, max_w):
    """Wrap text to fit max_w pixels. Returns list of lines."""
    words = str(text).split(" ")
    lines, cur = [], ""
    for w in words:
        t = (cur + " " + w).strip()
        if draw.textlength(t, font=font) <= max_w:
            cur = t
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines[:4]


def games_build_bingo_card(card, marked, battle_id):
    """Render a 5x5 bingo card image (24 events + FREE centre)."""
    try:
        W, H = 1100, 1280
        img = Image.new("RGBA", (W, H), (13, 15, 26, 255))
        d = ImageDraw.Draw(img)
        try:
            title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 52)
            sub_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 30)
            cell_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
            cell_bold = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
        except Exception:
            title_font = sub_font = cell_font = cell_bold = ImageFont.load_default()
        # header band
        d.rectangle((0, 0, W, 120), fill=(26, 20, 10, 255))
        d.rectangle((0, 116, W, 120), fill=(245, 200, 66, 255))
        d.text((W // 2 - d.textlength("MCWV WAR BINGO", font=title_font) // 2, 16), "MCWV WAR BINGO", font=title_font, fill=(245, 200, 66, 255))
        sub = f"Battle: {battle_id}"
        d.text((W // 2 - d.textlength(sub, font=sub_font) // 2, 78), sub, font=sub_font, fill=(200, 205, 220, 255))
        # grid
        cell = 200
        gap = 10
        x0 = (W - (5 * cell + 4 * gap)) // 2
        y0 = 160
        free_idx = 12
        for i in range(25):
            col, row = i % 5, i // 5
            x = x0 + col * (cell + gap)
            y = y0 + row * (cell + gap)
            if i == free_idx:
                d.rounded_rectangle((x, y, x + cell, y + cell), radius=14, fill=(34, 26, 8, 255), outline=(245, 200, 66, 255), width=4)
                star_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 56)
                st_w = d.textlength("★", font=star_font)
                d.text((x + (cell - st_w) // 2, y + 34), "★", font=star_font, fill=(245, 200, 66, 255))
                d.text((x + (cell - d.textlength("FREE", font=cell_bold)) // 2, y + 118), "FREE", font=cell_bold, fill=(255, 255, 255, 255))
                continue
            card_i = i - 1 if i > free_idx else i
            if card_i >= len(card):
                continue
            ev = str(card[card_i])
            is_marked = card_i in marked
            bg = (18, 66, 42, 255) if is_marked else (26, 29, 48, 255)
            outline = (34, 197, 94, 255) if is_marked else (72, 78, 104, 255)
            d.rounded_rectangle((x, y, x + cell, y + cell), radius=14, fill=bg, outline=outline, width=3)
            if is_marked:
                tick = "✓"
                d.text((x + 12, y + 8), tick, font=cell_bold, fill=(134, 239, 172, 255))
            fg = (226, 232, 240, 255) if is_marked else (188, 194, 210, 255)
            lines = _games_wrap_text(d, ev, cell_font, cell - 28)
            ty = y + 44
            for ln in lines:
                d.text((x + 14, ty), ln, font=cell_font, fill=fg)
                ty += 34
        # footer strip
        d.rectangle((0, H - 56, W, H), fill=(26, 20, 10, 255))
        footer = f"{len(marked)}/24 marked"
        d.text((W // 2 - d.textlength(footer, font=sub_font) // 2, H - 46), footer, font=sub_font, fill=(245, 200, 66, 255))
        buf = BytesIO()
        img.convert("RGB").save(buf, format="PNG", optimize=True)
        buf.seek(0)
        return buf
    except Exception as exc:
        print(f"[games] bingo card build failed: {exc}")
        return None


def games_bingo_lines(marked_indices):
    """Count complete 5-cell lines on the card (rows/cols/diagonals; FREE always marked)."""
    if len(marked_indices) == 24:
        return 12  # blackout = every line
    grid = [[False] * 5 for _ in range(5)]
    for cell_i in range(25):
        if cell_i == 12:  # FREE
            grid[cell_i // 5][cell_i % 5] = True
            continue
        card_i = cell_i - 1 if cell_i > 12 else cell_i
        if card_i in marked_indices:
            grid[cell_i // 5][cell_i % 5] = True
    lines = 0
    for r in range(5):
        if all(grid[r][c] for c in range(5)):
            lines += 1
    for c in range(5):
        if all(grid[r][c] for r in range(5)):
            lines += 1
    if all(grid[i][i] for i in range(5)):
        lines += 1
    if all(grid[i][4 - i] for i in range(5)):
        lines += 1
    return lines


GAMES_BINGO_PRIZES = [("line", 100), ("bingo", 1000), ("blackout", 2500)]


async def games_bingo_mark_all():
    """Housekeeping: mark cards against live war state + pay line/bingo/blackout prizes."""
    battle_id = await get_active_battle_id_for_placement()
    if not battle_id:
        return
    announcements = []

    war_state = get_battles_war_state()

    def _mark(battle_id_str):
        ctx = {"rank": None, "points": 0, "top_member_points": 0, "scorers": 0,
               "progress_pct": 0, "hours_left": 999}
        st = war_state
        if st and st.get("finish"):
            now = time.time()
            if st["finish"] > now:
                ctx["hours_left"] = (st["finish"] - now) / 3600.0

        # Live war facts come only from an optional SELECT-only MCWV connection.
        data_worker = _readonly_connection()
        if data_worker is not None:
            try:
                data_worker.set_session(readonly=True, autocommit=True)
                with data_worker.cursor() as data_cur:
                    data_cur.execute(
                        "SELECT rank, battle_points, progress_percent FROM war_snapshots "
                        "WHERE clan_name = %s AND battle_id = %s ORDER BY captured_at DESC LIMIT 1",
                        (CLAN_NAME, battle_id_str),
                    )
                    row = data_cur.fetchone()
                    if row:
                        ctx["rank"] = int(row[0] or 0)
                        ctx["points"] = int(row[1] or 0)
                        try:
                            ctx["progress_pct"] = float(row[2] or 0)
                        except Exception:
                            pass
                    data_cur.execute(
                        "SELECT COALESCE(MAX(points),0) FROM player_leaderboard_history "
                        "WHERE battle_id = lower(%s)", (battle_id_str,),
                    )
                    ctx["top_member_points"] = int(data_cur.fetchone()[0] or 0)
                    data_cur.execute(
                        "SELECT COUNT(*) FROM player_leaderboard_history "
                        "WHERE battle_id = lower(%s) AND points > 0", (battle_id_str,),
                    )
                    ctx["scorers"] = int(data_cur.fetchone()[0] or 0)
            except Exception as exc:
                print(f"[mcwv-readonly] bingo facts unavailable: {exc}")
            finally:
                data_worker.close()

        # Card state and rewards are written only to the isolated games DB.
        worker = games_new_db_connection()
        if worker is None:
            return
        try:
            with worker:
                with worker.cursor() as cur:
                    cur.execute(
                        "SELECT id, discord_id, card, marked, bingo_paid FROM mcwv_bingo_cards "
                        "WHERE battle_id = %s FOR UPDATE", (battle_id_str,),
                    )
                    cards = cur.fetchall()

                    for cid, uid, card_json, marked_json, paid_json in cards:
                        card = card_json if isinstance(card_json, list) else json.loads(card_json or "[]")
                        marked = marked_json if isinstance(marked_json, list) else json.loads(marked_json or "[]")
                        paid = paid_json if isinstance(paid_json, list) else json.loads(paid_json or "[]")
                        new_marked = set(marked)
                        for i, ev_name in enumerate(card):
                            ev = next((e for e in GAMES_BINGO_EVENTS if e[0] == ev_name), None)
                            if ev and ev[1](ctx):
                                new_marked.add(i)
                        lines = games_bingo_lines(new_marked)
                        reached = []
                        if lines >= 1:
                            reached.append(("line", 100))
                        if lines >= 2:
                            reached.append(("bingo", 1000))
                        if len(new_marked) == 24:
                            reached.append(("blackout", 2500))
                        awards = [(tier, amount) for tier, amount in reached if tier not in paid]
                        new_paid = paid + [tier for tier, _ in awards]
                        cur.execute(
                            "UPDATE mcwv_bingo_cards SET marked = %s::jsonb, bingo_paid = %s::jsonb WHERE id = %s",
                            (json.dumps(sorted(new_marked)), json.dumps(new_paid), cid),
                        )
                        if not awards:
                            continue
                        cur.execute(
                            "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                            (uid,),
                        )
                        for tier, amount in awards:
                            cur.execute(
                                """UPDATE mcwv_coins
                                   SET balance = balance + %s, total_earned = total_earned + %s
                                   WHERE discord_id = %s RETURNING balance""",
                                (amount, amount, uid),
                            )
                            balance_after = int(cur.fetchone()[0])
                            cur.execute(
                                """INSERT INTO mcwv_coin_log
                                   (actor_id, target_id, type, amount, balance_after, meta)
                                   VALUES (%s,%s,%s,%s,%s,%s::jsonb)""",
                                (uid, uid, f"bingo_{tier}", amount, balance_after,
                                 json.dumps({"battle": battle_id_str, "lines": lines})),
                            )
                            cur.execute("""
                                INSERT INTO mcwv_game_stats (game, sessions, coins_minted, coins_burned, last_played)
                                VALUES ('bingo', 0, %s, 0, NOW())
                                ON CONFLICT (game) DO UPDATE SET
                                    coins_minted = mcwv_game_stats.coins_minted + EXCLUDED.coins_minted,
                                    last_played = NOW()
                            """, (amount,))
                            announcements.append((int(uid), tier, amount))
        finally:
            worker.close()

    try:
        await asyncio.to_thread(_mark, str(battle_id))
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[games] bingo marking failed: {exc}")
        return
    if announcements:
        tier_emoji = {"line": "🎯", "bingo": "🎉", "blackout": "🌑"}
        try:
            chans = json.loads(db_get_setting(GAMES_SETTING_SPAWN_CHANNELS, "[]") or "[]")
        except Exception:
            chans = []
        channel = bot.get_channel(int(chans[0])) if chans else None
        if channel:
            for uid, tier, amount in announcements:
                embed = discord.Embed(
                    title=f"{tier_emoji.get(tier, '🎯')} War Bingo — {tier.upper()}!",
                    description=f"<@{uid}> wins **{amount:,}** 🪙 on their {str(battle_id)} bingo card!",
                    color=games_color("gold"),
                )
                await channel.send(embed=embed)


# ---------------- TOWER OF PETS ----------------

# Legacy button-tower implementation removed; the live chat tower is below.


# ---------------- GAMES GUIDE HUB ----------------

class GamesGuideSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Economy", value="economy", emoji="🪙", description="Coins, daily, bank, interest"),
            discord.SelectOption(label="Guess the Pet", value="guess", emoji="🐾", description="Name the pet, win coins"),
            discord.SelectOption(label="Hatch", value="hatch", emoji="🥚", description="Eggs, odds, collections"),
            discord.SelectOption(label="Duels", value="duels", emoji="⚔️", description="1v1 wagers"),
            discord.SelectOption(label="Trivia", value="trivia", emoji="🧠", description="PS99 knowledge"),
            discord.SelectOption(label="Spin", value="spin", emoji="🎡", description="Daily wheel + jackpot"),
            discord.SelectOption(label="Scratch", value="scratch", emoji="🎴", description="Match 3 pets"),
            discord.SelectOption(label="Lottery", value="lottery", emoji="🎟", description="Weekly pool"),
            discord.SelectOption(label="Bingo", value="bingo", emoji="🎯", description="Live war bingo"),
            discord.SelectOption(label="Tower", value="tower", emoji="🏗", description="Endless climb"),
            discord.SelectOption(label="Word games", value="words", emoji="🔤", description="Petdle, scramble, hangman"),
            discord.SelectOption(label="Rules", value="rules", emoji="📜", description="Fair play"),
        ]
        super().__init__(placeholder="What do you want to learn about?", options=options, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        pages = {
            "economy": ("🪙 Economy", f"**Earning:** `/daily` (100 + 10×streak, reward caps at day {GAMES_DAILY_STREAK_REWARD_CAP}) · winning games · duels · lottery · bank interest\n"
                       "**Spending:** cases, extra hatches/spins/scratches, lottery tickets, duel wagers\n"
                       "**Bank:** `/deposit` + `/withdraw` — banked coins earn **1%/day** (capped at 100k banked).\n"
                       "**Commands:** `/coins` · `/pay` · `/shop` · `/cases`"),
            "guess": ("🐾 Guess the Pet", "Rewarded rounds spawn randomly; game staff run no-coin practice with `/guess start`.\n"
                      "Eight modes: Zoom, Silhouette, Pixel, Tiles, Blur, Monochrome, Negative and Mixed Letters.\n"
                      "You get **90 seconds**, exact hints at 30s/60s, hot/warm/cold reactions and 8 valid guesses. "
                      "Normal chat never costs an attempt. Fast solves, hard modes and streaks earn up to **500 coins**.\n\n"
                      "`/guess stats` shows speed, streaks, win rate and the leaderboard."),
            "hatch": ("🥚 Hatch", "`/hatch` — 3 free hatches per 24h window, extras cost **100 coins**.\n"
                      "Each synced egg uses its real contents and weights; `/eggs <name>` shows them before you hatch.\n"
                      "The daily featured egg boosts its two rarest pets. Huge/Titanic hatches announce publicly.\n`/pets` = your collection book."),
            "duels": ("⚔️ Duels", "`/duel @player <wager>` — challenge someone. They accept, wagers lock, and the bot\n"
                      "runs a live head-to-head round (pet zoom or scramble — bot picks).\n"
                      "First correct answer takes the pot. Timeout = refund."),
            "trivia": ("🧠 Trivia", f"`/trivia` — 5 PS99 questions, 20s each, **{GAMES_TRIVIA_CORRECT_REWARD} coins per correct**.\n"
                       f"Perfect run bonus: **+{GAMES_TRIVIA_PERFECT_BONUS}** · 15-minute cooldown. `/historytrivia` uses MCWV's real war history."),
            "spin": ("🎡 Spin", "`/spin` — 1 free daily spin. Sectors pay 100–1000 coins, plus a **progressive jackpot**\n"
                      "that grows with every spin until someone hits it. Extra spins: **250 coins**."),
            "scratch": ("🎴 Scratch", "`/scratch` — reveals 3 pets. 3-of-a-kind pays: Titanic **10,000** · Huge **5,000** · any **500** · pair **100**.\n"
                       "1 free daily, extras cost 100 coins · miss protection guarantees a pair after a dry streak."),
            "lottery": ("🎟 Lottery", "`/lottery buy` — tickets cost **50 coins**. Winner takes **70%** of the pool.\n"
                       "Draws Sunday 20:00 UTC (owner can draw early)."),
            "bingo": ("🎯 War Bingo", "`/bingo` at war start — free 5×5 card of war events (rank milestones, point bars, timer).\n"
                       "The bot marks it live during the war. Line = 100 · Bingo = 1,000 · Blackout = 2,500."),
            "tower": ("🏗 Tower of Pets", f"`/tower` — climb {GAMES_TOWER_MAX_FLOOR} floors mixing trivia + guess-the-pet. 3 hearts · {GAMES_TOWER_FLOOR_TIMEOUT}s per floor.\n"
                       "Coins scale with depth (25 × floor) and correct-answer combos add a bonus. Reach the roof to win!"),
            "words": ("🔤 Word games", f"`/petdle <guess>` — daily pet-name Wordle ({GAMES_PETDLE_MAX_GUESSES} guesses, resets 00:00 UTC).\n"
                       "`/scramble` — first to unscramble wins 100.\n"
                       "`/hangman` — 6 lives, guess letters or the full name."),
            "rules": ("📜 Rules", "• One account per person — no alt farming dailies/hatches\n"
                      "• No colluding to feed duel/scramble wins\n"
                      "• Coins are fake and for fun — don't sell or trade them for real stuff\n"
                      "• Exploits found? Report to staff — you might get a finder's bonus 😉"),
        }
        title, body = pages.get(self.values[0], ("?", "?"))
        embed = discord.Embed(title=title, description=body, color=discord.Color.from_rgb(108, 34, 245))
        await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="gamesguide", description="Learn how the games and economy work", guild=guild_obj)
async def games_guide(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    view = discord.ui.View(timeout=300)
    view.add_item(GamesGuideSelect())
    embed = discord.Embed(
        title="🎮 MCWV Games",
        description="Pick a topic below to learn how everything works.",
        color=discord.Color.from_rgb(108, 34, 245),
    )
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


# ---------------- GAMES HOUSEKEEPING LOOP ----------------

@tasks.loop(minutes=1)
async def games_housekeeping_loop():
    try:
        if not DATABASE_URL or conn is None or conn.closed != 0:
            return
        # 1. Expire stale rounds / duels / sessions (reveal answers on timeout)
        now = time.time()
        for ch, r in list(ACTIVE_GUESS_ROUNDS.items()):
            if now - float(r["started"]) >= GAMES_GUESS_TIMEOUT:
                await games_guess_timeout(ch, r["started"])
        for ch, s in list(ACTIVE_SCRAMBLE.items()):
            elapsed = now - float(s["started"])
            if elapsed > GAMES_ROUND_TIMEOUT:
                ACTIVE_SCRAMBLE.pop(ch, None)
                games_track_participants("scramble", s.get("participants"), winner_id=None)
                try:
                    channel = bot.get_channel(int(ch))
                    if channel:
                        e = discord.Embed(title="⏰ Scramble Over", description=f"Time's up! The word was **{s.get('answer', '?')}**.", color=games_color("slate"))
                        await channel.send(embed=e)
                except Exception:
                    pass
            elif elapsed > 60 and not s.get("hinted"):
                # mid-round hint: first letter revealed
                s["hinted"] = True
                try:
                    channel = bot.get_channel(int(ch))
                    if channel:
                        word_letters = re.sub(r"[^A-Za-z]", "", str(s.get("answer", "")))
                        hint = f"**{word_letters[0].upper()}**{'_' * max(len(word_letters) - 1, 0)}" if word_letters else "_ _ _"
                        e = discord.Embed(title="💡 Scramble Hint", description=f"`{hint}`", color=games_color("cyan"))
                        e.set_footer(text="First letter revealed — 100 🪙 on the line!")
                        await channel.send(embed=e)
                except Exception:
                    pass
        for ch, g in list(ACTIVE_HANGMAN.items()):
            if now - float(g["started"]) > 300:
                ACTIVE_HANGMAN.pop(ch, None)
                games_track_participants("hangman", g.get("participants"), winner_id=None)
                e = discord.Embed(title="⏰ Hangman Expired", description=f"It was **{g.get('word', '?')}**.", color=games_color("slate"))
                await _hangman_update(g, e)
        # Fallback settlement if an in-memory timeout task was cancelled or lost.
        for duel_id, duel in list(ACTIVE_DUELS.items()):
            if (duel.get("state") == "active" and duel.get("round_started")
                    and now - float(duel["round_started"]) >= GAMES_DUEL_TIMEOUT):
                await settle_duel(duel_id, None, "timeout")
        # expire stale pending duels (DB + memory)
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE mcwv_duels SET state = 'expired' WHERE state = 'pending' AND created_at < NOW() - INTERVAL '5 minutes' RETURNING id")
                expired = [int(r[0]) for r in cur.fetchall()]
            conn.commit()
            for duel_id in expired:
                ACTIVE_DUELS.pop(duel_id, None)
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[games] duel expiry failed: {exc}")
        # Trivia sessions self-advance on question timeout; this is a final leak guard.
        for uid, s in list(ACTIVE_TRIVIA.items()):
            if now - float(s.get("last_activity", s.get("started", now))) > 10 * 60:
                ACTIVE_TRIVIA.pop(uid, None)
        # Every live tower floor has a real deadline. This also unsticks abandoned runs.
        for uid, s in list(ACTIVE_TOWER.items()):
            if not s.get("active"):
                ACTIVE_TOWER.pop(uid, None)
                continue
            if s.get("kind") and now - float(s.get("floor_started", now)) > GAMES_TOWER_FLOOR_TIMEOUT:
                await games_tower_timeout(uid, s)
        # 2. Daily interest tick (1%/day, capped, integer math) — in a thread so
        #    it can never stall commands.
        rate = float(db_get_setting(GAMES_SETTING_INTEREST_RATE, str(GAMES_INTEREST_RATE_PCT_DEFAULT)) or 0)
        cap = int(db_get_setting(GAMES_SETTING_INTEREST_CAP, str(GAMES_INTEREST_CAP_DEFAULT)) or 0)

        def _interest_tick():
            if rate <= 0:
                return
            worker = games_new_db_connection()
            if worker is None:
                return
            rows = []
            try:
                with worker:
                    with worker.cursor() as cur:
                        cur.execute("""
                            SELECT discord_id, bank,
                                   CASE WHEN last_interest_at IS NULL THEN 1 ELSE
                                       LEAST(7, GREATEST(1, FLOOR(EXTRACT(EPOCH FROM (NOW() - last_interest_at)) / 86400)::int))
                                   END AS days_due
                            FROM mcwv_coins
                            WHERE bank > 0
                              AND (last_interest_at IS NULL OR last_interest_at < NOW() - INTERVAL '24 hours')
                            FOR UPDATE
                        """)
                        due = cur.fetchall()
                        for uid, old_bank, days_due in due:
                            interest = int(min(int(old_bank), cap) * rate * int(days_due) / 100)
                            cur.execute(
                                """UPDATE mcwv_coins
                                   SET bank = bank + %s, total_earned = total_earned + %s,
                                       last_interest_at = NOW()
                                   WHERE discord_id = %s RETURNING bank""",
                                (interest, interest, uid),
                            )
                            new_bank = int(cur.fetchone()[0])
                            cur.execute(
                                """INSERT INTO mcwv_coin_log
                                   (actor_id, target_id, type, amount, balance_after, meta)
                                   VALUES (%s,%s,'interest',%s,%s,%s::jsonb)""",
                                (uid, uid, interest, new_bank,
                                 json.dumps({"bank_after": new_bank, "days": int(days_due), "rate": rate})),
                            )
                            rows.append((uid, interest))
                if rows:
                    print(f"[games] interest ticked for {len(rows)} members")
            finally:
                worker.close()

        try:
            await asyncio.to_thread(_interest_tick)
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[games] interest tick failed: {exc}")
        # 3. Bingo marking (every 5 min only)
        if int(time.time()) % 300 < 60:
            try:
                await games_bingo_mark_all()
            except Exception as exc:
                print(f"[games] bingo marking failed: {exc}")
        # Monthly rotating game role (idempotent via its month setting).
        try:
            guild = bot.get_guild(GUILD_ID)
            if guild:
                await games_monthly_petmaster(guild)
        except Exception as exc:
            print(f"[games] monthly petmaster check failed: {exc}")
        # 4. Real pet/egg sync (daily, threaded, batched — a few seconds)
        last_sync = int(db_get_setting("games_eggs_synced_at", "0") or 0)
        if time.time() - last_sync > 24 * 3600:
            try:
                await asyncio.to_thread(games_sync_eggs_v2)
            except Exception as exc:
                print(f"[games] egg sync failed: {exc}")
            try:
                await asyncio.to_thread(games_sync_pets_from_web)
            except Exception as exc:
                print(f"[games] pet sync failed: {exc}")
        # 5. Lottery auto-draw (Sunday 20:00 UTC window, threaded)
        now_dt = datetime.now(timezone.utc)
        if now_dt.weekday() == 6 and now_dt.hour >= 20:
            last_draw = db_get_setting("games_lottery_last_draw_week", "")
            draw_key = now_dt.strftime("%Y-%m-%d")
            if last_draw != draw_key:
                raw = db_get_setting(GAMES_SETTING_SPAWN_CHANNELS, "[]")
                try:
                    chans = json.loads(raw or "[]")
                except Exception:
                    chans = []
                ch = bot.get_channel(int(chans[0])) if chans else None
                # Draw even without an announcement channel; never mark it done first.
                await games_lottery_draw(ch)
                db_set_setting("games_lottery_last_draw_week", draw_key)
    except Exception as exc:
        print(f"[games] housekeeping error: {exc}")


@games_housekeeping_loop.before_loop
async def before_games_housekeeping():
    await bot.wait_until_ready()


# ---------------- MESSAGE HOOKS ----------------

@bot.event
async def on_message(message):
    if message.guild is None:
        return
    if message.author.bot:
        return
    try:
        # A message can settle at most one chat game (prevents double rewards).
        handled = await games_handle_tower_answer(message)
        if not handled:
            handled = await games_handle_answer(message)
        if not handled:
            handled = bool(await games_handle_duel_answer(message))
        if not handled:
            handled = await games_handle_scramble(message)
        if not handled:
            await games_handle_hangman(message)
        # random Guess the Pet spawn
        await games_maybe_spawn(message)
    except Exception as exc:
        print(f"[games] on_message error: {exc}")
    # keep prefix commands working
    try:
        await bot.process_commands(message)
    except Exception:
        pass



# ============================================================
# GAMES v3 — REAL EGGS SYNC + RANDOMNESS + CASE ADMIN + LIMITS
# ============================================================

# ---------- RANDOMNESS: recently-used tracking (no repetitive rounds) ----------
_RECENT_SCOPE = {}      # scope_key -> list of recently used keys (newest last)
_RECENT_MAX = 5


def games_pick_random(pool, scope="global", max_recent=None):
    """Pick a random item, avoiding the recently used ones in this scope.
    Falls back to a fully random pick when everything was recent."""
    if not pool:
        return None
    recent = _RECENT_SCOPE.setdefault(str(scope), [])
    cap = max_recent if max_recent is not None else _RECENT_MAX
    candidates = [p for p in pool if p not in recent]
    if not candidates:
        candidates = list(pool)
    pick = secrets.choice(candidates)
    recent.append(pick)
    if len(recent) > cap:
        recent.pop(0)
    return pick


def games_remember(scope, item, max_recent=None):
    recent = _RECENT_SCOPE.setdefault(str(scope), [])
    recent.append(item)
    cap = max_recent if max_recent is not None else _RECENT_MAX
    if len(recent) > cap:
        recent.pop(0)


# ---------- REAL EGGS: sync from the Big Games database ----------
def games_egg_fallback_pool():
    return [
        {
            "slug": "clan-egg", "name": "Clan Egg", "icon_asset": None,
            "rarity": "Legendary", "rap": 0,
        },
        {
            "slug": "exclusive-cosmic-egg", "name": "Exclusive Cosmic Egg", "icon_asset": "14146204107",
            "rarity": "Exclusive", "rap": 728_520_000,
        },
        {
            "slug": "active-huge-egg", "name": "Active Huge Egg", "icon_asset": "16756521111",
            "rarity": "Exclusive", "rap": 292_970_000,
        },
    ]


def _games_parse_eggs_index():
    """Fetch + parse the public eggs index. Returns list of dicts."""
    import urllib.request as _ur
    try:
        req = _ur.Request(
            "https://db.biggames.io/database/eggs",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        with _ur.urlopen(req, timeout=25) as r:
            html = r.read().decode("utf-8", "replace")
    except Exception as exc:
        print(f"[games] egg index fetch failed: {exc}")
        return []
    eggs = []
    # cards: <img src="/api/thumbnails/asset/{id}" alt="{Name}" ...> ... Rarity: {rarity} ... RAP {value}
    for m in re.finditer(
        r'<img src="/api/thumbnails/asset/(\d+)" alt="([^"]+)"[^>]*>.*?Rarity:\s*([^"<]+).*?aria-label="RAP">.*?</svg>\s*<span[^>]*>([\d.]+)([KMBT]?)</span>',
        html, re.S,
    ):
        icon_asset, name, rarity, rap_val, rap_suffix = m.groups()
        try:
            rap = float(rap_val)
            mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}.get(rap_suffix.upper(), 1)
            rap = int(rap * mult)
        except Exception:
            rap = 0
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        eggs.append({
            "slug": slug, "name": name, "icon_asset": icon_asset,
            "rarity": rarity.strip(), "rap": rap,
        })
    # dedupe by name, keep first
    seen = set()
    unique = []
    for e in eggs:
        if e["slug"] in seen:
            continue
        seen.add(e["slug"])
        unique.append(e)
    print(f"[games] parsed {len(unique)} eggs from the Big Games database")
    return unique


def games_sync_eggs_from_web():
    """Sync the real egg list into mcwv_game_eggs (threaded callers only)."""
    try:
        if not db_enabled():
            return False
        eggs = _games_parse_eggs_index()
        if len(eggs) < 3:
            print("[games] egg sync got too few results — using fallback pool")
            eggs = games_egg_fallback_pool()
        with conn.cursor() as cur:
            for e in eggs:
                cur.execute("""
                    INSERT INTO mcwv_game_eggs (slug, name, icon_asset, rarity, rap, synced_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (slug) DO UPDATE SET
                        name = EXCLUDED.name,
                        icon_asset = EXCLUDED.icon_asset,
                        rarity = EXCLUDED.rarity,
                        rap = EXCLUDED.rap,
                        synced_at = NOW()
                """, (e["slug"][:80], e["name"][:80], e["icon_asset"], e["rarity"][:40], int(e["rap"])))
        conn.commit()
        db_set_setting("games_eggs_synced_at", str(int(time.time())))
        return True
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[games] egg sync failed: {exc}")
        return False


# Theme mapping for fallback egg contents.
_GAMES_THEME_KEYWORDS = {
    "capybara": ["Capybara"], "cat": ["Cat", "Calico", "Nyan"],
    "dog": ["Dog", "Shiba", "Corgi", "Wolfhound", "Puppy"],
    "dragon": ["Dragon", "Kraken", "Wyvern"], "kitsune": ["Kitsune", "Fox"],
    "bunny": ["Bunny", "Rabbit"], "axolotl": ["Axolotl"],
    "penguin": ["Penguin"], "unicorn": ["Unicorn"], "phoenix": ["Phoenix"],
    "griffin": ["Griffin"], "cosmic": ["Cosmic", "Astral", "Nebula"],
    "anime": ["Anime", "Otaku"], "cyber": ["Cyber", "Neon"],
    "halloween": ["Pumpkin", "Ghost", "Bat"],
    "christmas": ["Snow", "Elf", "Reindeer"],
    "space": ["Space", "Astro", "Rocket"],
}


def games_theme_pool(egg_name, prefix):
    """Pets starting with prefix whose name matches the egg's theme."""
    pool = [p for p in GAMES_PET_SEED if p.startswith(prefix)]
    if not pool:
        return []
    key = normalize_answer(egg_name)
    themed = [p for p in pool if any(kw.lower() in key for kw in _GAMES_THEME_KEYWORDS.keys())
              and any(p.lower().startswith(t.lower()) for t in _GAMES_THEME_KEYWORDS.get(next(kw for kw in _GAMES_THEME_KEYWORDS if kw.lower() in key), []))]
    return themed or pool


def games_egg_tiers(egg):
    """Build the contents tier list for a real egg (theme-matched where possible)."""
    name = egg.get("name") or "Clan Egg"
    titanics = games_theme_pool(name, "Titanic") or [p for p in GAMES_PET_SEED if p.startswith("Titanic")]
    huges = games_theme_pool(name, "Huge") or games_theme_pool(name, "Gargantuan") or [p for p in GAMES_PET_SEED if p.startswith(("Huge", "Gargantuan"))]
    if not titanics:
        titanics = [p for p in GAMES_PET_SEED if p.startswith("Titanic")]
    if not huges:
        huges = [p for p in GAMES_PET_SEED if p.startswith(("Huge", "Gargantuan"))]
    theme_word = next((w.title() for w in _GAMES_THEME_KEYWORDS if w in normalize_answer(name)), "")
    exclusive = [f"{theme_word} Exclusive Pet"] if theme_word else ["Exclusive Mystery Pet"]
    epic = [f"{theme_word} Epic Pet"] if theme_word else ["Epic Mystery Pet"]
    rare = [f"{theme_word} Rare Pet"] if theme_word else ["Rare Mystery Pet"]
    common = [f"{theme_word} Pet"] if theme_word else ["Mystery Pet"]
    return [
        ("titanic", 0.5, titanics),
        ("huge", 2.0, huges),
        ("exclusive", 6.0, exclusive),
        ("epic", 15.0, epic),
        ("rare", 25.0, rare),
        ("common", None, common),
    ]


def games_case_reward_role_error(guild, role):
    if role is None:
        return "That role no longer exists."
    if role.is_default() or role.name == "@everyone":
        return "@everyone can't be a case reward."
    if role.managed:
        return "Managed/integration roles can't be case rewards."
    if role.permissions.administrator or role.permissions.manage_guild or role.permissions.manage_roles:
        return "Staff or elevated-permission roles can't be case rewards."
    blocked_staff_roles = games_staff_role_ids() | {int(rid) for rid in MCWV_TICKET_STAFF_ROLE_IDS}
    if role.id in blocked_staff_roles or role.id == ALLOWED_ROLE_ID:
        return "Configured staff roles can't be case rewards."
    me = guild.me if guild else None
    if me and role >= me.top_role:
        return "That role is above my top role, so I couldn't grant it."
    return None


def games_case_admin_embed(selected_case_id=None, notice=None):
    cases = games_case_catalog(enabled_only=False, limit=25)
    selected = next((case for case in cases if case["id"] == selected_case_id), None)
    if selected is None and cases:
        selected = cases[0]
    embed = discord.Embed(
        title="🎁 MCWV Case Manager",
        description=(
            "Create and edit role cases without remembering command syntax.\n\n"
            "1. Pick a case from the menu.\n"
            "2. Use the buttons to change its price, prizes, odds, or status.\n"
            "3. Open `/shop` to see exactly what members will see."
        ),
        color=games_color("violet"),
    )
    if notice:
        embed.add_field(name="✅ Updated", value=str(notice)[:1024], inline=False)
    if not cases:
        embed.add_field(
            name="No cases yet",
            value="Press **Create Case** below. You'll only need a name, price, and optional emoji.",
            inline=False,
        )
        return embed
    summary = []
    for case in cases:
        status = "🟢" if case["enabled"] else "🔴"
        marker = "👉" if selected and case["id"] == selected["id"] else "•"
        summary.append(
            f"{marker} {status} {case['emoji']} **{case['name']}** — {case['price']:,} credits · "
            f"{len(case['contents'])} prize(s)"
        )
    embed.add_field(name=f"All cases ({len(cases)})", value="\n".join(summary)[:1024], inline=False)
    if selected:
        total = sum(chance for _rid, chance in selected["contents"])
        lines = [f"<@&{rid}> — **{chance:.2f}%**" for rid, chance in selected["contents"]]
        nothing = max(0.0, 100.0 - total)
        if nothing > 0.0001:
            lines.append(f"Nothing — **{nothing:.2f}%**")
        embed.add_field(
            name=f"Selected: {selected['emoji']} {selected['name']}",
            value=(
                f"**Status:** {'Enabled 🟢' if selected['enabled'] else 'Disabled 🔴'}\n"
                f"**Price:** {selected['price']:,} credits\n"
                f"**Configured chance:** {total:.2f}% / 100%\n\n"
                f"**What's inside:**\n" + ("\n".join(lines) if lines else "*No prizes yet — press Add Prize.*")
            )[:1024],
            inline=False,
        )
    games_footer(embed, "Case controls are staff-only · dangerous permission roles are blocked automatically")
    return embed


class CaseAdminSelect(discord.ui.Select):
    def __init__(self, cases, selected_id=None):
        options = [
            discord.SelectOption(
                label=case["name"][:100],
                value=str(case["id"]),
                emoji=case["emoji"] if games_emoji_ok(case.get("emoji")) else "🎁",
                description=f"{case['price']:,} credits · {len(case['contents'])} prizes · {'enabled' if case['enabled'] else 'disabled'}"[:100],
                default=case["id"] == selected_id,
            )
            for case in cases[:25]
        ]
        if not options:
            options = [discord.SelectOption(label="No cases yet", value="0", description="Use Create Case below")]
        super().__init__(
            placeholder="Choose a case to manage…",
            min_values=1,
            max_values=1,
            options=options,
            disabled=not cases,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, CaseAdminPanelView):
            return await interaction.response.send_message("This panel expired — run `/caseadmin panel`.", ephemeral=True)
        view.selected_case_id = int(self.values[0])
        await view.refresh_message(interaction)


class CreateCaseModal(discord.ui.Modal, title="Create a role case"):
    case_name = discord.ui.TextInput(label="Case name", placeholder="Tasty Case", max_length=60)
    price = discord.ui.TextInput(label="Price in credits", placeholder="500", max_length=12)
    emoji = discord.ui.TextInput(label="Emoji (optional)", placeholder="🎁", required=False, max_length=16)

    def __init__(self, staff_id):
        super().__init__()
        self.staff_id = int(staff_id)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.staff_id or not games_case_staff_check(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        name = str(self.case_name).strip()
        try:
            price = int(str(self.price).replace(",", "").strip())
        except Exception:
            return await interaction.response.send_message("❌ Price must be a whole number.", ephemeral=True)
        if not name or price < 10 or price > GAMES_MAX_TRANSACTION:
            return await interaction.response.send_message(
                f"❌ Use a name and a price between 10 and {GAMES_MAX_TRANSACTION:,} credits.", ephemeral=True
            )
        use_emoji = str(self.emoji).strip() if games_emoji_ok(str(self.emoji)) else "🎁"
        try:
            duplicate = False
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM mcwv_cases WHERE LOWER(name) = LOWER(%s)", (name,))
                duplicate = cur.fetchone() is not None
                if not duplicate:
                    cur.execute(
                        "INSERT INTO mcwv_cases (name, price, emoji, created_by) VALUES (%s,%s,%s,%s)",
                        (name, price, use_emoji, interaction.user.id),
                    )
            conn.commit()
            if duplicate:
                return await interaction.response.send_message("❌ A case with that name already exists.", ephemeral=True)
            await interaction.response.send_message(
                f"✅ Created {use_emoji} **{name}** for **{price:,} credits**. Press **Refresh** on the manager, select it, then add prizes.",
                ephemeral=True,
            )
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            await interaction.response.send_message(f"❌ Create failed: `{type(exc).__name__}`", ephemeral=True)


class EditCaseModal(discord.ui.Modal, title="Edit case details"):
    price = discord.ui.TextInput(label="Price in credits", max_length=12)
    emoji = discord.ui.TextInput(label="Emoji", required=False, max_length=16)

    def __init__(self, case, staff_id):
        super().__init__()
        self.case_id = int(case["id"])
        self.case_name = case["name"]
        self.staff_id = int(staff_id)
        self.price.default = str(case["price"])
        self.emoji.default = str(case["emoji"])

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.staff_id or not games_case_staff_check(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        try:
            price = int(str(self.price).replace(",", "").strip())
        except Exception:
            return await interaction.response.send_message("❌ Price must be a whole number.", ephemeral=True)
        if price < 10 or price > GAMES_MAX_TRANSACTION:
            return await interaction.response.send_message("❌ Invalid price.", ephemeral=True)
        use_emoji = str(self.emoji).strip() if games_emoji_ok(str(self.emoji)) else "🎁"
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE mcwv_cases SET price = %s, emoji = %s WHERE id = %s RETURNING id",
                    (price, use_emoji, self.case_id),
                )
                row = cur.fetchone()
            conn.commit()
            if not row:
                return await interaction.response.send_message("❌ That case no longer exists.", ephemeral=True)
            await interaction.response.send_message(
                f"✅ Updated {use_emoji} **{self.case_name}** to **{price:,} credits**. Press **Refresh** on the manager.",
                ephemeral=True,
            )
        except Exception as exc:
            await interaction.response.send_message(f"❌ Edit failed: `{type(exc).__name__}`", ephemeral=True)


class AddCasePrizeChanceModal(discord.ui.Modal, title="Set prize chance"):
    chance = discord.ui.TextInput(label="Win chance (%)", placeholder="Example: 14 or 1.5", max_length=8)

    def __init__(self, case_id, role_id, role_name, staff_id):
        super().__init__()
        self.case_id = int(case_id)
        self.role_id = int(role_id)
        self.role_name = str(role_name)
        self.staff_id = int(staff_id)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.staff_id or not games_case_staff_check(interaction.user):
            return await interaction.response.send_message("❌ Staff only.", ephemeral=True)
        try:
            chance = round(float(str(self.chance).strip().rstrip("%")), 2)
        except Exception:
            return await interaction.response.send_message("❌ Chance must be a number, such as `14` or `1.5`.", ephemeral=True)
        if not (0.01 <= chance <= 100):
            return await interaction.response.send_message("❌ Chance must be between 0.01% and 100%.", ephemeral=True)
        role = interaction.guild.get_role(self.role_id) if interaction.guild else None
        role_error = games_case_reward_role_error(interaction.guild, role)
        if role_error:
            return await interaction.response.send_message(f"❌ {role_error}", ephemeral=True)
        try:
            error = None
            case_name = None
            new_total = 0.0
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT name FROM mcwv_cases WHERE id = %s FOR UPDATE", (self.case_id,))
                    case_row = cur.fetchone()
                    if not case_row:
                        error = "That case no longer exists."
                    else:
                        case_name = str(case_row[0])
                        cur.execute(
                            """SELECT COALESCE(SUM(chance), 0),
                                      COALESCE(MAX(chance) FILTER (WHERE role_id = %s), 0)
                               FROM mcwv_case_contents WHERE case_id = %s""",
                            (self.role_id, self.case_id),
                        )
                        used, previous = (float(v or 0) for v in cur.fetchone())
                        new_total = used - previous + chance
                        if new_total > 100.0001:
                            error = (f"Total odds would be {new_total:.2f}%. "
                                     f"Only {100 - used + previous:.2f}% is available.")
                        else:
                            cur.execute("""
                                INSERT INTO mcwv_case_contents (case_id, role_id, chance)
                                VALUES (%s,%s,%s)
                                ON CONFLICT (case_id, role_id) DO UPDATE SET chance = EXCLUDED.chance
                            """, (self.case_id, self.role_id, chance))
            if error:
                return await interaction.response.send_message(f"❌ {error}", ephemeral=True)
            await interaction.response.send_message(
                f"✅ Set <@&{self.role_id}> to **{chance:.2f}%** in **{case_name}**. "
                f"Configured total: **{new_total:.2f}%**. Press **Refresh** on the manager.",
                ephemeral=True,
            )
        except Exception as exc:
            await interaction.response.send_message(f"❌ Prize update failed: `{type(exc).__name__}`", ephemeral=True)


class CasePrizeRoleSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(placeholder="Choose the prize role…", min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, CasePrizeRolePickerView):
            return await interaction.response.send_message("This picker expired.", ephemeral=True)
        role = self.values[0]
        error = games_case_reward_role_error(interaction.guild, role)
        if error:
            return await interaction.response.send_message(f"❌ {error}", ephemeral=True)
        await interaction.response.send_modal(
            AddCasePrizeChanceModal(view.case_id, role.id, role.name, view.staff_id)
        )


class CasePrizeRolePickerView(discord.ui.View):
    def __init__(self, case_id, staff_id):
        super().__init__(timeout=180)
        self.case_id = int(case_id)
        self.staff_id = int(staff_id)
        self.add_item(CasePrizeRoleSelect())

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.staff_id or not games_case_staff_check(interaction.user):
            await interaction.response.send_message("❌ This picker belongs to another staff member.", ephemeral=True)
            return False
        return True


class RemoveCasePrizeSelect(discord.ui.Select):
    def __init__(self, case):
        options = [
            discord.SelectOption(
                label=f"{rid}" if len(f"Role {rid}") > 100 else f"Role {rid}",
                value=str(rid),
                description=f"Current chance: {chance:.2f}%",
            )
            for rid, chance in case["contents"][:25]
        ]
        super().__init__(placeholder="Choose a prize to remove…", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, RemoveCasePrizeView):
            return await interaction.response.send_message("This picker expired.", ephemeral=True)
        role_id = int(self.values[0])
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM mcwv_case_contents WHERE case_id = %s AND role_id = %s RETURNING role_id",
                    (view.case_id, role_id),
                )
                row = cur.fetchone()
            conn.commit()
            await interaction.response.send_message(
                f"✅ Removed <@&{role_id}> from **{view.case_name}**." if row else "❌ Prize not found.",
                ephemeral=True,
            )
            self.disabled = True
            view.stop()
        except Exception as exc:
            await interaction.response.send_message(f"❌ Remove failed: `{type(exc).__name__}`", ephemeral=True)


class RemoveCasePrizeView(discord.ui.View):
    def __init__(self, case, staff_id):
        super().__init__(timeout=180)
        self.case_id = int(case["id"])
        self.case_name = case["name"]
        self.staff_id = int(staff_id)
        self.add_item(RemoveCasePrizeSelect(case))

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.staff_id or not games_case_staff_check(interaction.user):
            await interaction.response.send_message("❌ This picker belongs to another staff member.", ephemeral=True)
            return False
        return True


class DeleteCaseConfirmView(discord.ui.View):
    def __init__(self, case, staff_id):
        super().__init__(timeout=60)
        self.case_id = int(case["id"])
        self.case_name = case["name"]
        self.staff_id = int(staff_id)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.staff_id or not games_case_staff_check(interaction.user):
            await interaction.response.send_message("❌ This confirmation belongs to another staff member.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Delete permanently", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM mcwv_case_contents WHERE case_id = %s", (self.case_id,))
                cur.execute("DELETE FROM mcwv_cases WHERE id = %s RETURNING id", (self.case_id,))
                deleted = cur.fetchone()
        self.stop()
        await interaction.response.edit_message(
            content=f"✅ Deleted **{self.case_name}**." if deleted else "❌ Case not found.",
            embed=None,
            view=None,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await interaction.response.edit_message(content="Deletion cancelled.", embed=None, view=None)


class CaseAdminPanelView(discord.ui.View):
    def __init__(self, staff_id, selected_case_id=None):
        # Stay just inside Discord's ~15-minute ephemeral webhook lifetime so
        # on_timeout can reliably disable the controls instead of leaving stale buttons.
        super().__init__(timeout=840)
        self.staff_id = int(staff_id)
        self.message = None
        self.cases = games_case_catalog(enabled_only=False, limit=25)
        valid_ids = {case["id"] for case in self.cases}
        self.selected_case_id = selected_case_id if selected_case_id in valid_ids else (self.cases[0]["id"] if self.cases else None)
        self.add_item(CaseAdminSelect(self.cases, self.selected_case_id))
        if self.selected_case_id is None:
            for child in self.children:
                if isinstance(child, discord.ui.Button) and child.custom_id not in ("case_manager_create", "case_manager_refresh"):
                    child.disabled = True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    def selected_case(self):
        return next((case for case in self.cases if case["id"] == self.selected_case_id), None)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.staff_id or not games_case_staff_check(interaction.user):
            await interaction.response.send_message("❌ This case manager belongs to another staff member.", ephemeral=True)
            return False
        return True

    async def refresh_message(self, interaction, notice=None):
        new_view = CaseAdminPanelView(self.staff_id, self.selected_case_id)
        new_view.message = interaction.message
        embed = games_case_admin_embed(new_view.selected_case_id, notice=notice)
        await interaction.response.edit_message(embed=embed, view=new_view)

    @discord.ui.button(label="Create Case", style=discord.ButtonStyle.success, emoji="➕", row=1, custom_id="case_manager_create")
    async def create_case(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CreateCaseModal(self.staff_id))

    @discord.ui.button(label="Add Prize", style=discord.ButtonStyle.primary, emoji="🎭", row=1, custom_id="case_manager_add")
    async def add_prize(self, interaction: discord.Interaction, button: discord.ui.Button):
        case = self.selected_case()
        if not case:
            return await interaction.response.send_message("Create or select a case first.", ephemeral=True)
        await interaction.response.send_message(
            f"Choose the role to add to **{case['name']}**. You'll enter its chance next.",
            view=CasePrizeRolePickerView(case["id"], self.staff_id),
            ephemeral=True,
        )

    @discord.ui.button(label="Remove Prize", style=discord.ButtonStyle.secondary, emoji="➖", row=1, custom_id="case_manager_remove")
    async def remove_prize(self, interaction: discord.Interaction, button: discord.ui.Button):
        case = self.selected_case()
        if not case or not case["contents"]:
            return await interaction.response.send_message("That case has no prizes to remove.", ephemeral=True)
        # Rebuild labels with actual guild role names for a friendlier picker.
        view = RemoveCasePrizeView(case, self.staff_id)
        select = view.children[0]
        for option, (rid, _chance) in zip(select.options, case["contents"]):
            role = interaction.guild.get_role(rid) if interaction.guild else None
            option.label = (role.name if role else f"Deleted role {rid}")[:100]
            option.emoji = "🎭"
        await interaction.response.send_message(
            f"Choose a prize to remove from **{case['name']}**:", view=view, ephemeral=True
        )

    @discord.ui.button(label="Edit Price / Emoji", style=discord.ButtonStyle.secondary, emoji="✏️", row=1, custom_id="case_manager_edit")
    async def edit_case(self, interaction: discord.Interaction, button: discord.ui.Button):
        case = self.selected_case()
        if not case:
            return await interaction.response.send_message("Select a case first.", ephemeral=True)
        await interaction.response.send_modal(EditCaseModal(case, self.staff_id))

    @discord.ui.button(label="Enable / Disable", style=discord.ButtonStyle.secondary, emoji="🔄", row=1, custom_id="case_manager_toggle")
    async def toggle_case(self, interaction: discord.Interaction, button: discord.ui.Button):
        case = self.selected_case()
        if not case:
            return await interaction.response.send_message("Select a case first.", ephemeral=True)
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE mcwv_cases SET enabled = NOT enabled WHERE id = %s RETURNING enabled", (case["id"],))
                row = cur.fetchone()
            conn.commit()
            if not row:
                return await interaction.response.send_message("❌ That case no longer exists.", ephemeral=True)
            await self.refresh_message(
                interaction, notice=f"{case['name']} is now {'enabled' if row[0] else 'disabled'}."
            )
        except Exception as exc:
            await interaction.response.send_message(f"❌ Toggle failed: `{type(exc).__name__}`", ephemeral=True)

    @discord.ui.button(label="Delete Case", style=discord.ButtonStyle.danger, emoji="🗑️", row=2, custom_id="case_manager_delete")
    async def delete_case(self, interaction: discord.Interaction, button: discord.ui.Button):
        case = self.selected_case()
        if not case:
            return await interaction.response.send_message("Select a case first.", ephemeral=True)
        await interaction.response.send_message(
            f"⚠️ Permanently delete **{case['name']}** and all its configured prizes? Roll history is kept.",
            view=DeleteCaseConfirmView(case, self.staff_id),
            ephemeral=True,
        )

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji="🔃", row=2, custom_id="case_manager_refresh")
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.refresh_message(interaction)


# ---------- CASE ADMIN (staff-friendly) ----------
@bot.tree.command(name="caseadmin", description="Interactive role-case manager for staff", guild=guild_obj)
@app_commands.describe(
    action="Open the panel, or use a legacy quick action",
    case="Case name",
    role="Role to add/remove",
    chance="Chance % for that role",
    price="Case price in coins",
    emoji="Case emoji (optional)",
)
@app_commands.choices(action=[
    app_commands.Choice(name="open interactive panel", value="panel"),
    app_commands.Choice(name="create", value="create"),
    app_commands.Choice(name="add role", value="add"),
    app_commands.Choice(name="remove role", value="remove"),
    app_commands.Choice(name="list", value="list"),
    app_commands.Choice(name="delete", value="delete"),
    app_commands.Choice(name="toggle", value="toggle"),
    app_commands.Choice(name="stats", value="stats"),
])
async def games_case_admin(interaction: discord.Interaction, action: str = "panel", case: str = None,
                           role: discord.Role = None, chance: float = None, price: int = None, emoji: str = None):
    await interaction.response.defer(ephemeral=True)
    if not games_case_staff_check(interaction.user):
        return await interaction.followup.send("❌ Staff only.", ephemeral=True)

    if action == "panel":
        view = CaseAdminPanelView(interaction.user.id)
        view.message = await interaction.followup.send(
            embed=games_case_admin_embed(view.selected_case_id), view=view, ephemeral=True, wait=True
        )
        return

    try:
        if action == "create":
            if not case or price is None:
                return await interaction.followup.send("Usage: `/caseadmin create` with case name + price (optional emoji).", ephemeral=True)
            if price < 10:
                return await interaction.followup.send("❌ Price must be at least 10 coins.", ephemeral=True)
            # emoji must register correctly — validate or fall back to 🎁
            use_emoji = emoji.strip() if games_emoji_ok(emoji) else "🎁"
            emoji_note = "" if use_emoji == (emoji or "").strip() else " (invalid emoji — using 🎁)"
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM mcwv_cases WHERE LOWER(name) = LOWER(%s)", (case.strip(),))
                if cur.fetchone():
                    return await interaction.followup.send("❌ A case with that name already exists.", ephemeral=True)
                cur.execute(
                    "INSERT INTO mcwv_cases (name, price, emoji, created_by) VALUES (%s, %s, %s, %s) RETURNING id",
                    (case.strip(), int(price), use_emoji, interaction.user.id))
                cid = cur.fetchone()[0]
            conn.commit()
            return await interaction.followup.send(
                f"✅ Case **{case}** created ({price:,} coins, id {cid}){emoji_note}. Add roles with `/caseadmin add`.",
                ephemeral=True)

        if action == "add":
            if not case or role is None or chance is None:
                return await interaction.followup.send("Usage: `/caseadmin add` with case, @role and chance %.", ephemeral=True)
            chance = float(chance)
            if not (0 < chance <= 100):
                return await interaction.followup.send("❌ Chance must be between 0 and 100.", ephemeral=True)
            if role.is_default() or role.name == "@everyone":
                return await interaction.followup.send("❌ @everyone can't be a case reward.", ephemeral=True)
            if role.managed:
                return await interaction.followup.send("❌ Managed/integration roles can't be case rewards.", ephemeral=True)
            dangerous = role.permissions.administrator or role.permissions.manage_guild or role.permissions.manage_roles
            blocked_staff_roles = games_staff_role_ids() | {int(rid) for rid in MCWV_TICKET_STAFF_ROLE_IDS}
            if dangerous or role.id in blocked_staff_roles or role.id == ALLOWED_ROLE_ID:
                return await interaction.followup.send("❌ Staff or elevated-permission roles can't be case rewards.", ephemeral=True)
            me = interaction.guild.me
            if me and role >= me.top_role:
                return await interaction.followup.send("❌ That role is above my top role — I couldn't grant it.", ephemeral=True)
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM mcwv_cases WHERE LOWER(name) = LOWER(%s)", (case.strip(),))
                row = cur.fetchone()
                if not row:
                    return await interaction.followup.send("❌ Case not found.", ephemeral=True)
                cid = int(row[0])
                cur.execute(
                    "SELECT COALESCE(SUM(chance), 0), COALESCE(MAX(chance) FILTER (WHERE role_id = %s), 0) "
                    "FROM mcwv_case_contents WHERE case_id = %s",
                    (role.id, cid),
                )
                used, previous = (float(x or 0) for x in cur.fetchone())
                new_total = used - previous + chance
                if new_total > 100:
                    return await interaction.followup.send(
                        f"❌ Chances would total {new_total:g}% — max 100% (currently {used:g}% used).",
                        ephemeral=True,
                    )
                cur.execute("""
                    INSERT INTO mcwv_case_contents (case_id, role_id, chance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (case_id, role_id) DO UPDATE SET chance = EXCLUDED.chance
                """, (cid, role.id, chance))
            conn.commit()
            return await interaction.followup.send(f"✅ {role.mention} added to **{case}** at **{chance:g}%** (total now {new_total:g}%).", ephemeral=True)

        if action == "remove":
            if not case or role is None:
                return await interaction.followup.send("Usage: `/caseadmin remove` with case + @role.", ephemeral=True)
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM mcwv_cases WHERE LOWER(name) = LOWER(%s)", (case.strip(),))
                row = cur.fetchone()
                if not row:
                    return await interaction.followup.send("❌ Case not found.", ephemeral=True)
                cur.execute("DELETE FROM mcwv_case_contents WHERE case_id = %s AND role_id = %s", (int(row[0]), role.id))
            conn.commit()
            return await interaction.followup.send(f"✅ {role.mention} removed from **{case}**.", ephemeral=True)

        if action == "list":
            with conn.cursor() as cur:
                cur.execute("SELECT id, name, price, emoji, enabled FROM mcwv_cases ORDER BY name")
                cases = cur.fetchall()
            if not cases:
                return await interaction.followup.send("No cases yet — `/caseadmin create` one.", ephemeral=True)
            lines = []
            for cid, cname, cprice, cemoji, cenabled in cases:
                with conn.cursor() as cur:
                    cur.execute("SELECT COALESCE(SUM(chance), 0) FROM mcwv_case_contents WHERE case_id = %s", (cid,))
                    used = float(cur.fetchone()[0] or 0)
                    cur.execute("SELECT role_id, chance FROM mcwv_case_contents WHERE case_id = %s ORDER BY chance DESC", (cid,))
                    contents = cur.fetchall()
                status = "🟢" if cenabled else "🔴"
                roles_txt = ", ".join(f"<@&{rid}> {ch:g}%" for rid, ch in contents) or "no roles"
                lines.append(f"{status} {cemoji} **{cname}** — {cprice:,} coins · roles used {used:g}%\n　{roles_txt}")
            embed = discord.Embed(title="🎁 All Cases", description="\n".join(lines), color=discord.Color.from_rgb(168, 130, 255))
            embed.set_footer(text="/caseadmin add|remove|delete|toggle|stats")
            return await interaction.followup.send(embed=embed, ephemeral=True)

        if action == "delete":
            if not case:
                return await interaction.followup.send("Usage: `/caseadmin delete` + case name.", ephemeral=True)
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM mcwv_cases WHERE LOWER(name) = LOWER(%s)", (case.strip(),))
                row = cur.fetchone()
                if not row:
                    return await interaction.followup.send("❌ Case not found.", ephemeral=True)
                cur.execute("DELETE FROM mcwv_case_contents WHERE case_id = %s", (row[0],))
                cur.execute("DELETE FROM mcwv_cases WHERE id = %s", (row[0],))
            conn.commit()
            return await interaction.followup.send(f"✅ Case **{case}** deleted (roll history kept).", ephemeral=True)

        if action == "toggle":
            if not case:
                return await interaction.followup.send("Usage: `/caseadmin toggle` + case name.", ephemeral=True)
            with conn.cursor() as cur:
                cur.execute("UPDATE mcwv_cases SET enabled = NOT enabled WHERE LOWER(name) = LOWER(%s) RETURNING enabled", (case.strip(),))
                row = cur.fetchone()
            conn.commit()
            if not row:
                return await interaction.followup.send("❌ Case not found.", ephemeral=True)
            return await interaction.followup.send(f"✅ **{case}** is now {'enabled 🟢' if row[0] else 'disabled 🔴'}.", ephemeral=True)

        if action == "stats":
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.name, COUNT(r.id), COALESCE(SUM(r.price_paid), 0)
                    FROM mcwv_cases c
                    LEFT JOIN mcwv_case_rolls r ON r.case_id = c.id
                    GROUP BY c.id, c.name ORDER BY 3 DESC
                """)
                rows = cur.fetchall()
            lines = [f"• **{n}** — {cnt} rolls · {sum_paid:,} coins sunk" for n, cnt, sum_paid in rows] or ["No cases yet."]
            embed = discord.Embed(title="🎁 Case Stats", description="\n".join(lines), color=discord.Color.from_rgb(168, 130, 255))
            return await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[games] caseadmin failed: {exc}")
        return await interaction.followup.send(f"❌ Failed: `{type(exc).__name__}`", ephemeral=True)
    return await interaction.followup.send("❓ Unknown action.", ephemeral=True)



# ============================================================
# GAMES v3 PART 2 — TRIVIA REWORK + RANDOMNESS + LIMITS + SYNC
# ============================================================

# ---------- TRIVIA (event-driven — no long-running handlers) ----------
class TriviaAnswerView(discord.ui.View):
    def __init__(self, correct, session):
        super().__init__(timeout=20)
        self.correct = correct
        self.session = session
        self.answered = False

    async def _answer(self, interaction: discord.Interaction, idx: int):
        if self.answered or interaction.user.id != self.session["user_id"]:
            return await interaction.response.send_message("This question is already answered / not yours.", ephemeral=True)
        self.answered = True
        self.stop()  # prevents on_timeout from advancing the session a second time
        self.session["last_activity"] = time.time()
        for i, child in enumerate(self.children):
            child.disabled = True
            if i == self.correct:
                child.style = discord.ButtonStyle.success
            elif i == idx:
                child.style = discord.ButtonStyle.danger
        try:
            await interaction.response.edit_message(view=self)
        except Exception:
            pass
        correct = idx == self.correct
        if correct:
            self.session["score"] += 1
        else:
            self.session["wrong"] += 1
        await games_trivia_next(interaction, self.session)

    @discord.ui.button(label="A", style=discord.ButtonStyle.secondary)
    async def opt_a(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._answer(interaction, 0)

    @discord.ui.button(label="B", style=discord.ButtonStyle.secondary)
    async def opt_b(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._answer(interaction, 1)

    @discord.ui.button(label="C", style=discord.ButtonStyle.secondary)
    async def opt_c(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._answer(interaction, 2)

    @discord.ui.button(label="D", style=discord.ButtonStyle.secondary)
    async def opt_d(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._answer(interaction, 3)

    async def on_timeout(self):
        # Answered views also time out unless explicitly stopped; never double-advance.
        if self.answered:
            return
        self.answered = True
        self.stop()
        self.session["wrong"] += 1
        self.session["last_activity"] = time.time()
        # find the interaction's channel through the last known reference
        ctx = self.session.get("ctx")
        if ctx is None:
            return
        try:
            await games_trivia_next(ctx, self.session)
        except Exception as exc:
            print(f"[games] trivia timeout advance failed: {exc}")


_TRIVIA_QUESTIONS_PER_RUN = 5


def games_trivia_questions(session):
    """Pick 5 questions avoiding the user's recent ones."""
    pool = [q for q in GAMES_TRIVIA_SEED]
    recent = _RECENT_SCOPE.setdefault(f"trivia:{session['user_id']}", [])
    candidates = [q for q in pool if q[0] not in recent]
    if len(candidates) < _TRIVIA_QUESTIONS_PER_RUN:
        candidates = pool
    picks = secrets.SystemRandom().sample(candidates, min(_TRIVIA_QUESTIONS_PER_RUN, len(candidates)))
    for q in picks:
        recent.append(q[0])
        if len(recent) > 10:
            recent.pop(0)
    return picks


async def games_trivia_next(ctx, session):
    q_idx = session["q_index"]
    questions = session["questions"]
    total = len(questions)
    if q_idx >= len(questions):
        score = session["score"]
        game_key = session.get("game", "trivia")
        award = score * GAMES_TRIVIA_CORRECT_REWARD + (GAMES_TRIVIA_PERFECT_BONUS if score == total else 0)
        if award:
            games_coin_adjust(session["user_id"], award, f"{game_key}_win", meta={"score": score, "total": total})
            games_track(game_key, getattr(ctx.channel, "id", 0), minted=award)
        games_track_user(game_key, session["user_id"], win=score >= max(1, (total + 1) // 2))
        ACTIVE_TRIVIA.pop(session["user_id"], None)
        rank, rank_color, stars = ("S", "gold", "⭐⭐⭐") if score == total else (
            ("A", "green", "⭐⭐") if score >= total - 1 else
            ("B", "blue", "⭐") if score >= total - 2 else
            ("C", "amber", "🌟") if score >= 1 else ("D", "slate", "💤"))
        mode_title = "MCWV History" if game_key == "historytrivia" else "PS99 Trivia"
        embed = discord.Embed(
            title=f"🏁 {mode_title} Complete — Rank {rank}",
            description=(
                f"{stars}\n\n"
                f"Score: **{score}/{total}** ({session['wrong']} wrong)\n"
                f"Earned: **{award:,}** 🪙" + ("\n\n💯 PERFECT RUN!" if score == total else "")
            ),
            color=games_color(rank_color),
        )
        games_footer(embed, "/trivia to play again · /historytrivia for MCWV history")
        await ctx.followup.send(embed=embed, ephemeral=True)
        return
    q, options, correct = questions[q_idx]
    session["q_index"] += 1
    embed = discord.Embed(
        title=f"❓ Question {q_idx + 1}/{total}",
        description=q,
        color=games_color("indigo"),
    )
    for i, opt in enumerate(options):
        embed.add_field(name=f"{GAMES_ABCD[i]}", value=opt, inline=False)
    embed.add_field(name="Progress", value=f"`{games_bar(q_idx + 1, total, total)}`", inline=False)
    embed.set_footer(text=f"20s per question · {GAMES_TRIVIA_CORRECT_REWARD} 🪙 per correct · perfect = +{GAMES_TRIVIA_PERFECT_BONUS} · score {session['score']}/{session['q_index']}")
    view = TriviaAnswerView(correct, session)
    view.ctx = ctx
    await ctx.followup.send(embed=embed, view=view, ephemeral=True)


@bot.tree.command(name="trivia", description="PS99 trivia — 5 questions, 20 coins per correct", guild=guild_obj)
async def games_trivia(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    if interaction.user.id in ACTIVE_TRIVIA:
        return await interaction.followup.send("❌ You already have a trivia session running — finish it first!", ephemeral=True)
    allowed, retry_at = (True, None) if games_is_unlimited(interaction.user.id) else \
        games_cooldown_claim(interaction.user.id, "trivia", GAMES_TRIVIA_COOLDOWN)
    if not allowed:
        retry = discord.utils.format_dt(retry_at, "R") if retry_at else "soon"
        return await interaction.followup.send(f"⏳ Your next trivia run is available {retry}.", ephemeral=True)
    session = {
        "user_id": interaction.user.id,
        "game": "trivia",
        "q_index": 0,
        "score": 0,
        "wrong": 0,
        "questions": games_trivia_questions({"user_id": interaction.user.id}),
        "ctx": interaction,
        "started": time.time(),
        "last_activity": time.time(),
    }
    ACTIVE_TRIVIA[interaction.user.id] = session
    await games_trivia_next(interaction, session)


# ---------- RANDOMNESS: wire into guess / scramble / hangman / tower ----------
# (games_start_guess_round uses games_pick_random with per-channel scope)
GAMES_TOWER_RUNS_PER_DAY = 3
GAMES_DUEL_MIN_WAGER = 10
GAMES_LOTTERY_WEEKLY_TICKET_CAP = 50


def games_lottery_tickets_this_week(user_id):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(SUM(-amount), 0) / %s FROM mcwv_coin_log
                WHERE target_id = %s AND type = 'lottery_ticket'
                  AND created_at > date_trunc('week', NOW())
            """, (GAMES_LOTTERY_TICKET_COST, int(user_id)))
            return int(cur.fetchone()[0] or 0)
    except Exception:
        return 0


# ---------- THREADED LOTTERY DRAW ----------



# ============================================================
# GAMES v4 — REAL DATA LAYER (biggamesapi.io collections)
# ============================================================

GAMES_PUBLIC_API = "https://biggamesapi.io"


def _games_api_json(path, timeout=30):
    import urllib.request as _ur
    try:
        req = _ur.Request(
            GAMES_PUBLIC_API + path,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"},
        )
        with _ur.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except Exception as exc:
        print(f"[games] API {path} failed: {exc}")
        return None


def games_sync_pets_from_web():
    """Sync the REAL pet database: /api/collection/Pets + /api/exists (batch inserts)."""
    try:
        if not db_enabled():
            return False
        coll = _games_api_json("/api/collection/Pets")
        pets_data = (coll or {}).get("data", [])
        exists = _games_api_json("/api/exists")
        exist_map = {}
        if exists:
            for it in (exists.get("data") or []):
                if it.get("category") == "Pet":
                    cd = it.get("configData") or {}
                    name = cd.get("id") or cd.get("name")
                    if name:
                        exist_map[str(name)] = int(it.get("value") or 0)
        if not pets_data:
            return False
        rows = []
        seen = set()
        for p in pets_data:
            cfg = p.get("configData") or {}
            name = str(cfg.get("name") or p.get("configName") or "").strip()
            if not name:
                continue
            category = str(p.get("category") or cfg.get("category") or "")
            thumb = str(cfg.get("thumbnail") or cfg.get("icon") or "")
            m = re.search(r"(\d+)", thumb)
            asset = m.group(1) if m else None
            exist_count = exist_map.get(name, 0)
            slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:80]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            rows.append((slug, name[:100], category[:40], asset, exist_count))
        worker = games_new_db_connection()
        if worker is None:
            return False
        try:
            with worker:
                with worker.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO mcwv_game_pets (slug, name, category, asset, exist_count)
                        VALUES %s
                        ON CONFLICT (slug) DO UPDATE SET
                            name = EXCLUDED.name, category = EXCLUDED.category,
                            asset = EXCLUDED.asset, exist_count = EXCLUDED.exist_count,
                            synced_at = NOW()
                    """, rows, page_size=1000)
                    cur.execute(
                        """INSERT INTO settings (key, value) VALUES ('games_pets_synced_at', %s)
                           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""",
                        (str(int(time.time())),),
                    )
        finally:
            worker.close()
        print(f"[games] synced {len(rows)} real pets")
        return True
    except Exception as exc:
        print(f"[games] pet sync failed: {exc}")
        return False


def games_sync_eggs_v2():
    """Sync REAL eggs with REAL contents + odds from /api/collection/Eggs (batch inserts)."""
    try:
        if not db_enabled():
            return False
        coll = _games_api_json("/api/collection/Eggs")
        eggs_data = (coll or {}).get("data", [])
        if not eggs_data:
            return False
        rows = []
        seen = set()
        for e in eggs_data:
            cfg = e.get("configData") or {}
            name = str(cfg.get("name") or e.get("configName") or "").strip()
            if not name:
                continue
            rarity = str((cfg.get("rarity") or {}).get("_id") or e.get("category") or "")
            icon = str(cfg.get("icon") or "")
            m = re.search(r"(\d+)", icon)
            icon_asset = m.group(1) if m else None
            pets = cfg.get("pets") or []
            contents = []
            for entry in pets:
                if isinstance(entry, list) and len(entry) >= 2:
                    try:
                        contents.append([str(entry[0]), float(entry[1])])
                    except Exception:
                        pass
            slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:80]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            rows.append((slug, name[:100], icon_asset, rarity[:40], 0, json.dumps(contents)))
        worker = games_new_db_connection()
        if worker is None:
            return False
        try:
            with worker:
                with worker.cursor() as cur:
                    execute_values(cur, """
                        INSERT INTO mcwv_game_eggs (slug, name, icon_asset, rarity, rap, contents)
                        VALUES %s
                        ON CONFLICT (slug) DO UPDATE SET
                            name = EXCLUDED.name, icon_asset = EXCLUDED.icon_asset,
                            rarity = EXCLUDED.rarity, contents = EXCLUDED.contents,
                            synced_at = NOW()
                    """, rows, page_size=1000)
                    cur.execute(
                        """INSERT INTO settings (key, value) VALUES ('games_eggs_synced_at', %s)
                           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""",
                        (str(int(time.time())),),
                    )
        finally:
            worker.close()
        print(f"[games] synced {len(rows)} real eggs with real contents")
        return True
    except Exception as exc:
        print(f"[games] egg v2 sync failed: {exc}")
        return False

def games_get_eggs():
    """Real eggs from the DB (fallback to a tiny pool before first sync)."""
    try:
        if db_enabled():
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT slug, name, icon_asset, rarity, rap, contents
                    FROM mcwv_game_eggs ORDER BY name LIMIT 500
                """)
                rows = cur.fetchall()
            if rows:
                out = []
                for r in rows:
                    try:
                        contents = r[5] if isinstance(r[5], list) else json.loads(r[5] or "[]")
                    except Exception:
                        contents = []
                    out.append({
                        "slug": r[0], "name": r[1], "icon_asset": r[2], "rarity": r[3],
                        "rap": int(r[4] or 0), "contents": contents,
                    })
                return out
    except Exception as exc:
        print(f"[games] egg list read failed: {exc}")
    return [
        {"slug": "clan-egg", "name": "Clan Egg", "icon_asset": None, "rarity": "Legendary", "rap": 0, "contents": []},
    ]


def games_get_pets(categories=None):
    """Real pets from the DB; filter by category prefixes (e.g. ('Huge','Titanic'))."""
    try:
        if db_enabled():
            with conn.cursor() as cur:
                if categories:
                    like = " OR ".join(["category ILIKE %s" for _ in categories])
                    params = tuple(f"{c}%" for c in categories)
                    cur.execute(f"SELECT name, category, asset, exist_count FROM mcwv_game_pets WHERE {like}", params)
                else:
                    cur.execute("SELECT name, category, asset, exist_count FROM mcwv_game_pets")
                return [
                    {"name": r[0], "category": r[1], "asset": r[2], "exist_count": int(r[3] or 0)}
                    for r in cur.fetchall()
                ]
    except Exception as exc:
        print(f"[games] pets read failed: {exc}")
    # fallback to seed
    return [
        {"name": n, "category": ("Huge" if n.startswith("Huge") else "Titanic" if n.startswith("Titanic") else "Gargantuan"), "asset": a, "exist_count": 0}
        for n, a in GAMES_PET_SEED.items()
    ]


def games_pet_asset(name):
    """Asset id for a pet name (from the pets table or the seed)."""
    if name in GAMES_PET_SEED:
        return GAMES_PET_SEED[name]
    try:
        if db_enabled():
            with conn.cursor() as cur:
                cur.execute("SELECT asset FROM mcwv_game_pets WHERE LOWER(name) = LOWER(%s) LIMIT 1", (name,))
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0])
    except Exception:
        pass
    return None


def games_guess_pet_pool():
    """Huges + Titanics + Gargantuans (recognizable)."""
    pool = [p["name"] for p in games_get_pets(("Huge", "Titanic", "Gargantuan"))]
    if not pool:
        pool = [p for p in GAMES_PET_SEED]
    return pool


def games_random_word_pool():
    """All real pet names (for scramble/hangman/petdle)."""
    pool = [p["name"] for p in games_get_pets()]
    if len(pool) < 10:
        pool = list(GAMES_PET_SEED.keys())
    return pool


async def games_egg_autocomplete(interaction: discord.Interaction, current: str):
    eggs = games_get_eggs()
    cur = (current or "").strip().lower()
    matches = [e["name"] for e in eggs if not cur or cur in e["name"].lower()][:25]
    return [app_commands.Choice(name=n, value=n) for n in matches]


def games_hatch_roll(egg):
    """Roll from an egg dict with real `contents` ([[name, chance%], ...]).
    Falls back to the theme ladder for eggs without contents."""
    contents = egg.get("contents") or []
    if contents:
        total = sum(w for _, w in contents)
        if total > 0:
            names = [c[0] for c in contents]
            weights = [c[1] for c in contents]
            pick = games_weighted_choice(names, weights)
            w = float(dict(contents).get(pick, 0))
            tier = games_pet_roll_tier(pick, w, egg)
            effective_chance = (w / total * 100.0) if total > 0 else 0.0
            return pick, tier, effective_chance
    # fallback ladder
    tiers = games_egg_tiers(egg)
    pet, tier = games_hatch_roll_tiers(tiers)
    return pet, tier, None


def games_hatch_roll_tiers(tiers):
    """Roll from a tiers list: (tier, chance, pets)."""
    tier_names, tier_weights = [], []
    for tier, chance, pets in tiers:
        if not pets:
            continue
        if chance is None:
            continue
        tier_names.append(tier)
        tier_weights.append(float(chance))
    common_weight = max(0.0, 100.0 - sum(tier_weights))
    tier_names.append("common")
    tier_weights.append(common_weight)
    tier = games_weighted_choice(tier_names, tier_weights)
    pool = next((pets for t, c, pets in tiers if t == tier), [])
    if not pool:
        pool = [t for t, c, pets in tiers for pets_ in [pets] for t in pets_] or ["Mystery Pet"]
    pet = games_pick_random(pool, scope=f"hatch:{tier}")
    return pet, tier


# ---------- FEATURED EGG (daily, double odds on the top tiers) ----------
def games_featured_egg():
    """Today's featured egg slug (rotates daily among eggs with contents)."""
    key = "games_featured_egg"
    raw = db_get_setting(key, "")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if raw and "|" in raw:
        day, slug = raw.split("|", 1)
        if day == today and slug:
            return slug
    eggs = [e for e in games_get_eggs() if e.get("contents")]
    if eggs:
        slug = secrets.choice(eggs)["slug"]
        db_set_setting(key, f"{today}|{slug}")
        return slug
    return None


def games_hatch_roll_featured(egg):
    """Featured roll: double the top two tiers' odds (renormalized)."""
    contents = egg.get("contents") or []
    if not contents:
        return games_hatch_roll(egg)
    sorted_c = sorted((c for c in contents if float(c[1]) > 0), key=lambda c: c[1])
    doubled = {c[0] for c in sorted_c[:2]}
    names, weights = [], []
    base_weights = {}
    for n, w in contents:
        w = max(0.0, float(w))
        names.append(n)
        base_weights[n] = w
        weights.append(w * 2 if n in doubled else w)
    pick = games_weighted_choice(names, weights)
    base_w = base_weights.get(pick, 0.0)
    tier = games_pet_roll_tier(pick, base_w, egg)
    adjusted = base_w * 2 if pick in doubled else base_w
    effective_chance = adjusted / sum(weights) * 100.0 if sum(weights) > 0 else 0.0
    return pick, tier, effective_chance


# ---------- EXIST COUNT DUEL DATA ----------
def games_random_exist_pet():
    """A random real pet with a known exist count (>0), for duels/trivia."""
    pets = [p for p in games_get_pets() if p.get("exist_count", 0) > 1000]
    if not pets:
        return None
    return secrets.choice(pets)



# ============================================================
# GAMES v4 — /hatch + /eggs (real eggs, real odds, icons, featured)
# ============================================================

@bot.tree.command(name="hatch", description="Hatch a REAL egg — real contents and odds", guild=guild_obj)
@app_commands.describe(egg="Egg name (default: featured egg)")
@app_commands.autocomplete(egg=games_egg_autocomplete)
async def games_hatch(interaction: discord.Interaction, egg: str = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer()
    eggs = games_get_eggs()
    if not eggs:
        return await interaction.followup.send("❌ No eggs available right now.", ephemeral=True)

    featured_slug = games_featured_egg()
    egg_def = None
    if egg:
        egg_def = next((e for e in eggs if e["name"].lower() == egg.lower()), None)
        if egg_def is None:
            return await interaction.followup.send("❌ Egg not found — try `/eggs` for the list.", ephemeral=True)
    else:
        egg_def = next((e for e in eggs if e["slug"] == featured_slug), None) or eggs[0]
    egg_name = egg_def["name"]
    is_featured = egg_def["slug"] == featured_slug

    free, used = games_free_use(interaction.user.id, "hatch")
    if not free:
        if not games_prepaid_consume(interaction.user.id, "hatch"):
            ok, res = games_coin_spend(interaction.user.id, GAMES_HATCH_COST, "hatch_extra", meta={"egg": egg_name})
            if not ok:
                return await interaction.followup.send(f"❌ {res}", ephemeral=True)

    if is_featured and egg_def.get("contents"):
        pet_name, tier, real_chance = games_hatch_roll_featured(egg_def)
    else:
        pet_name, tier, real_chance = games_hatch_roll(egg_def)
    games_coin_log_zero(interaction.user.id, "hatch", meta={"egg": egg_name, "free": free, "featured": is_featured})

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_pet_collections (discord_id, pet_key, count)
                VALUES (%s, %s, 1)
                ON CONFLICT (discord_id, pet_key) DO UPDATE SET count = mcwv_pet_collections.count + 1
            """, (interaction.user.id, pet_name))
        conn.commit()
    except Exception as exc:
        print(f"[games] collection upsert failed: {exc}")

    asset_id = games_pet_asset(pet_name)
    icon = await games_fetch_pet_icon(asset_id) if asset_id else None
    games_track(
        "hatch", interaction.channel_id,
        burned=0 if free or games_is_unlimited(interaction.user.id) else GAMES_HATCH_COST,
    )
    games_track_user("hatch", interaction.user.id, win=tier in ("titanic", "huge", "gargantuan"))

    tier_emoji, tier_label, tier_rgb = games_tier_style(tier)
    odds_txt = f"{real_chance:g}%" if real_chance is not None else "—"
    public = tier in ("titanic", "huge", "gargantuan")

    # real exist count + how many the user owns
    exist_count = 0
    owned = 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT exist_count FROM mcwv_game_pets WHERE LOWER(name) = LOWER(%s) LIMIT 1", (pet_name,))
            row = cur.fetchone()
            if row:
                exist_count = int(row[0] or 0)
            cur.execute("SELECT count FROM mcwv_pet_collections WHERE discord_id = %s AND pet_key = %s", (interaction.user.id, pet_name))
            row2 = cur.fetchone()
            if row2:
                owned = int(row2[0] or 0)
    except Exception:
        pass

    # egg icon (author avatar) + pet image (thumbnail) — ALWAYS attach something:
    # real icon when available, drawn placeholder otherwise. No broken images.
    files = []
    egg_icon = None
    if egg_def.get("icon_asset"):
        egg_icon = await games_fetch_pet_icon(egg_def["icon_asset"])
    if egg_icon:
        egg_file = games_icon_file(egg_icon, "egg.png", size=64)
        if egg_file:
            files.append(egg_file)
    else:
        ph = games_build_egg_placeholder(size=64)
        if ph:
            files.append(discord.File(ph, filename="egg.png"))
    if icon:
        pet_file = games_icon_file(icon, "pet.png", size=128)
        if pet_file:
            files.append(pet_file)
    if not any(f.filename == "pet.png" for f in files):
        ph = games_build_pet_placeholder(pet_name, tier, size=128)
        if ph:
            files.append(discord.File(ph, filename="pet.png"))

    embed = discord.Embed(
        title=f"{tier_emoji} {pet_name}",
        description=(
            f"{interaction.user.mention} hatched a **{tier_label}** from **{egg_name}**"
            + (" ✨ featured odds" if is_featured else "") + "!"
        ),
        color=discord.Color.from_rgb(*tier_rgb),
    )
    if any(f.filename == "egg.png" for f in files):
        embed.set_author(name=f"{egg_def.get('rarity') or 'Egg'} · {egg_name}", icon_url="attachment://egg.png")
    else:
        embed.set_author(name=f"🥚 {egg_name}" + (" ✨ featured today" if is_featured else ""))
    if any(f.filename == "pet.png" for f in files):
        embed.set_thumbnail(url="attachment://pet.png")
    embed.add_field(name="Rarity", value=f"{tier_emoji} **{tier_label}**", inline=True)
    embed.add_field(name="Real odds", value=f"**{odds_txt}**" + (" · doubled" if is_featured else ""), inline=True)
    embed.add_field(name="Your collection", value=f"×{owned}", inline=True)
    if exist_count:
        embed.add_field(name="Exists in game", value=f"**{exist_count:,}**", inline=True)
    embed.add_field(name="Egg", value=f"**{egg_name}** · {egg_def.get('rarity') or '—'}", inline=True)
    embed.add_field(name="Hatches today", value=f"{used}/{GAMES_HATCH_FREE_PER_DAY} free", inline=True)
    games_footer(embed, "Real contents & odds from the BIG Games database")

    # egg wiggle → crack → reveal animation (pet image only appears at the reveal)
    frames = ["🥚 ...", "🥚 *wiggle*", "🐣 *wiggle wiggle*", "💥"]
    msg = await interaction.followup.send(frames[0], ephemeral=not public)
    try:
        await games_animate(msg, frames[1:], delay=0.5)
        await msg.edit(content=None, embed=embed, attachments=files or None)
    except Exception as exc:
        print(f"[games] hatch animation failed: {exc}")
        try:
            await msg.edit(content=None, embed=embed, attachments=files or None)
        except Exception:
            pass


@bot.tree.command(name="eggs", description="Browse the REAL eggs (contents + real odds)", guild=guild_obj)
@app_commands.describe(egg="Egg name for contents + odds")
@app_commands.autocomplete(egg=games_egg_autocomplete)
async def games_eggs(interaction: discord.Interaction, egg: str = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    eggs = games_get_eggs()
    if not eggs:
        return await interaction.followup.send("No eggs synced yet — try `/gamesadmin sync` (owner).", ephemeral=True)

    featured_slug = games_featured_egg()
    if egg:
        target = next((e for e in eggs if e["name"].lower() == egg.lower()), None)
        if not target:
            return await interaction.followup.send("❌ Egg not found — try `/eggs`.", ephemeral=True)
        contents = target.get("contents") or []
        is_featured = target["slug"] == featured_slug
        embed = discord.Embed(
            title=f"🥚 {target['name']}" + (" ✨ FEATURED TODAY" if is_featured else ""),
            description=f"Rarity: **{target.get('rarity') or '—'}**" + ("\n*Top-tier odds are doubled today!*" if is_featured else ""),
            color=games_color("violet"),
        )
        if contents:
            top12 = sorted(contents, key=lambda c: c[1])[:12]
            for pname, chance in top12:
                emo, tier_lbl = games_tier_from_chance(chance, pname, target)
                embed.add_field(
                    name=f"{emo} {pname}",
                    value=f"`{chance:g}%` `{games_bar(chance, 100, 8)}` {tier_lbl}",
                    inline=True,
                )
            if len(contents) > 12:
                embed.set_footer(text=f"+{len(contents) - 12} more · {len(contents)} possible hatches · /hatch {target['name']}")
        else:
            embed.add_field(name="Contents", value="Not exposed for this egg.", inline=False)
        egg_file = None
        egg_icon_bytes = None
        if target.get("icon_asset"):
            egg_icon_bytes = await games_fetch_pet_icon(target["icon_asset"])
        if egg_icon_bytes:
            egg_file = games_icon_file(egg_icon_bytes, "egg.png", size=128)
        if egg_file is None:
            ph = games_build_egg_placeholder(size=128)
            if ph:
                egg_file = discord.File(ph, filename="egg.png")
        if egg_file:
            embed.set_thumbnail(url="attachment://egg.png")
        if not (contents and len(contents) > 12):
            embed.set_footer(text=f"{len(contents)} possible hatches · 3 free daily · /hatch {target['name']}")
        return await interaction.followup.send(embed=embed, file=egg_file, ephemeral=True)

    with_contents = [e for e in eggs if e.get("contents")]
    featured = next((e for e in eggs if e["slug"] == featured_slug), None)
    lines = []
    if featured:
        lines.append(f"✨ **{featured['name']}** — FEATURED TODAY (double top odds)")
    for e in with_contents[:12]:
        if featured and e["slug"] == featured["slug"]:
            continue
        n = len(e["contents"])
        lines.append(f"🥚 **{e['name']}** — {e.get('rarity') or '?'} · {n} pets")
    embed = discord.Embed(
        title="🥚 Real Eggs — BIG Games database",
        description="\n".join(lines) or "No eggs synced.",
        color=games_color("violet"),
    )
    feat_file = None
    if featured and featured.get("icon_asset"):
        feat_bytes = await games_fetch_pet_icon(featured["icon_asset"])
        if feat_bytes:
            feat_file = games_icon_file(feat_bytes, "feat.png", size=96)
    if feat_file is None:
        ph = games_build_egg_placeholder(size=96)
        if ph:
            feat_file = discord.File(ph, filename="feat.png")
    if feat_file:
        embed.set_thumbnail(url="attachment://feat.png")
    embed.set_footer(text=f"{len(eggs)} eggs total · /eggs <name> for contents + odds · /hatch to open")
    await interaction.followup.send(embed=embed, file=feat_file, ephemeral=True)



# ============================================================
# GAMES v4 — LEADERBOARDS, USER STATS, STREAKS + ROLES, LOTTERY TABLE
# ============================================================

def games_track_user(game, user_id, win=False):
    """Per-user game stats (for leaderboards + streaks)."""
    try:
        if not db_enabled():
            return
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_user_stats (discord_id, game, wins, plays)
                VALUES (%s, %s, %s, 1)
                ON CONFLICT (discord_id, game) DO UPDATE SET
                    wins = mcwv_user_stats.wins + EXCLUDED.wins,
                    plays = mcwv_user_stats.plays + 1
            """, (int(user_id), str(game)[:40], 1 if win else 0))
        conn.commit()
    except Exception as exc:
        print(f"[games] track_user failed: {exc}")


def games_track_participants(game, user_ids, winner_id=None):
    """Record one play per participant and at most one winner in one transaction."""
    ids = sorted({int(uid) for uid in (user_ids or []) if uid})
    if not ids or not db_enabled():
        return
    try:
        with conn:
            with conn.cursor() as cur:
                for uid in ids:
                    cur.execute("""
                        INSERT INTO mcwv_user_stats (discord_id, game, wins, plays)
                        VALUES (%s, %s, %s, 1)
                        ON CONFLICT (discord_id, game) DO UPDATE SET
                            wins = mcwv_user_stats.wins + EXCLUDED.wins,
                            plays = mcwv_user_stats.plays + 1
                    """, (uid, str(game)[:40], 1 if winner_id and uid == int(winner_id) else 0))
    except Exception as exc:
        print(f"[games] participant stats failed: {exc}")


def games_user_wins(user_id, game):
    try:
        if not db_enabled():
            return 0
        with conn.cursor() as cur:
            cur.execute("SELECT wins FROM mcwv_user_stats WHERE discord_id = %s AND game = %s", (int(user_id), str(game)))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def games_role_id(kind):
    key = {"duelist": "games_role_duelist", "petmaster": "games_role_petmaster", "towermaster": "games_role_towermaster"}.get(kind)
    if not key:
        return None
    raw = db_get_setting(key, "0")
    try:
        rid = int(raw or 0)
        return rid if rid > 0 else None
    except Exception:
        return None


async def games_grant_role(guild, user_id, kind):
    rid = games_role_id(kind)
    if not rid or not guild:
        return
    try:
        role = guild.get_role(rid)
        if role is None:
            return
        member = guild.get_member(int(user_id))
        if member is None:
            member = await guild.fetch_member(int(user_id))
        if member and role not in member.roles:
            await member.add_roles(role, reason=f"MCWV games: {kind}")
    except Exception as exc:
        print(f"[games] role grant {kind} failed: {exc}")


@bot.tree.command(name="top", description="Coin leaderboard — who's richest", guild=guild_obj)
@app_commands.describe(games="Optional: game wins leaderboard instead")
@app_commands.choices(games=[
    app_commands.Choice(name="Guess the Pet", value="guess"),
    app_commands.Choice(name="Scramble", value="scramble"),
    app_commands.Choice(name="Hangman", value="hangman"),
    app_commands.Choice(name="Trivia", value="trivia"),
    app_commands.Choice(name="History Trivia", value="historytrivia"),
    app_commands.Choice(name="Petdle", value="petdle"),
    app_commands.Choice(name="Spin", value="spin"),
    app_commands.Choice(name="Scratch", value="scratch"),
    app_commands.Choice(name="Hatching", value="hatch"),
    app_commands.Choice(name="Cases", value="case"),
    app_commands.Choice(name="Duels", value="duel"),
    app_commands.Choice(name="Tower", value="tower"),
])
async def games_top(interaction: discord.Interaction, games: str = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        embed = await games_top_embed(interaction.user.id, games)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"❌ Leaderboard failed: `{type(exc).__name__}`", ephemeral=True)


# ---------- LOTTERY TICKETS TABLE (proper ownership) ----------
def games_lottery_next_draw(now_dt=None):
    now_dt = now_dt or datetime.now(timezone.utc)
    days_ahead = (6 - now_dt.weekday()) % 7
    draw = (now_dt + timedelta(days=days_ahead)).replace(hour=20, minute=0, second=0, microsecond=0)
    if draw <= now_dt:
        draw += timedelta(days=7)
    return draw


def games_lottery_round_key(now_dt=None):
    """Tickets belong to the next undrawn Sunday round."""
    draw = games_lottery_next_draw(now_dt)
    key = draw.strftime("%Y-%m-%d")
    try:
        if db_enabled():
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM mcwv_lottery_draws WHERE round_key = %s", (key,))
                if cur.fetchone():
                    key = (draw + timedelta(days=7)).strftime("%Y-%m-%d")
    except Exception:
        pass
    return key


def games_lottery_purchase(user_id, tickets):
    """Charge, issue tickets and grow the pool in one transaction."""
    uid, tickets = int(user_id), int(tickets)
    if tickets < 1 or tickets > GAMES_LOTTERY_WEEKLY_TICKET_CAP:
        return False, "Invalid ticket amount."
    cost = tickets * GAMES_LOTTERY_TICKET_COST
    round_key = games_lottery_round_key()
    virtual = games_is_unlimited(uid)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                    (uid,),
                )
                cur.execute("""
                    INSERT INTO mcwv_lottery_tickets (user_id, week, tickets)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, week) DO UPDATE
                    SET tickets = mcwv_lottery_tickets.tickets + EXCLUDED.tickets
                    WHERE mcwv_lottery_tickets.tickets + EXCLUDED.tickets <= %s
                    RETURNING tickets
                """, (uid, round_key, tickets, GAMES_LOTTERY_WEEKLY_TICKET_CAP))
                ticket_row = cur.fetchone()
                if ticket_row is None:
                    raise ValueError(f"Weekly cap is {GAMES_LOTTERY_WEEKLY_TICKET_CAP} tickets.")
                total_owned = int(ticket_row[0])
                if virtual:
                    cur.execute("SELECT balance FROM mcwv_coins WHERE discord_id = %s", (uid,))
                    balance_after = int(cur.fetchone()[0] or 0)
                else:
                    cur.execute(
                        """UPDATE mcwv_coins
                           SET balance = balance - %s, total_spent = total_spent + %s
                           WHERE discord_id = %s AND balance >= %s RETURNING balance""",
                        (cost, cost, uid, cost),
                    )
                    paid = cur.fetchone()
                    if paid is None:
                        raise ValueError("Not enough coins.")
                    balance_after = int(paid[0])
                cur.execute(
                    "INSERT INTO settings (key, value) VALUES (%s, '0') ON CONFLICT (key) DO NOTHING",
                    (GAMES_SETTING_LOTTERY_POOL,),
                )
                cur.execute(
                    """UPDATE settings
                       SET value = (COALESCE(NULLIF(value, ''), '0')::bigint + %s)::text
                       WHERE key = %s RETURNING value""",
                    (cost, GAMES_SETTING_LOTTERY_POOL),
                )
                pool = int(cur.fetchone()[0])
                cur.execute(
                    """INSERT INTO mcwv_coin_log
                       (actor_id, target_id, type, amount, balance_after, meta)
                       VALUES (%s,%s,%s,%s,%s,%s::jsonb)""",
                    (uid, uid, "lottery_ticket_test" if virtual else "lottery_ticket",
                     0 if virtual else -cost, balance_after,
                     json.dumps({"tickets": tickets, "round": round_key, "virtual": virtual})),
                )
        return True, {"owned": total_owned, "pool": pool, "cost": cost, "round": round_key}
    except ValueError as exc:
        return False, str(exc)
    except Exception as exc:
        print(f"[games] lottery purchase failed: {exc}")
        return False, f"{type(exc).__name__}: {exc}"


def games_lottery_pool():
    try:
        return int(db_get_setting(GAMES_SETTING_LOTTERY_POOL, "0") or 0)
    except Exception:
        return 0


def games_lottery_owned(user_id):
    try:
        round_key = games_lottery_round_key()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tickets FROM mcwv_lottery_tickets WHERE user_id = %s AND week = %s",
                (int(user_id), round_key),
            )
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def games_lottery_draw_sync():
    """Exactly-once draw with the pool, payout and ticket deletion in one transaction."""
    worker = games_new_db_connection()
    if worker is None:
        return None
    try:
        with worker:
            with worker.cursor() as cur:
                cur.execute(
                    "INSERT INTO settings (key, value) VALUES (%s, '0') ON CONFLICT (key) DO NOTHING",
                    (GAMES_SETTING_LOTTERY_POOL,),
                )
                cur.execute("SELECT value FROM settings WHERE key = %s FOR UPDATE", (GAMES_SETTING_LOTTERY_POOL,))
                pool = int((cur.fetchone() or [0])[0] or 0)
                if pool <= 0:
                    return None
                # Oldest ticket round wins; this also safely drains pre-v5 test rounds.
                cur.execute("""
                    SELECT t.week FROM mcwv_lottery_tickets t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM mcwv_lottery_draws d WHERE d.round_key = t.week
                    )
                    GROUP BY t.week ORDER BY MIN(t.created_at) LIMIT 1
                """)
                round_row = cur.fetchone()
                if not round_row:
                    return None  # never erase a pool that has no matching entries
                round_key = str(round_row[0])
                cur.execute("SELECT 1 FROM mcwv_lottery_draws WHERE round_key = %s", (round_key,))
                if cur.fetchone():
                    return None
                cur.execute(
                    "SELECT user_id, tickets FROM mcwv_lottery_tickets WHERE week = %s FOR UPDATE",
                    (round_key,),
                )
                entries = [(int(uid), int(t)) for uid, t in cur.fetchall() if int(t) > 0]
                total_tickets = sum(t for _uid, t in entries)
                if total_tickets <= 0:
                    return None
                roll = secrets.randbelow(total_tickets)
                winner = entries[-1][0]
                running = 0
                for uid, count in entries:
                    running += count
                    if roll < running:
                        winner = uid
                        break
                prize = int(pool * 0.70)
                burned = pool - prize
                cur.execute(
                    "INSERT INTO mcwv_coins (discord_id) VALUES (%s) ON CONFLICT (discord_id) DO NOTHING",
                    (winner,),
                )
                cur.execute(
                    """UPDATE mcwv_coins
                       SET balance = balance + %s, total_earned = total_earned + %s
                       WHERE discord_id = %s RETURNING balance""",
                    (prize, prize, winner),
                )
                balance_after = int(cur.fetchone()[0])
                cur.execute(
                    """INSERT INTO mcwv_coin_log
                       (actor_id, target_id, type, amount, balance_after, meta)
                       VALUES (%s,%s,'lottery_win',%s,%s,%s::jsonb)""",
                    (winner, winner, prize, balance_after,
                     json.dumps({"pool": pool, "round": round_key, "tickets": total_tickets})),
                )
                cur.execute("UPDATE settings SET value = '0' WHERE key = %s", (GAMES_SETTING_LOTTERY_POOL,))
                cur.execute("DELETE FROM mcwv_lottery_tickets WHERE week = %s", (round_key,))
                cur.execute(
                    "INSERT INTO mcwv_lottery_draws (round_key, pool, winner_id, prize) VALUES (%s,%s,%s,%s)",
                    (round_key, pool, winner, prize),
                )
                cur.execute("""
                    INSERT INTO mcwv_game_stats (game, sessions, coins_minted, coins_burned, last_played)
                    VALUES ('lottery', 1, 0, %s, NOW())
                    ON CONFLICT (game) DO UPDATE SET
                        sessions = mcwv_game_stats.sessions + 1,
                        coins_burned = mcwv_game_stats.coins_burned + EXCLUDED.coins_burned,
                        last_played = NOW()
                """, (burned,))
        return winner, prize, pool, round_key
    except Exception as exc:
        print(f"[games] lottery draw failed: {exc}")
        return None
    finally:
        worker.close()


async def games_lottery_draw(channel=None):
    result = await asyncio.to_thread(games_lottery_draw_sync)
    if result is None:
        if channel:
            embed = discord.Embed(
                title="🎟 Lottery",
                description="No drawable pool and ticket round yet — existing coins were left untouched.",
                color=games_color("slate"),
            )
            await channel.send(embed=embed)
        return None
    winner, prize, pool, round_key = result
    if channel:
        embed = discord.Embed(
            title="🎟 Lottery Drawn!",
            description=f"🎆 <@{winner}> wins **{prize:,}** 🪙 — 70% of the **{pool:,}** pool!",
            color=games_color("gold"),
        )
        embed.add_field(name="Winner", value=f"<@{winner}>", inline=True)
        embed.add_field(name="Prize", value=f"**{prize:,}** 🪙", inline=True)
        embed.add_field(name="Round", value=f"`{round_key}`", inline=True)
        games_footer(embed, "Next draw Sunday 20:00 UTC · /lottery buy")
        await channel.send(embed=embed)
    return result


async def games_lottery_draw_async(channel):
    """Alias for the /lottery draw action (owner)."""
    return await games_lottery_draw(channel)


# ---------- STREAK ROLES: duelist ----------
async def games_check_duelist_role(guild, winner_id):
    wins = games_user_wins(winner_id, "duel")
    if wins > 0 and wins % 5 == 0:
        await games_grant_role(guild, winner_id, "duelist")


# ---------- TOWER MASTER: top-3 all-time ----------
async def games_check_tower_master(guild, user_id):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT discord_id FROM mcwv_tower_scores
                ORDER BY best_floor DESC, best_score DESC LIMIT 3
            """)
            top3 = {int(r[0]) for r in cur.fetchall()}
        if int(user_id) in top3:
            await games_grant_role(guild, user_id, "towermaster")
    except Exception as exc:
        print(f"[games] tower master check failed: {exc}")


# ---------- PET MASTER: monthly top guesser (housekeeping job) ----------
async def games_monthly_petmaster(guild):
    try:
        last = db_get_setting("games_petmaster_month", "")
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        if not last:
            # Establish a baseline; don't crown/reset mid-month on first deploy.
            db_set_setting("games_petmaster_month", month)
            return
        if last == month:
            return
        prev_holder = db_get_setting("games_petmaster_holder", "0")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT target_id, COUNT(*) AS wins
                FROM mcwv_coin_log
                WHERE type = 'guess_win'
                  AND created_at >= date_trunc('month', NOW()) - INTERVAL '1 month'
                  AND created_at < date_trunc('month', NOW())
                GROUP BY target_id
                ORDER BY wins DESC, target_id ASC LIMIT 1
            """)
            row = cur.fetchone()
        if not row or int(row[1] or 0) <= 0:
            db_set_setting("games_petmaster_month", month)
            return
        new_holder = int(row[0])
        # remove from previous holder, grant to new
        try:
            prev_role = games_role_id("petmaster")
            if prev_role and prev_holder and int(prev_holder) > 0:
                prev_member = guild.get_member(int(prev_holder))
                if prev_member:
                    role = guild.get_role(prev_role)
                    if role and role in prev_member.roles:
                        await prev_member.remove_roles(role, reason="Pet Master: new monthly winner")
        except Exception as exc:
            print(f"[games] petmaster prev removal failed: {exc}")
        await games_grant_role(guild, new_holder, "petmaster")
        db_set_setting("games_petmaster_holder", str(new_holder))
        db_set_setting("games_petmaster_month", month)
        try:
            chans = json.loads(db_get_setting(GAMES_SETTING_SPAWN_CHANNELS, "[]") or "[]")
            channel = bot.get_channel(int(chans[0])) if chans else None
            if channel:
                await channel.send(f"🐾 **Pet Master of the month:** <@{new_holder}> — most Guess-the-Pet wins! The crown moves on.")
        except Exception:
            pass
    except Exception as exc:
        print(f"[games] monthly petmaster failed: {exc}")



# ============================================================
# GAMES v4 — POLISH: animations, hints, chat answers, admin hub
# ============================================================

# ---------- WHEEL ANIMATION (3 frames) ----------
def games_build_wheel_frame(win_idx, spin_degrees):
    """Wheel frame with an extra visual rotation (for the animation)."""
    return _games_wheel_render(win_idx, spin_degrees)


# ---------- SCRATCH IMAGE STRIP ----------
async def games_build_scratch_strip(pet_names, covered=False):
    # icons aligned to pet_names; missing icons stay None → drawn placeholder cell
    icons = []
    if not covered:
        any_icon = False
        for name in pet_names:
            asset = games_pet_asset(name)
            img = None
            if asset:
                icon = await games_fetch_pet_icon(asset)
                if icon:
                    try:
                        img = Image.open(BytesIO(icon)).convert("RGBA").resize((160, 160), Image.Resampling.LANCZOS)
                        any_icon = True
                    except Exception:
                        img = None
            icons.append(img)
        if not any_icon:
            icons = [None] * len(pet_names)
    pad = 24
    n = 3
    W = n * 160 + (n + 1) * pad
    H = 200
    strip = Image.new("RGBA", (W, H), (15, 17, 30, 255))
    d = ImageDraw.Draw(strip)
    try:
        qf = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 72)
    except Exception:
        qf = ImageFont.load_default()
    for i in range(n):
        x0 = pad + i * (160 + pad)
        d.rounded_rectangle((x0, 20, x0 + 160, 180), radius=16, fill=(26, 29, 48, 255),
                            outline=(245, 200, 66, 170), width=3)
        if covered:
            # hatch pattern + question mark, CLIPPED to this cell only
            cell_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            cd = ImageDraw.Draw(cell_layer)
            for k in range(-200, 220, 14):
                cd.line((x0 + k, 20, x0 + k + 200, 180), fill=(62, 68, 100, 255), width=2)
            bb = cd.textbbox((0, 0), "?", font=qf)
            tw, th = bb[2] - bb[0], bb[3] - bb[1]
            cd.text((x0 + (160 - tw) // 2, 20 + (160 - th) // 2 - 8), "?", font=qf, fill=(128, 134, 168, 255))
            mask = Image.new("L", (W, H), 0)
            ImageDraw.Draw(mask).rounded_rectangle((x0, 20, x0 + 160, 180), radius=16, fill=255)
            # multiply (not replace): keep the drawn pixels' alpha, clip to the cell
            cell_layer.putalpha(ImageChops.multiply(cell_layer.split()[3], mask))
            strip.alpha_composite(cell_layer)
        elif i < len(icons) and icons[i] is not None:
            mask = Image.new("L", (160, 160), 0)
            ImageDraw.Draw(mask).rounded_rectangle((0, 0, 160, 160), radius=16, fill=255)
            strip.paste(icons[i], (x0, 20), mask)
        elif i < len(icons):
            # missing icon → drawn placeholder cell (never a blank slot)
            bb = d.textbbox((0, 0), "?", font=qf)
            tw, th = bb[2] - bb[0], bb[3] - bb[1]
            d.text((x0 + (160 - tw) // 2, 20 + (160 - th) // 2 - 8), "?", font=qf, fill=(128, 134, 168, 255))
    buf = BytesIO()
    strip.convert("RGB").save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf


# ---------- GUESS HINTS ----------
async def games_post_hint(channel, round_info):
    """Progressive clue with a reduced live reward; called by the exact round clock."""
    try:
        pet = str(round_info.get("pet_name", ""))
        words = [word for word in re.findall(r"[A-Za-z0-9]+", pet) if word]
        if not words:
            return
        step = int(round_info.get("hint_step", 0))
        if step >= len(GAMES_GUESS_HINT_TIMES):
            return
        round_info["hint_step"] = step + 1  # claim before awaiting so a fallback cannot duplicate it
        if step == 0:
            clue = " ".join(word[0].upper() + "_" * (len(word) - 1) for word in words)
            detail = f"**{len(words)} word{'s' if len(words) != 1 else ''}** · **{sum(len(w) for w in words)} letters**"
            title = "💡 Hint 1 — Shape of the name"
        else:
            clue_words = []
            for word in words:
                clue_words.append("".join(ch.upper() if i % 2 == 0 else "_" for i, ch in enumerate(word)))
            clue = " ".join(clue_words)
            detail = "Every other letter has been revealed — plus a clearer image."
            title = "🔦 Final Hint — Closer look"
        current_max = games_guess_reward(round_info, time.time() - float(round_info["started"]), 0)
        embed = discord.Embed(
            title=title,
            description=f"`{clue}`\n{detail}",
            color=games_color("cyan"),
        )
        embed.add_field(
            name="Base reward now",
            value=(f"**{current_max:,}** 🪙 before personal streak bonus"
                   if round_info.get("rewarded", True) else "Practice round"),
            inline=True,
        )
        embed.add_field(
            name="Attempts",
            value=f"**{GAMES_MAX_ANSWER_ATTEMPTS}** valid pet guesses per player",
            inline=True,
        )
        games_footer(embed, "Only real pet names consume attempts · 🔥 close · 🟡 warm · ❌ cold")
        file = None
        if step == 1 and round_info.get("icon"):
            reveal = games_build_round_image(round_info["icon"], "reveal")
            if reveal:
                file = discord.File(reveal, filename="hint_pet.png")
                embed.set_thumbnail(url="attachment://hint_pet.png")
        await channel.send(embed=embed, file=file)
    except Exception as exc:
        print(f"[games] hint failed: {exc}")


# ---------- TOWER CHAT ANSWERS ----------
async def games_handle_tower_answer(message):
    session = ACTIVE_TOWER.get(message.author.id)
    if not session or not session.get("active"):
        return False
    if session.get("kind") != "guess" or message.channel.id != session.get("channel_id"):
        return False
    correct = games_answers_match(message.content, session.get("answer", ""))
    if not correct:
        # Ignore ordinary chat. A wrong answer only counts if it is another real pet name.
        candidate = normalize_answer(message.content)
        known = {normalize_answer(p) for p in games_guess_pet_pool()}
        if candidate not in known:
            return False
    session["kind"] = None
    await games_tower_result_from_chat(message.channel, message.author, correct)
    return True


async def games_tower_finish(channel, user, session, reached_roof=False):
    timeout_task = ACTIVE_TOWER_TIMEOUT_TASKS.pop(int(user.id), None)
    if timeout_task is not None and timeout_task is not asyncio.current_task():
        timeout_task.cancel()
    session["active"] = False
    reached = min(int(session["floor"]) - 1, GAMES_TOWER_MAX_FLOOR)
    best = 0
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mcwv_tower_scores (discord_id, best_floor, best_score, runs)
                VALUES (%s,%s,%s,1)
                ON CONFLICT (discord_id) DO UPDATE SET
                    best_floor = GREATEST(mcwv_tower_scores.best_floor, EXCLUDED.best_floor),
                    best_score = GREATEST(mcwv_tower_scores.best_score, EXCLUDED.best_score),
                    runs = mcwv_tower_scores.runs + 1, reached_at = NOW()
                RETURNING best_floor
            """, (user.id, reached, session["score"]))
            best = int(cur.fetchone()[0] or 0)
        conn.commit()
    except Exception as exc:
        print(f"[games] tower score save failed: {exc}")
    games_track("tower", getattr(channel, "id", 0), minted=session["score"])
    games_track_user("tower", user.id, win=reached_roof or reached >= 5)
    try:
        guild = bot.get_guild(GUILD_ID)
        if guild:
            await games_check_tower_master(guild, user.id)
    except Exception:
        pass
    ACTIVE_TOWER.pop(user.id, None)
    embed = discord.Embed(
        title="👑 Tower Conquered!" if reached_roof else "💀 Tower Run Over",
        description=(
            f"<@{user.id}> reached the roof at floor **{GAMES_TOWER_MAX_FLOOR}**!"
            if reached_roof else f"<@{user.id}> fell on floor **{session['floor']}**."
        ),
        color=games_color("gold" if reached_roof else "red"),
    )
    embed.add_field(name="Reached floor", value=f"**{reached}**", inline=True)
    embed.add_field(name="Score", value=f"**{session['score']:,}** 🪙", inline=True)
    embed.add_field(name="Personal best", value=f"**{best}**", inline=True)
    games_footer(embed, "3 runs per day · /tower to climb again")
    await channel.send(embed=embed)


async def games_tower_result_from_chat(channel, user, correct):
    session = ACTIVE_TOWER.get(user.id)
    if not session:
        return
    if correct:
        cleared_floor = session["floor"]
        session["combo"] = int(session.get("combo", 0)) + 1
        combo_bonus = min(max(session["combo"] - 1, 0) * 10, 50)
        award = cleared_floor * 25 + combo_bonus
        session["floor"] += 1
        session["score"] += award
        games_coin_adjust(user.id, award, "tower_floor", meta={"floor": cleared_floor, "combo": session["combo"]})
        embed = discord.Embed(
            title=f"✅ Floor {cleared_floor} Clear!",
            description=(f"<@{user.id}> reached the roof!" if cleared_floor >= GAMES_TOWER_MAX_FLOOR
                         else f"<@{user.id}> climbs to floor **{session['floor']}**…"),
            color=games_color("green"),
        )
        embed.add_field(name="Reward", value=f"+`{award}` 🪙", inline=True)
        embed.add_field(name="Combo", value=f"🔥 **×{session['combo']}**" + (f" · +{combo_bonus}" if combo_bonus else ""), inline=True)
        embed.add_field(name="Score", value=f"**{session['score']:,}**", inline=True)
        await channel.send(embed=embed)
        if cleared_floor >= GAMES_TOWER_MAX_FLOOR:
            await games_tower_finish(channel, user, session, reached_roof=True)
        else:
            await games_tower_ask_chat(channel, user.id)
    else:
        session["combo"] = 0
        session["hearts"] -= 1
        if session["hearts"] <= 0:
            await games_tower_finish(channel, user, session, reached_roof=False)
        else:
            embed = discord.Embed(
                title="💔 Wrong Answer",
                description=f"<@{user.id}> lost a heart — **{session['hearts']}** left. Next floor:",
                color=games_color("pink"),
            )
            await channel.send(embed=embed)
            await games_tower_ask_chat(channel, user.id)


async def games_tower_timeout(user_id, session):
    """Apply one missed-floor penalty and keep abandoned runs from getting stuck."""
    if not session.get("active") or session.get("kind") is None:
        return
    channel = bot.get_channel(int(session.get("channel_id", 0)))
    if channel is None:
        ACTIVE_TOWER.pop(int(user_id), None)
        return
    session["kind"] = None
    try:
        await channel.send(f"⏰ <@{int(user_id)}> ran out of time on floor **{session['floor']}**.")
    except Exception:
        pass
    user = bot.get_user(int(user_id)) or discord.Object(id=int(user_id))
    await games_tower_result_from_chat(channel, user, False)


async def games_tower_timeout_after(user_id, floor_started):
    """Apply the floor deadline on time; housekeeping remains a fallback."""
    user_id = int(user_id)
    try:
        await asyncio.sleep(GAMES_TOWER_FLOOR_TIMEOUT)
        session = ACTIVE_TOWER.get(user_id)
        if not session or not session.get("active") or session.get("kind") is None:
            return
        if float(session.get("floor_started", 0)) != float(floor_started):
            return
        await games_tower_timeout(user_id, session)
    except asyncio.CancelledError:
        return
    except Exception as exc:
        print(f"[games] scheduled tower timeout failed: {exc}")
    finally:
        task = ACTIVE_TOWER_TIMEOUT_TASKS.get(user_id)
        if task is asyncio.current_task():
            ACTIVE_TOWER_TIMEOUT_TASKS.pop(user_id, None)


def _tower_floor_fields(embed, session):
    embed.add_field(name="Floor", value=f"**{session['floor']}**", inline=True)
    embed.add_field(name="Hearts", value=games_hearts(session.get("hearts", 3), 3), inline=True)
    embed.add_field(
        name="Score",
        value=f"**{session.get('score', 0):,}** 🪙 · combo ×{session.get('combo', 0)}",
        inline=True,
    )


async def games_tower_ask_chat(channel, user_id):
    session = ACTIVE_TOWER.get(user_id)
    if not session or not session.get("active"):
        return
    session["channel_id"] = channel.id
    session["floor_started"] = time.time()
    if secrets.choice([True, False]):
        # trivia floor in chat: post question, answer via /toweranswer
        q, options, correct = games_pick_random(GAMES_TRIVIA_SEED, scope=f"tower_trivia:{user_id}", max_recent=8)
        session["kind"] = "trivia"
        session["trivia_answer"] = options[correct]
        session["trivia_options"] = options
        embed = discord.Embed(title=f"🏗 Floor {session['floor']} — Trivia", description=q, color=games_color("indigo"))
        for i, opt in enumerate(options):
            embed.add_field(name=f"{GAMES_ABCD[i]}", value=opt, inline=False)
        _tower_floor_fields(embed, session)
        embed.set_footer(text=f"<@{user_id}> answer with /toweranswer <letter or text> · {GAMES_TOWER_FLOOR_TIMEOUT}s")
        await channel.send(embed=embed)
    else:
        pets = games_guess_pet_pool()
        pet = games_pick_random(pets, scope=f"tower_pet:{user_id}", max_recent=8)
        session["answer"] = pet
        session["kind"] = "guess"
        asset = games_pet_asset(pet)
        icon = await games_fetch_pet_icon(asset) if asset else None
        file = None
        if icon:
            buf = games_build_round_image(icon, "zoom")
            if buf:
                file = discord.File(buf, filename="tower.png")
        embed = discord.Embed(title=f"🏗 Floor {session['floor']} — Name this pet!", color=games_color("purple"))
        if file is None:
            # no icon available → scrambled letters so the floor is still playable
            letters = list(re.sub(r"[^A-Za-z]", "", str(pet)).lower())
            shuffled = letters[:]
            while len(shuffled) > 1 and shuffled == letters:
                secrets.SystemRandom().shuffle(shuffled)
            embed.description = f"No image this time — unscramble: `{' '.join(shuffled).upper()}`"
        _tower_floor_fields(embed, session)
        embed.set_footer(text=f"<@{user_id}> type the pet name in chat · {GAMES_TOWER_FLOOR_TIMEOUT}s")
        await channel.send(embed=embed, file=file)
    old_task = ACTIVE_TOWER_TIMEOUT_TASKS.pop(int(user_id), None)
    if old_task is not None and old_task is not asyncio.current_task():
        old_task.cancel()
    task = asyncio.create_task(games_tower_timeout_after(user_id, session["floor_started"]))
    ACTIVE_TOWER_TIMEOUT_TASKS[int(user_id)] = task



# ============================================================
# GAMES v4 — PLAYER HUB + ADMIN + LOTTERY + TOWER + HISTORY TRIVIA
# ============================================================

# ---------- /games (player hub) ----------
class GamesHubView(discord.ui.View):
    """Quick-play buttons wired straight into the real game commands."""

    def __init__(self, owner_id):
        super().__init__(timeout=600)
        self._owner = int(owner_id)
        self.message = None

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    async def _run(self, interaction, cmd, *args):
        if interaction.user.id != self._owner:
            await interaction.response.send_message("This hub isn't yours — run `/games`.", ephemeral=True)
            return
        try:
            await cmd(interaction, *args)
        except Exception as exc:
            print(f"[games] hub quick-play failed: {exc}")
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ That game failed to start — try the command directly.", ephemeral=True)
            else:
                await interaction.followup.send("❌ That game failed to start — try the command directly.", ephemeral=True)

    @discord.ui.button(label="📅 Daily", style=discord.ButtonStyle.success, row=0)
    async def b_daily(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_daily.callback)

    @discord.ui.button(label="🥚 Hatch", style=discord.ButtonStyle.primary, row=0)
    async def b_hatch(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_hatch.callback, None)

    @discord.ui.button(label="🎡 Spin", style=discord.ButtonStyle.primary, row=0)
    async def b_spin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_spin.callback)

    @discord.ui.button(label="🎴 Scratch", style=discord.ButtonStyle.primary, row=0)
    async def b_scratch(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_scratch.callback)

    @discord.ui.button(label="🏆 Top", style=discord.ButtonStyle.secondary, row=0)
    async def b_top(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_top.callback, None)

    @discord.ui.button(label="🧠 Trivia", style=discord.ButtonStyle.primary, row=1)
    async def b_trivia(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_trivia.callback)

    @discord.ui.button(label="🐾 Petdle", style=discord.ButtonStyle.primary, row=1)
    async def b_petdle(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_petdle.callback, None)

    @discord.ui.button(label="🏗 Tower", style=discord.ButtonStyle.primary, row=1)
    async def b_tower(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_tower.callback)

    @discord.ui.button(label="🛒 Shop", style=discord.ButtonStyle.secondary, row=1)
    async def b_shop(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._run(interaction, games_shop.callback)


@bot.tree.command(name="games", description="The MCWV games hub — how everything works", guild=guild_obj)
async def games_hub(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    embed = await games_hub_embed(interaction.user)
    view = GamesHubView(interaction.user.id)
    games_footer(embed, "Quick-play buttons below · full rules in /gamesguide")
    view.message = await interaction.followup.send(embed=embed, view=view, ephemeral=True, wait=True)


def games_staff_embed(guild, notice=None):
    role_ids = games_staff_role_ids()
    role_lines = []
    for role_id in sorted(role_ids):
        role = guild.get_role(role_id) if guild else None
        role_lines.append(f"• {role.mention} — `{role_id}`" if role else f"• Deleted/missing role — `{role_id}`")
    embed = discord.Embed(
        title="🎮 Game Staff Roles",
        description=(
            (f"✅ {notice}\n\n" if notice else "")
            + "Choose one or more roles below. Members with any selected role can:\n"
              "• Open and use `/caseadmin`\n"
              "• Start and stop manual `/guess` rounds\n\n"
              "They **cannot** use `/gamesadmin`, `/coinsadmin`, economy resets, or manual lottery draws."
        ),
        color=games_color("purple"),
    )
    embed.add_field(
        name=f"Current roles ({len(role_ids)})",
        value="\n".join(role_lines) if role_lines else "**Owner only** — no game-staff roles selected.",
        inline=False,
    )
    games_footer(embed, "Owner-only configuration · saved in PostgreSQL")
    return embed


class GameStaffRoleSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(
            placeholder="Select the game staff role(s)…",
            min_values=1,
            max_values=25,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, GameStaffRoleView):
            return await interaction.response.send_message("This picker expired.", ephemeral=True)
        roles = list(self.values)
        invalid = [role for role in roles if role.is_default() or role.managed]
        if invalid:
            names = ", ".join(role.name for role in invalid[:5])
            return await interaction.response.send_message(
                f"❌ `@everyone` and managed/integration roles cannot be game staff: {names}", ephemeral=True
            )
        ok, result = games_set_staff_role_ids(role.id for role in roles)
        if not ok:
            return await interaction.response.send_message(f"❌ Could not save roles: `{result}`", ephemeral=True)
        new_view = GameStaffRoleView(view.owner_id)
        new_view.message = interaction.message
        await interaction.response.edit_message(
            embed=games_staff_embed(interaction.guild, f"Saved {len(result)} game-staff role(s)."),
            view=new_view,
        )


class GameStaffRoleView(discord.ui.View):
    def __init__(self, owner_id):
        super().__init__(timeout=300)
        self.owner_id = int(owner_id)
        self.message = None
        self.add_item(GameStaffRoleSelect())

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.owner_id or not games_owner_check(interaction.user):
            await interaction.response.send_message("❌ Only the owner who opened this panel can edit game staff.", ephemeral=True)
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

    @discord.ui.button(label="Clear Roles (Owner Only)", style=discord.ButtonStyle.danger, emoji="🧹", row=1)
    async def clear_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        ok, result = games_set_staff_role_ids([])
        if not ok:
            return await interaction.response.send_message(f"❌ Could not clear roles: `{result}`", ephemeral=True)
        new_view = GameStaffRoleView(self.owner_id)
        new_view.message = interaction.message
        await interaction.response.edit_message(
            embed=games_staff_embed(interaction.guild, "Game staff cleared; only owners now have access."),
            view=new_view,
        )


# ---------- /gamesadmin (owner) ----------
@bot.tree.command(name="gamesadmin", description="Owner controls for games & economy", guild=guild_obj)
@app_commands.describe(action="What to do", value="Value for the action")
@app_commands.choices(action=[
    app_commands.Choice(name="toggle public/testing", value="toggle"),
    app_commands.Choice(name="tester add", value="tester_add"),
    app_commands.Choice(name="tester remove", value="tester_remove"),
    app_commands.Choice(name="game staff roles", value="staff_roles"),
    app_commands.Choice(name="tester list", value="tester_list"),
    app_commands.Choice(name="spawn chance", value="spawn_chance"),
    app_commands.Choice(name="spawn channel add", value="spawn_add"),
    app_commands.Choice(name="spawn channel remove", value="spawn_remove"),
    app_commands.Choice(name="spawn channels list", value="spawn_list"),
    app_commands.Choice(name="sync pets+eggs", value="sync"),
    app_commands.Choice(name="interest rate", value="interest"),
    app_commands.Choice(name="jackpot seed", value="jackpot"),
    app_commands.Choice(name="game role", value="role"),
    app_commands.Choice(name="setup games channel", value="setup"),
    app_commands.Choice(name="reset testing economy", value="reset_testing"),
    app_commands.Choice(name="stats", value="stats"),
])
async def games_admin(interaction: discord.Interaction, action: str, value: str = None):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    if not games_owner_check(interaction.user):
        return await interaction.followup.send("❌ Owner only.", ephemeral=True)

    if action == "staff_roles":
        view = GameStaffRoleView(interaction.user.id)
        view.message = await interaction.followup.send(
            embed=games_staff_embed(interaction.guild), view=view, ephemeral=True, wait=True
        )
        return

    if action == "reset_testing":
        if games_enabled():
            return await interaction.followup.send("❌ Turn games `off` (testing-only) before resetting.", ephemeral=True)
        if str(value or "").strip() != "RESET":
            return await interaction.followup.send(
                "⚠️ This wipes game balances, collections, tickets, scores and test history. "
                "Run it again with value **`RESET`** to confirm. Cases and synced pet/egg data are kept.",
                ephemeral=True,
            )
        try:
            with conn:
                with conn.cursor() as cur:
                    for table in (
                        "mcwv_case_rolls", "mcwv_coin_log", "mcwv_pet_collections", "mcwv_duels",
                        "mcwv_game_stats", "mcwv_bingo_cards", "mcwv_user_stats", "mcwv_guess_profiles",
                        "mcwv_lottery_tickets", "mcwv_lottery_draws", "mcwv_tower_scores",
                        "mcwv_game_cooldowns", "mcwv_petdle_progress", "mcwv_coins",
                    ):
                        cur.execute(f"DELETE FROM {table}")
                    seed = int(db_get_setting(GAMES_SETTING_JACKPOT_SEED, "5000") or 5000)
                    cur.execute(
                        "UPDATE settings SET value = '0' WHERE key = %s",
                        (GAMES_SETTING_LOTTERY_POOL,),
                    )
                    cur.execute(
                        "UPDATE settings SET value = %s WHERE key = %s",
                        (str(seed), GAMES_SETTING_JACKPOT),
                    )
            ACTIVE_GUESS_ROUNDS.clear()
            for task in ACTIVE_GUESS_TASKS.values():
                task.cancel()
            ACTIVE_GUESS_TASKS.clear()
            for task in ACTIVE_GUESS_START_TASKS.values():
                task.cancel()
            ACTIVE_GUESS_START_TASKS.clear()
            ACTIVE_GUESS_STARTING.clear()
            ACTIVE_GUESS_CANCEL_REQUESTS.clear()
            ACTIVE_DUELS.clear()
            for task in ACTIVE_DUEL_TIMEOUT_TASKS.values():
                task.cancel()
            ACTIVE_DUEL_TIMEOUT_TASKS.clear()
            ACTIVE_TRIVIA.clear()
            ACTIVE_HANGMAN.clear()
            ACTIVE_SCRAMBLE.clear()
            ACTIVE_TOWER.clear()
            for task in ACTIVE_TOWER_TIMEOUT_TASKS.values():
                task.cancel()
            ACTIVE_TOWER_TIMEOUT_TASKS.clear()
            return await interaction.followup.send("✅ Testing economy reset. Configuration and synced pets/eggs were preserved.", ephemeral=True)
        except Exception as exc:
            print(f"[games] testing reset failed: {exc}")
            return await interaction.followup.send(f"❌ Reset failed: `{type(exc).__name__}`", ephemeral=True)

    if action == "stats":
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT game, sessions, coins_minted, coins_burned, last_played FROM mcwv_game_stats ORDER BY sessions DESC LIMIT 15")
                rows = cur.fetchall()
            embed = discord.Embed(title="🎮 Game Stats", color=games_color("gold"))
            game_emoji = {
                "guess": "🐾", "hatch": "🥚", "spin": "🎡", "scratch": "🎴", "trivia": "🧠",
                "scramble": "🔀", "hangman": "🎩", "duel": "⚔️", "tower": "🏗", "case": "🎁",
                "lottery": "🎟", "daily": "📅",
            }
            lines = [f"{game_emoji.get(str(g), '🎮')} **{g}** — `{s}` sessions · +{m:,} minted · −{b:,} burned" for g, s, m, b, _ in rows] or ["Nothing played yet!"]
            embed.description = "\n".join(lines)
            embed.add_field(name="🎰 Spin jackpot", value=f"**{games_jackpot_get():,}** 🪙", inline=True)
            embed.add_field(name="🥚 Eggs synced", value=str(len(games_get_eggs())), inline=True)
            embed.add_field(name="🐾 Pets synced", value=str(len(games_get_pets())), inline=True)
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ Stats failed: `{type(exc).__name__}`", ephemeral=True)
        return

    if action == "toggle":
        new_state = str(value or "").strip().lower()
        if new_state not in ("on", "off"):
            return await interaction.followup.send("Usage: value = `on` or `off`.", ephemeral=True)
        db_set_setting(GAMES_SETTING_ENABLED, "1" if new_state == "on" else "0")
        return await interaction.followup.send(f"✅ Games are now **{'PUBLIC' if new_state == 'on' else 'TESTING-ONLY'}**.", ephemeral=True)

    if action == "tester_add":
        uid = int(re.sub(r"\D", "", value or "")) if value and re.search(r"\d{15,}", value) else None
        if not uid:
            return await interaction.followup.send("Mention a user: `/gamesadmin tester add @user`.", ephemeral=True)
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO mcwv_game_testers (discord_id, added_by) VALUES (%s,%s) ON CONFLICT (discord_id) DO NOTHING", (uid, interaction.user.id))
            conn.commit()
            await interaction.followup.send(f"✅ <@{uid}> added as a game tester.", ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ Failed: `{type(exc).__name__}`", ephemeral=True)
        return

    if action == "tester_remove":
        uid = int(re.sub(r"\D", "", value or "")) if value and re.search(r"\d{15,}", value) else None
        if not uid:
            return await interaction.followup.send("Mention a user to remove.", ephemeral=True)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM mcwv_game_testers WHERE discord_id = %s", (uid,))
            conn.commit()
            await interaction.followup.send(f"✅ <@{uid}> removed from testers.", ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ Failed: `{type(exc).__name__}`", ephemeral=True)
        return

    if action == "tester_list":
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT discord_id FROM mcwv_game_testers ORDER BY added_at")
                rows = cur.fetchall()
            await interaction.followup.send("Testers: " + (" ".join(f"<@{r[0]}>" for r in rows) if rows else "none (owner only)"), ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ Failed: `{type(exc).__name__}`", ephemeral=True)
        return

    if action == "spawn_chance":
        try:
            pct = float(value or "")
        except Exception:
            return await interaction.followup.send("Usage: value = percent (e.g. 1 = 1% per message).", ephemeral=True)
        safe_pct = max(0.0, min(100.0, pct))
        db_set_setting(GAMES_SETTING_SPAWN_CHANCE, str(safe_pct))
        return await interaction.followup.send(f"✅ Spawn chance set to **{safe_pct:g}%** per message.", ephemeral=True)

    if action in ("spawn_add", "spawn_remove"):
        m = re.search(r"\d{15,}", value or "")
        if not m:
            return await interaction.followup.send(
                "Mention the channel (`#games`) or paste its ID.", ephemeral=True)
        cid = int(m.group(0))
        chan = interaction.guild.get_channel(cid) if interaction.guild else None
        if chan is None:
            return await interaction.followup.send(
                "❌ I can't find that channel — mention it directly (e.g. `#games`).", ephemeral=True)
        if getattr(chan, "type", None) not in (discord.ChannelType.text, discord.ChannelType.news):
            return await interaction.followup.send("❌ Spawns only work in text channels.", ephemeral=True)
        raw = db_get_setting(GAMES_SETTING_SPAWN_CHANNELS, "[]")
        try:
            chans = json.loads(raw or "[]")
        except Exception:
            chans = []
        if action == "spawn_add" and cid not in chans:
            chans.append(cid)
        elif action == "spawn_remove" and cid in chans:
            chans.remove(cid)
        db_set_setting(GAMES_SETTING_SPAWN_CHANNELS, json.dumps(chans))
        names = " ".join(f"<#{c}>" for c in chans) or "none"
        return await interaction.followup.send(
            f"✅ {'Added' if action == 'spawn_add' else 'Removed'} <#{cid}>\n"
            f"Active spawn channels: {names}", ephemeral=True)

    if action == "spawn_list":
        raw = db_get_setting(GAMES_SETTING_SPAWN_CHANNELS, "[]")
        try:
            chans = json.loads(raw or "[]")
        except Exception:
            chans = []
        return await interaction.followup.send("Spawn channels: " + (" ".join(f"<#{c}>" for c in chans) if chans else "none set"), ephemeral=True)

    if action == "sync":
        await interaction.followup.send("🔄 Syncing the real pet/egg database…", ephemeral=True)
        try:
            ok_pets = await asyncio.to_thread(games_sync_pets_from_web)
            ok_eggs = await asyncio.to_thread(games_sync_eggs_v2)
            pets = len(games_get_pets())
            eggs = len(games_get_eggs())
            await interaction.followup.send(
                f"✅ Sync done — **{pets}** pets, **{eggs}** eggs (pets {'ok' if ok_pets else 'FAILED'}, eggs {'ok' if ok_eggs else 'FAILED'}).",
                ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ Sync failed: `{type(exc).__name__}`", ephemeral=True)
        return

    if action == "interest":
        try:
            parts = str(value or "").split()
            rate = float(parts[0])
            cap = int(parts[1]) if len(parts) > 1 else None
        except Exception:
            return await interaction.followup.send("Usage: value = `rate% [cap]` e.g. `1 100000`.", ephemeral=True)
        safe_rate = max(0.0, min(100.0, rate))
        safe_cap = max(0, cap) if cap is not None else None
        db_set_setting(GAMES_SETTING_INTEREST_RATE, str(safe_rate))
        if safe_cap is not None:
            db_set_setting(GAMES_SETTING_INTEREST_CAP, str(safe_cap))
        return await interaction.followup.send(
            f"✅ Interest: **{safe_rate:g}%/day** on up to **{safe_cap if safe_cap is not None else 'the current cap'}** banked.",
            ephemeral=True,
        )

    if action == "jackpot":
        try:
            seed = int(value or 0)
        except Exception:
            return await interaction.followup.send("Usage: value = seed amount (also resets the current jackpot).", ephemeral=True)
        if seed < 100:
            return await interaction.followup.send("❌ Seed must be at least 100.", ephemeral=True)
        games_jackpot_set(seed)
        db_set_setting(GAMES_SETTING_JACKPOT_SEED, str(seed))
        return await interaction.followup.send(f"✅ Jackpot reset to **{seed:,}**.", ephemeral=True)

    if action == "role":
        # value = "<kind> @role"
        try:
            m = re.match(r"(\w+)\s*<@&?(\d+)>", value or "")
            kind = m.group(1).lower()
            rid = int(m.group(2))
        except Exception:
            return await interaction.followup.send("Usage: value = `duelist|petmaster|towermaster @role`.", ephemeral=True)
        if kind not in ("duelist", "petmaster", "towermaster"):
            return await interaction.followup.send("❌ Kind must be duelist, petmaster or towermaster.", ephemeral=True)
        db_set_setting(f"games_role_{kind}", str(rid))
        return await interaction.followup.send(f"✅ **{kind}** role set to <@&{rid}>.", ephemeral=True)

    if action == "setup":
        try:
            guild = interaction.guild
            ch = await guild.create_text_channel("games", reason="MCWV games channel setup")
            raw = db_get_setting(GAMES_SETTING_SPAWN_CHANNELS, "[]")
            try:
                chans = json.loads(raw or "[]")
            except Exception:
                chans = []
            if ch.id not in chans:
                chans.append(ch.id)
            db_set_setting(GAMES_SETTING_SPAWN_CHANNELS, json.dumps(chans))
            guide = discord.Embed(
                title="🎮 MCWV Games — Live Here!",
                description=(
                    "Games spawn in this channel! Play with:\n"
                    "`/hatch` · `/spin` · `/scratch` · `/trivia` · `/scramble` · `/hangman` · `/petdle` · `/tower`\n"
                    "Economy: `/daily` · `/coins` · `/shop` · `/cases` · `/top` · `/duel`\n"
                    "Learn everything: `/gamesguide`"
                ),
                color=discord.Color.from_rgb(108, 34, 245),
            )
            msg = await ch.send(embed=guide)
            try:
                await msg.pin()
            except Exception:
                pass
            await interaction.followup.send(f"✅ Created {ch.mention}, added it as a spawn channel, and pinned the guide.", ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ Setup failed: `{type(exc).__name__}`", ephemeral=True)
        return

    return await interaction.followup.send("Unknown action.", ephemeral=True)


# ---------- /lottery (tickets table) ----------
@bot.tree.command(name="lottery", description="Weekly lottery — tickets from /shop, draws Sunday", guild=guild_obj)
@app_commands.describe(action="buy / view / draw (owner)")
@app_commands.choices(action=[
    app_commands.Choice(name="buy tickets", value="buy"),
    app_commands.Choice(name="view pool", value="view"),
    app_commands.Choice(name="draw now (owner)", value="draw"),
])
async def games_lottery(interaction: discord.Interaction, action: str, amount: int = 1):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    if action == "buy":
        n = max(1, int(amount))
        owned = games_lottery_owned(interaction.user.id)
        if owned + n > GAMES_LOTTERY_WEEKLY_TICKET_CAP:
            return await interaction.followup.send(
                f"❌ Weekly cap is **{GAMES_LOTTERY_WEEKLY_TICKET_CAP}** tickets — you already hold {owned}.", ephemeral=True)
        ok, result = games_lottery_purchase(interaction.user.id, n)
        if not ok:
            return await interaction.followup.send(f"❌ {result}", ephemeral=True)
        embed = discord.Embed(
            title="🎟 Tickets Bought",
            description=(f"You now hold **{result['owned']}** ticket{'s' if result['owned'] != 1 else ''} "
                         f"for the **{result['round']}** draw."),
            color=games_color("green"),
        )
        embed.add_field(name="Pool", value=f"**{result['pool']:,}** 🪙", inline=True)
        embed.add_field(name="Spent", value=f"`{result['cost']:,}` 🪙", inline=True)
        games_footer(embed, "Draw: Sunday 20:00 UTC · winner takes 70%")
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    if action == "view":
        pool = games_lottery_pool()
        owned = games_lottery_owned(interaction.user.id)
        try:
            round_key = games_lottery_round_key()
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(SUM(tickets), 0) FROM mcwv_lottery_tickets WHERE week = %s", (round_key,))
                total_entries = int(cur.fetchone()[0] or 0)
        except Exception:
            total_entries = 0
        odds = (owned / total_entries * 100) if total_entries else 0.0
        nxt = games_lottery_next_draw()
        embed = discord.Embed(
            title="🎟 Weekly Lottery",
            description=f"**Pool:** 🪙 **{pool:,}** · winner takes **70%** (`{int(pool * 0.7):,}`)",
            color=games_color("gold"),
        )
        embed.add_field(name="Your tickets", value=f"**{owned}** / {GAMES_LOTTERY_WEEKLY_TICKET_CAP} cap `{games_bar(owned, GAMES_LOTTERY_WEEKLY_TICKET_CAP, 10)}`", inline=True)
        embed.add_field(name="Total entries", value=f"**{total_entries}**", inline=True)
        embed.add_field(name="Your win chance", value=f"**{odds:.2f}%**", inline=True)
        embed.add_field(name="Next draw", value=discord.utils.format_dt(nxt, "R"), inline=False)
        games_footer(embed, "Buy: /lottery buy <amount> · tickets 50 🪙 each")
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    if action == "draw":
        if not games_owner_check(interaction.user):
            return await interaction.followup.send("❌ Owner only.", ephemeral=True)
        await games_lottery_draw_async(interaction.channel)
        return
    return await interaction.followup.send("Unknown action.", ephemeral=True)


# ---------- /tower (chat-based) ----------
@bot.tree.command(name="tower", description="Tower of Pets — endless mixed floors, 3 hearts (chat game)", guild=guild_obj)
async def games_tower(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    session = ACTIVE_TOWER.get(interaction.user.id)
    if session and session.get("active"):
        return await interaction.followup.send(
            f"🏗 You're already climbing — floor **{session['floor']}**, {session['hearts']} ❤️ left.", ephemeral=True)
    free_run, runs_today = games_free_use(interaction.user.id, "tower")
    if not free_run and not games_is_unlimited(interaction.user.id):
        return await interaction.followup.send(
            f"❌ You've used all **{GAMES_TOWER_RUNS_PER_DAY}** tower runs in this 24h window — come back tomorrow!", ephemeral=True)
    games_coin_log_zero(interaction.user.id, "tower_run", meta={"runs_today": runs_today})
    ACTIVE_TOWER[interaction.user.id] = {
        "user_id": interaction.user.id, "floor": 1, "hearts": 3, "score": 0,
        "combo": 0, "active": True, "started": time.time(),
        "floor_started": time.time(), "channel_id": interaction.channel.id,
    }
    embed = discord.Embed(
        title="🏗 Tower of Pets",
        description=(
            f"{interaction.user.mention} starts the climb!\n\n"
            "🧠 **Trivia floors** — answer with `/toweranswer A/B/C/D`\n"
            f"🐾 **Guess floors** — just type the pet name in chat!\n\n"
            f"Reach floor **{GAMES_TOWER_MAX_FLOOR}** to conquer the tower."
        ),
        color=games_color("purple"),
    )
    embed.add_field(name="Hearts", value=games_hearts(3, 3), inline=True)
    embed.add_field(name="Runs today", value=f"{runs_today}/{GAMES_TOWER_RUNS_PER_DAY}", inline=True)
    embed.add_field(name="Reward per floor", value="`25 × floor` 🪙", inline=True)
    games_footer(embed, f"{GAMES_TOWER_MAX_FLOOR} floors · coins scale with depth + combo")
    await interaction.followup.send(embed=embed, ephemeral=False)
    await games_tower_ask_chat(interaction.channel, interaction.user.id)


@bot.tree.command(name="toweranswer", description="Answer a Tower trivia floor", guild=guild_obj)
@app_commands.describe(answer="A/B/C/D or the answer text")
async def games_tower_answer(interaction: discord.Interaction, answer: str):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    session = ACTIVE_TOWER.get(interaction.user.id)
    if not session or not session.get("active"):
        return await interaction.followup.send("No active tower run — start one with `/tower`.", ephemeral=True)
    if interaction.channel.id != session.get("channel_id"):
        return await interaction.followup.send(f"❌ Finish this run in <#{session.get('channel_id')}>.", ephemeral=True)
    if time.time() - float(session.get("floor_started", 0)) > GAMES_TOWER_FLOOR_TIMEOUT:
        return await interaction.followup.send("⏰ That floor already timed out — wait for the next one.", ephemeral=True)
    kind = session.get("kind")
    correct = False
    if kind == "trivia":
        options = session.get("trivia_options") or []
        target = str(session.get("trivia_answer") or "")
        ans = (answer or "").strip()
        if len(ans) == 1 and ans.isalpha() and ans.upper() in "ABCD" and options:
            correct = options["ABCD".index(ans.upper())] == target
        else:
            correct = games_answers_match(ans, target)
    elif kind == "guess":
        correct = games_answers_match(answer, session.get("answer", ""))
    else:
        return await interaction.followup.send("Current floor is a guess floor — just type the name in chat!", ephemeral=True)
    session["kind"] = None
    await games_tower_result_from_chat(interaction.channel, interaction.user, correct)
    await interaction.followup.send("📤 Answered!", ephemeral=True)


# ---------- MCWV HISTORY TRIVIA ----------
def games_history_trivia_questions(limit=8):
    """Generate questions through the optional SELECT-only MCWV DB connection."""
    questions = []
    worker = _readonly_connection()
    try:
        if worker is None:
            raise RuntimeError("MCWV_READONLY_DATABASE_URL is not configured")
        worker.set_session(readonly=True, autocommit=True)
        with worker.cursor() as cur:
            # battles with places
            cur.execute("SELECT battle_id, clan_place FROM cross_clan_player_history WHERE clan_name = %s AND clan_place IS NOT NULL GROUP BY battle_id, clan_place ORDER BY RANDOM() LIMIT 6", (CLAN_NAME,))
            for battle_id, place in cur.fetchall():
                title = _friendly_battle_name(str(battle_id))
                others = sorted({p for p in (place + 1, place + 2, max(1, place - 1), place + 5) if p != place and p > 0})
                options = [str(place)] + [str(p) for p in others[:3]]
                secrets.SystemRandom().shuffle(options)
                questions.append((f"Where did MCWV finish in **{title}**?", options, options.index(str(place))))
            # top scorers
            cur.execute("""
                SELECT battle_id, roblox_id, points FROM cross_clan_player_history
                WHERE clan_name = %s AND points > 0
                ORDER BY RANDOM() LIMIT 4
            """, (CLAN_NAME,))
            for battle_id, rid, pts in cur.fetchall():
                title = _friendly_battle_name(str(battle_id))
                # find the actual top scorer for that battle
                cur.execute("""
                    SELECT roblox_id FROM cross_clan_player_history
                    WHERE clan_name = %s AND battle_id = %s AND points > 0
                    ORDER BY points DESC LIMIT 1
                """, (CLAN_NAME, battle_id))
                top = cur.fetchone()
                if not top:
                    continue
                top_name = "player " + str(top[0])[-4:]
                other_ids = [r for r in (rid, 4544162965, 3071457976, 1882212690) if r != top[0]][:3]
                options = [top_name] + ["player " + str(x)[-4:] for x in other_ids]
                secrets.SystemRandom().shuffle(options)
                questions.append((f"Who was MCWV's top scorer in **{title}**?", options, options.index(top_name)))
            # member clan history: which clan before MCWV
            cur.execute("""
                SELECT clan_name FROM cross_clan_player_history
                WHERE clan_name != %s AND roblox_id IN (SELECT TRIM(roblox_id) FROM users WHERE roblox_id IS NOT NULL)
                GROUP BY clan_name
                ORDER BY RANDOM() LIMIT 4
            """, (CLAN_NAME,))
            for (prev_clan,) in cur.fetchall():
                cur.execute("""
                    SELECT username FROM users WHERE roblox_id IN (
                        SELECT roblox_id FROM cross_clan_player_history WHERE clan_name = %s LIMIT 1
                    ) LIMIT 1
                """, (prev_clan,))
                urow = cur.fetchone()
                who = str(urow[0]) if urow and urow[0] else "a former member"
                decoys = ["V1LN", "SOPU", "EROS", "T1ED", "B332"]
                decoys = [d for d in decoys if d != prev_clan][:3]
                options = [prev_clan] + decoys
                secrets.SystemRandom().shuffle(options)
                questions.append((f"Which rival clan did we recruit **{who}** from?", options, options.index(prev_clan)))
    except Exception as exc:
        print(f"[games] history trivia live data unavailable: {exc}")
    finally:
        if worker is not None:
            worker.close()
    if not questions:
        # Safe static fallback when the read-only integration is not configured.
        questions = [
            ("Which battle did MCWV finish #24 in?", ["Ninja Battle 2026", "Gummy Battle 2026", "Lunar Battle 2026", "Soccer Battle 2026"], 0),
            ("Which war came first?", ["Gummy Battle 2026", "Ninja Battle 2026", "Soccer Battle 2026", "Lunar Battle 2026"], 0),
        ]
    secrets.SystemRandom().shuffle(questions)
    return questions[:limit]


@bot.tree.command(name="historytrivia", description="MCWV history trivia — questions from OUR war database", guild=guild_obj)
async def games_history_trivia(interaction: discord.Interaction):
    if not games_gate_allowed(interaction):
        return await interaction.response.send_message("🎮 Games are still in testing — coming soon.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    if interaction.user.id in ACTIVE_TRIVIA:
        return await interaction.followup.send("❌ You already have a trivia session running — finish it first!", ephemeral=True)
    allowed, retry_at = (True, None) if games_is_unlimited(interaction.user.id) else games_cooldown_claim(
        interaction.user.id, "historytrivia", GAMES_HISTORY_TRIVIA_COOLDOWN
    )
    if not allowed:
        retry = discord.utils.format_dt(retry_at, "R") if retry_at else "soon"
        return await interaction.followup.send(f"⏳ Your next history run is available {retry}.", ephemeral=True)
    qs = games_history_trivia_questions(5)
    if not qs:
        return await interaction.followup.send("❌ Couldn't generate history questions right now.", ephemeral=True)
    session = {
        "user_id": interaction.user.id,
        "game": "historytrivia",
        "q_index": 0,
        "score": 0,
        "wrong": 0,
        "questions": qs,
        "ctx": interaction,
        "started": time.time(),
        "last_activity": time.time(),
    }
    ACTIVE_TRIVIA[interaction.user.id] = session
    await games_trivia_next(interaction, session)




# Extended trivia bank (v4)
GAMES_TRIVIA_SEED.extend([
    ("Which rarity is rarer than Huge?", ["Titanic", "Exclusive", "Epic", "Legendary"], 0),
    ("What does a 'pt' value of 2 mean on a pet?", ["Rainbow variant", "Golden variant", "Shiny variant", "Normal"], 0),
    ("Which egg type can contain Titanics?", ["Exclusive eggs", "Basic eggs", "Starter eggs", "No eggs"], 0),
    ("What is the Rainbow Chance on most exclusive eggs?", ["200%", "100%", "50%", "0%"], 0),
    ("Which pet rarity announces server-wide when hatched?", ["Titanic", "Rare", "Epic", "Common"], 0),
    ("What does RAP stand for?", ["Recent Average Price", "Rare Active Pets", "Random Auction Price", "Rapid Auction Points"], 0),
    ("How many pets are in the PS99 database?", ["~15,000", "~1,500", "~150", "~150,000"], 0),
    ("Which of these is a real Titanic?", ["Titanic Nyan Cat", "Titanic Mega Dog", "Titanic Ultra Cat", "Titanic Bob"], 0),
    ("What is the fun hatch rate for a Huge in the Clan Egg?", ["2%", "0.5%", "10%", "20%"], 0),
    ("Which stat do /duel exist-count rounds compare?", ["A pet's exist count", "Points", "Diamonds", "Account age"], 0),
    ("What does the Featured Egg get daily?", ["Doubled top-tier odds", "Free hatches", "Half price", "Nothing"], 0),
    ("How many free hatches per day?", ["3", "1", "5", "10"], 0),
    ("What resets your /daily streak?", ["48h without claiming", "24h", "A week", "Never"], 0),
    ("Which of these is a PS99 enchant?", ["Strong Pets", "Fire Aspect", "Sharpness", "Looting"], 0),
    ("What does the bank interest cap at?", ["100k banked", "10k banked", "1M banked", "No cap"], 0),
    ("Who can create role cases?", ["Staff", "Anyone", "Owner only", "Members"], 0),
    ("How often does the lottery draw?", ["Weekly (Sunday)", "Daily", "Monthly", "Hourly"], 0),
    ("What's the duel minimum wager?", ["10 coins", "1 coin", "100 coins", "1000 coins"], 0),
    ("Which game has a progressive jackpot?", ["Spin", "Hatch", "Scramble", "Petdle"], 0),
    ("How many tower runs per day?", ["3", "1", "5", "Unlimited"], 0),
])

# Remove stale test-egg/API-count questions and exact duplicates before serving trivia.
_TRIVIA_STALE_FRAGMENTS = (
    "clan egg", "how many pets are in the ps99 database",
    "rainbow chance on most exclusive eggs", "announces server-wide",
)
_trivia_seen = set()
_trivia_clean = []
for _q in GAMES_TRIVIA_SEED:
    _key = normalize_answer(_q[0])
    if any(fragment in _q[0].lower() for fragment in _TRIVIA_STALE_FRAGMENTS) or _key in _trivia_seen:
        continue
    _trivia_seen.add(_key)
    _trivia_clean.append(_q)
GAMES_TRIVIA_SEED[:] = _trivia_clean



@bot.event
async def on_ready():
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
    if not getattr(bot, "_games_commands_synced", False):
        synced = await bot.tree.sync(guild=guild_obj)
        bot._games_commands_synced = True
        print(f"[discord] synced {len(synced)} MCWV Games commands")
    if not games_housekeeping_loop.is_running():
        games_housekeeping_loop.start()
    await bot.change_presence(activity=discord.Game(name="MCWV Games · /games"))
    print(f"[discord] ready as {bot.user} ({bot.user.id})")


@bot.event
async def on_disconnect():
    print("[discord] disconnected; discord.py will attempt to reconnect")


def initialize_database():
    if not DATABASE_URL:
        raise RuntimeError("Missing required DATABASE_URL environment variable")
    if ensure_db_connection() is None:
        raise RuntimeError("Could not connect to the games database")
    init_base_schema()
    init_games_tables()


def main():
    if not TOKEN:
        raise RuntimeError("Missing required DISCORD_TOKEN environment variable")
    threading.Thread(target=run_health_server, name="health-server", daemon=True).start()
    initialize_database()
    bot.run(TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
