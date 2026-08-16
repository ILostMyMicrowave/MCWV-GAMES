import asyncio
import os
from io import BytesIO

os.environ.setdefault("GUILD_ID", "1501608673250640055")

from PIL import Image
import games_bot as game


def test_registration_and_health():
    commands = game.bot.tree.get_commands(guild=game.guild_obj)
    names = {command.name for command in commands}
    assert len(commands) == 30
    assert {"games", "guess", "shop", "caseadmin", "gamesadmin"} <= names
    assert not ({"ticket", "giveaway", "warinfo", "add", "invite"} & names)
    response = game.app.test_client().get("/health")
    assert response.status_code == 200
    assert response.get_json()["service"] == "MCWV Games"


def test_guess_pure_logic_and_images():
    pool = ["Huge Cosmic Agony", "Huge Happy Rock", "Titanic Banana Cat"]
    aliases = game.games_guess_aliases(pool[0], pool)
    assert game.games_guess_answer_result("Cosmic Agony", pool[0], aliases)[0]
    assert game.games_guess_answer_result("Cosmic Agonx", pool[0], aliases)[0]

    short_counts = {}
    for name in pool:
        short = game.normalize_answer(game.games_guess_short_name(name))
        short_counts[short] = short_counts.get(short, 0) + 1
    valid = {game.normalize_answer(name) for name in pool}
    valid.update(short for short, count in short_counts.items() if count == 1 and len(short) >= 4)
    index = game.games_guess_typo_index(valid)
    assert game.games_guess_is_catalogue_attempt(game.normalize_answer("Huge Happy Rocx"), valid, index)
    assert not game.games_guess_is_catalogue_attempt(game.normalize_answer("hello everyone"), valid, index)

    source = Image.new("RGBA", (96, 96), (20, 100, 220, 255))
    raw = BytesIO()
    source.save(raw, "PNG")
    for mode in ("zoom", "silhouette", "pixel", "scrambled", "blur", "monochrome", "negative", "reveal"):
        output = game.games_build_round_image(raw.getvalue(), mode)
        assert output and output.getbuffer().nbytes > 50

    for mode in game.GAMES_GUESS_MODE_INFO:
        round_info = {"rewarded": True, "mode": mode, "hint_step": 0, "pet_name": "Titanic Banana Cat"}
        reward = game.games_guess_reward(round_info, 0, 999)
        assert game.GAMES_GUESS_MIN_REWARD <= reward <= game.GAMES_GUESS_MAX_REWARD
    assert game.games_guess_reward({"rewarded": False}, 0, 999) == 0


class Channel:
    def __init__(self, channel_id):
        self.id = channel_id
        self.sent = []

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))
        return type("Sent", (), {"id": len(self.sent)})()


class Author:
    def __init__(self, user_id):
        self.id = user_id
        self.bot = False
        self.mention = f"<@{user_id}>"


class Message:
    def __init__(self, channel, author, content):
        self.channel = channel
        self.author = author
        self.content = content
        self.reactions = []

    async def add_reaction(self, value):
        self.reactions.append(value)


async def _practice_and_clock_checks():
    pool = ["Huge Cosmic Agony", "Huge Happy Rock", "Titanic Banana Cat"]
    valid = {game.normalize_answer(name) for name in pool}
    valid.update(game.normalize_answer(game.games_guess_short_name(name)) for name in pool)
    aliases = game.games_guess_aliases(pool[0], pool)
    index = game.games_guess_typo_index(valid)
    channel = Channel(901)
    user = Author(50)
    round_info = {
        "pet_name": pool[0], "pet_key": pool[0], "mode": "zoom", "started": game.time.time(),
        "attempts": {}, "participants": set(), "hint_step": 0, "rewarded": False,
        "aliases": aliases, "valid_names": valid, "typo_index": index,
        "channel_id": channel.id, "icon": None,
    }
    game.ACTIVE_GUESS_ROUNDS[channel.id] = round_info
    paid, recorded, tracked = [], [], []
    originals = (
        game.games_coin_adjust, game.games_guess_profile, game.games_guess_record_result,
        game.games_track, game.games_track_participants,
    )
    game.games_coin_adjust = lambda *a, **k: (paid.append(1) or (True, None))
    game.games_guess_profile = lambda uid: {
        "wins": 2, "rounds": 3, "current_streak": 2, "best_streak": 2,
        "fastest_ms": 1000, "total_reward": 500, "valid_guesses": 3,
    }
    game.games_guess_record_result = lambda *a, **k: recorded.append(1)
    game.games_track = lambda *a, **k: None
    game.games_track_participants = lambda *a, **k: tracked.append(1)
    assert await game.games_handle_answer(Message(channel, user, "Cosmic Agony")) is True
    assert not paid and not recorded and not tracked
    (
        game.games_coin_adjust, game.games_guess_profile, game.games_guess_record_result,
        game.games_track, game.games_track_participants,
    ) = originals

    events = []
    clock = {"now": 1000.0}
    started = clock["now"]
    game.ACTIVE_GUESS_ROUNDS[902] = {"started": started}
    real_sleep, real_time = game.asyncio.sleep, game.time.time
    real_hint, real_timeout = game.games_post_hint, game.games_guess_timeout

    async def fake_sleep(seconds):
        clock["now"] += seconds

    async def fake_hint(_channel, _round):
        events.append(("hint", round(clock["now"] - started)))

    async def fake_timeout(channel_id, _started):
        events.append(("timeout", round(clock["now"] - started)))
        game.ACTIVE_GUESS_ROUNDS.pop(channel_id, None)

    game.asyncio.sleep = fake_sleep
    game.time.time = lambda: clock["now"]
    game.games_post_hint = fake_hint
    game.games_guess_timeout = fake_timeout
    game.bot.get_channel = lambda _channel_id: channel
    await game.games_guess_clock(902, started)
    assert events == [("hint", 30), ("hint", 60), ("timeout", 90)]
    game.asyncio.sleep, game.time.time = real_sleep, real_time
    game.games_post_hint, game.games_guess_timeout = real_hint, real_timeout


def test_async_lifecycle():
    asyncio.run(_practice_and_clock_checks())


if __name__ == "__main__":
    test_registration_and_health()
    test_guess_pure_logic_and_images()
    test_async_lifecycle()
    print("standalone checks passed")
