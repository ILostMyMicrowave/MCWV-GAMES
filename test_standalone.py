import ast
import asyncio
import contextlib
import io
import os
from io import BytesIO
from pathlib import Path

os.environ.setdefault("GUILD_ID", "1501608673250640055")

from PIL import Image, ImageDraw
import games_bot as game


def test_registration_and_health():
    commands = game.bot.tree.get_commands(guild=game.guild_obj)
    names = {command.name for command in commands}
    assert len(commands) == 30
    assert {"games", "guess", "shop", "caseadmin", "gamesadmin"} <= names
    assert not ({"ticket", "giveaway", "warinfo", "add", "invite"} & names)
    original_probe = game.games_database_probe
    original_ready = game.bot.is_ready
    game.games_database_probe = lambda: True
    game.bot.is_ready = lambda: True
    try:
        response = game.app.test_client().get("/health")
    finally:
        game.games_database_probe = original_probe
        game.bot.is_ready = original_ready
    assert response.status_code == 200
    assert response.get_json()["service"] == "MCWV Games"

    game.games_database_probe = lambda: False
    game.bot.is_ready = lambda: True
    try:
        degraded = game.app.test_client().get("/health")
    finally:
        game.games_database_probe = original_probe
        game.bot.is_ready = original_ready
    assert degraded.status_code == 503
    assert degraded.get_json()["status"] == "degraded"


def test_connection_errors_do_not_log_secrets():
    secret = "do-not-print-this-password"
    original = game._connect_database

    def fail_with_secret():
        raise game.psycopg2.ProgrammingError(f"invalid DSN token: {secret}")

    game._connect_database = fail_with_secret
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            assert game.ensure_db_connection() is None
    finally:
        game._connect_database = original
    logged = output.getvalue()
    assert "ProgrammingError" in logged
    assert secret not in logged


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

    # Full-frame artwork mirrors real API icons. Silhouette mode must remain
    # visibly detailed rather than collapsing the opaque square to pure black.
    source = Image.new("RGBA", (96, 96), (20, 100, 220, 255))
    draw = ImageDraw.Draw(source)
    draw.ellipse((14, 10, 82, 84), fill=(245, 180, 60, 255))
    draw.ellipse((27, 30, 39, 44), fill=(10, 12, 25, 255))
    draw.ellipse((57, 30, 69, 44), fill=(10, 12, 25, 255))
    draw.arc((34, 42, 62, 68), 10, 170, fill=(30, 20, 45, 255), width=4)
    raw = BytesIO()
    source.save(raw, "PNG")
    for mode in ("zoom", "silhouette", "pixel", "scrambled", "blur", "monochrome", "negative", "reveal"):
        output = game.games_build_round_image(raw.getvalue(), mode)
        assert output and output.getbuffer().nbytes > 50
        if mode == "silhouette":
            rendered = Image.open(output).convert("RGB")
            colours = rendered.getcolors(maxcolors=rendered.width * rendered.height)
            low, high = rendered.convert("L").getextrema()
            assert colours is not None and len(colours) > 16
            assert high - low >= 30
            assert sum(count for count, rgb in colours if max(rgb) < 8) < rendered.width * rendered.height // 4

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


class DeniedChannel(Channel):
    async def send(self, *args, **kwargs):
        response = type("Response", (), {"status": 403, "reason": "Forbidden"})()
        raise game.discord.Forbidden(response, {"code": 50013, "message": "Missing Permissions"})


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
    # Discord permission failures should produce an actionable staff response,
    # not the misleading "another round is loading" message.
    real_pool, real_asset = game.games_guess_pet_pool, game.games_pet_asset
    game.games_guess_pet_pool = lambda: ["Huge Test Pet"]
    game.games_pet_asset = lambda _name: None
    permission_error = await game.games_start_guess_round(
        DeniedChannel(900), pet_key="Huge Test Pet", mode="letters", rewarded=False,
    )
    game.games_guess_pet_pool, game.games_pet_asset = real_pool, real_asset
    assert isinstance(permission_error, str)
    assert "Send Messages" in permission_error and "Attach Files" in permission_error
    assert 900 not in game.ACTIVE_GUESS_STARTING

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


def test_every_slash_command_defers_first():
    """Regression guard for Discord's initial interaction deadline."""
    tree = ast.parse(Path(game.__file__).read_text(encoding="utf-8"))
    checked = []
    for function in (node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)):
        is_command = any(
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr == "command"
            for decorator in function.decorator_list
        )
        if not is_command:
            continue
        first = function.body[0]
        assert isinstance(first, ast.Expr) and isinstance(first.value, ast.Await), function.name
        call = first.value.value
        assert isinstance(call, ast.Call), function.name
        assert isinstance(call.func, ast.Attribute) and call.func.attr == "defer", function.name
        checked.append(function.name)
    assert len(checked) == 30

    autocomplete_names = {
        "games_case_autocomplete",
        "games_guess_pet_autocomplete",
        "games_egg_autocomplete",
    }
    found = set()
    for function in (node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)):
        if function.name not in autocomplete_names:
            continue
        found.add(function.name)
        calls = {
            node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
            for node in ast.walk(function) if isinstance(node, ast.Call)
        }
        assert not any(name.startswith("db_") or name in {"cursor", "execute"} for name in calls)
    assert found == autocomplete_names

    def dotted(node):
        if isinstance(node, ast.Call):
            return dotted(node.func)
        if isinstance(node, ast.Attribute):
            base = dotted(node.value)
            return f"{base}.{node.attr}" if base else node.attr
        if isinstance(node, ast.Name):
            return node.id
        return ""

    acknowledgement_names = {
        "interaction.response.defer",
        "interaction.response.send_message",
        "interaction.response.edit_message",
        "interaction.response.send_modal",
    }
    dangerous_fragments = ("db_", "conn.cursor", ".fetch_", ".create_invite")
    callbacks = []
    for class_node in (node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)):
        methods = {
            node.name: node for node in class_node.body if isinstance(node, ast.AsyncFunctionDef)
        }
        for function in methods.values():
            is_button = any(
                isinstance(decorator, ast.Call) and dotted(decorator.func).endswith("discord.ui.button")
                for decorator in function.decorator_list
            )
            if function.name not in {"callback", "on_submit"} and not is_button:
                continue
            callbacks.append(f"{class_node.name}.{function.name}")
            ack_lines = [
                node.lineno for node in ast.walk(function)
                if isinstance(node, ast.Await)
                and isinstance(node.value, ast.Call)
                and dotted(node.value.func) in acknowledgement_names
            ]
            ack_line = min(ack_lines) if ack_lines else None
            if ack_line is None:
                delegated = sorted(
                    (
                        node.lineno,
                        dotted(node.value.func).removeprefix("self."),
                    )
                    for node in ast.walk(function)
                    if isinstance(node, ast.Await)
                    and isinstance(node.value, ast.Call)
                    and dotted(node.value.func).startswith("self.")
                )
                assert delegated, f"{class_node.name}.{function.name}"
                def helper_has_ack(helper_name, seen=None):
                    seen = set(seen or ())
                    if helper_name in seen:
                        return False
                    seen.add(helper_name)
                    helper = methods.get(helper_name)
                    if helper is None:
                        return False
                    if any(
                        isinstance(node, ast.Await)
                        and isinstance(node.value, ast.Call)
                        and dotted(node.value.func) in acknowledgement_names
                        for node in ast.walk(helper)
                    ):
                        return True
                    nested = sorted(
                        (node.lineno, dotted(node.value.func).removeprefix("self."))
                        for node in ast.walk(helper)
                        if isinstance(node, ast.Await)
                        and isinstance(node.value, ast.Call)
                        and dotted(node.value.func).startswith("self.")
                    )
                    return bool(nested) and helper_has_ack(nested[0][1], seen)

                assert helper_has_ack(delegated[0][1]), f"{class_node.name}.{function.name}"
            else:
                for node in ast.walk(function):
                    if not isinstance(node, ast.Call) or node.lineno >= ack_line:
                        continue
                    name = dotted(node)
                    assert not any(fragment in name for fragment in dangerous_fragments), (
                        class_node.name, function.name, node.lineno, name
                    )
    assert len(callbacks) == 44


if __name__ == "__main__":
    test_registration_and_health()
    test_connection_errors_do_not_log_secrets()
    test_guess_pure_logic_and_images()
    test_async_lifecycle()
    test_every_slash_command_defers_first()
    print("standalone checks passed")
