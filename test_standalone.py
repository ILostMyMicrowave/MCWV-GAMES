import ast
import asyncio
import contextlib
import io
import os
import types
from io import BytesIO
from pathlib import Path

os.environ.setdefault("GUILD_ID", "1501608673250640055")

from PIL import Image, ImageDraw
import games_bot as game


def test_registration_and_health():
    commands = game.bot.tree.get_commands(guild=game.guild_obj)
    names = {command.name for command in commands}
    assert len(commands) == 33
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
    assert len(checked) == 33

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
    assert len(callbacks) == 46


def test_no_blocking_db_on_event_loop():
    """Regression guard: async (event-loop) code must never touch the shared
    Postgres connection directly, and must not call the blocking DB helpers
    without going through asyncio.to_thread / async_db.

    Blocking Postgres on the loop freezes every user's interactions for the
    duration of the query — this test fails loudly if that ever comes back.
    """
    import ast
    path = Path(__file__).parent / "games_bot.py"
    tree = ast.parse(path.read_text())

    DB_HELPER_NAMES = {
        "games_coin_adjust", "games_coin_spend", "games_coin_transfer",
        "games_coin_balance", "games_coin_log_zero", "games_coin_log_test",
        "games_bank_move", "games_free_use", "games_prepaid_consume",
        "games_petdle_solved_today", "games_coin_rank", "games_is_unlimited",
        "get_user_cosmetic_roles", "set_user_cosmetic_role",
        "games_track", "games_track_user", "games_track_participants",
        "db_get_setting", "db_set_setting",
        "_ensure_case_claim_table", "_ensure_cosmetic_table",
        "games_jackpot_get", "games_get_eggs", "games_get_pets",
        "games_featured_egg", "games_lottery_round_key",
    }
    async_fns = [n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)]

    def enclosing_async(line):
        best = None
        for n in async_fns:
            if n.lineno <= line <= n.end_lineno:
                if best is None or n.lineno > best.lineno:
                    best = n
        return best

    sync_in_async = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and enclosing_async(n.lineno) is not None
    ]

    def in_nested_sync(line):
        return any(n.lineno <= line <= n.end_lineno for n in sync_in_async)

    parent_map = {}
    for n in ast.walk(tree):
        for ch in ast.iter_child_nodes(n):
            parent_map[id(ch)] = n

    problems = []
    for n in ast.walk(tree):
        # direct shared-connection access in async bodies
        if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "conn":
            fn = enclosing_async(n.lineno)
            if fn is None or in_nested_sync(n.lineno):
                continue
            # benign: status reads, and the shutdown close()
            if n.attr == "closed" or (fn.name == "close" and n.attr == "close"):
                continue
            problems.append(f"line {n.lineno}: conn.{n.attr} in {fn.name}")
        # blocking helper called directly from an async body
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in DB_HELPER_NAMES:
            fn = enclosing_async(n.lineno)
            if fn is None or in_nested_sync(n.lineno):
                continue
            cur = n
            wrapped = False
            while True:
                p = parent_map.get(id(cur))
                if p is None:
                    break
                if isinstance(p, ast.Call):
                    name = p.func.id if isinstance(p.func, ast.Name) else getattr(p.func, "attr", "")
                    if name in ("to_thread", "async_db"):
                        wrapped = True
                        break
                cur = p
            if not wrapped:
                problems.append(f"line {n.lineno}: {n.func.id}() in {fn.name}")

    assert not problems, "blocking DB access on the event loop:\n  " + "\n  ".join(problems)


def test_session_restart_recovery():
    """Restart recovery: live rounds restore, expired ones reveal, tower runs
    settle like the live clock, and `ended` tombstones are dropped."""
    sent = []
    rows = {}
    calls = {"tower_timeout": [], "tower_ask": []}

    class FakeChannel:
        def __init__(self, cid):
            self.id = cid

        async def send(self, **kwargs):
            sent.append((self.id, kwargs))

    async def fake_async_db(fn, *args, **kwargs):
        if fn is game.games_session_load:
            return list(rows.values())
        if fn is game.games_session_delete:
            rows.pop(str(args[0]), None)
            return True
        if fn is game.games_session_end:
            rows[str(args[0])][2]["ended"] = True
            return True
        return None

    now = game.time.time()
    rows["guess:101"] = ("guess:101", "guess", {
        "channel_id": 101, "started": now - 10, "pet_name": "Huge Pet", "mode": "zoom",
        "rewarded": True, "participants": [1], "message_id": 5,
    })
    rows["guess:102"] = ("guess:102", "guess", {
        "channel_id": 102, "started": now - 200, "pet_name": "Titanic Cat", "mode": "zoom",
        "rewarded": True, "participants": [], "message_id": 6,
    })
    rows["scramble:103"] = ("scramble:103", "scramble", {
        "channel_id": 103, "started": now - 30, "answer": "Huge Fox", "participants": [],
    })
    rows["hangman:104"] = ("hangman:104", "hangman", {
        "channel_id": 104, "started": now - 400, "word": "Huge Dog", "core": "hugedog", "participants": [],
    })
    rows["tower:201"] = ("tower:201", "tower", {
        "user_id": 201, "floor": 3, "hearts": 2, "score": 100, "combo": 1, "active": True,
        "started": now - 300, "floor_started": now - 10, "channel_id": 301,
        "kind": "trivia", "trivia_answer": "A", "trivia_options": ["A", "B"],
    })
    rows["tower:202"] = ("tower:202", "tower", {
        "user_id": 202, "floor": 5, "hearts": 3, "score": 0, "combo": 0, "active": True,
        "started": now - 600, "floor_started": now - 120, "channel_id": 302,
        "kind": "guess", "answer": "Huge Cat",
    })
    rows["tower:203"] = ("tower:203", "tower", {
        "user_id": 203, "floor": 2, "hearts": 3, "score": 25, "combo": 0, "active": True,
        "started": now - 100, "floor_started": now - 100, "channel_id": 303,
        "kind": None,
    })
    rows["tower:204"] = ("tower:204", "tower", {
        "user_id": 204, "floor": 2, "hearts": 3, "score": 25, "combo": 0, "active": True,
        "started": now - 100, "floor_started": now - 100, "channel_id": 999,
        "kind": None,
    })
    rows["guess:105"] = ("guess:105", "guess", {
        "channel_id": 105, "started": now - 10, "pet_name": "Huge Owl", "mode": "zoom",
        "rewarded": True, "participants": [], "message_id": 7, "ended": True,
    })

    real = {
        name: getattr(game, name)
        for name in (
            "async_db", "games_guess_clock", "games_tower_timeout", "games_tower_ask_chat",
        )
    }
    real_fetch, real_get = game.bot.fetch_channel, game.bot.get_channel

    async def stub_clock(channel_id, started):
        return None

    async def stub_tower_timeout(user_id, session):
        calls["tower_timeout"].append(user_id)

    async def stub_tower_ask(channel, user_id):
        calls["tower_ask"].append((channel.id, user_id))

    async def fake_fetch_channel(cid):
        return FakeChannel(cid)

    game.async_db = fake_async_db
    game.games_guess_clock = stub_clock
    game.games_tower_timeout = stub_tower_timeout
    game.games_tower_ask_chat = stub_tower_ask
    game.bot.fetch_channel = fake_fetch_channel
    game.bot.get_channel = lambda cid: FakeChannel(cid) if cid in (301, 302, 303) else None
    try:
        asyncio.run(game.games_recover_sessions())

        # Live rounds were restored (guess clock re-armed, row left alone).
        g101 = game.ACTIVE_GUESS_ROUNDS.get(101)
        assert g101 and g101["pet_name"] == "Huge Pet" and g101["started"] == now - 10
        assert g101["participants"] == {1}
        assert 101 in game.ACTIVE_GUESS_TASKS
        assert "guess:101" in rows and not rows["guess:101"][2].get("ended")
        assert game.ACTIVE_SCRAMBLE.get(103, {}).get("answer") == "Huge Fox"
        t201 = game.ACTIVE_TOWER.get(201)
        assert t201 and t201["floor"] == 3 and t201["kind"] == "trivia" and t201["active"]

        # Tower past the floor deadline settles like the live timeout; a
        # between-floors restart re-asks the floor; a lost channel ends the run.
        assert calls["tower_timeout"] == [202]
        assert calls["tower_ask"] == [(303, 203)]
        t204 = game.ACTIVE_TOWER.get(204)
        assert t204 and t204["active"] is False
        assert rows["tower:204"][2].get("ended") is True

        # Expired rounds revealed their answers and their rows were dropped.
        titles = " | ".join(
            str(kwargs.get("embed").title)
            for _cid, kwargs in sent
            if kwargs.get("embed") is not None
        )
        assert "nobody got it" in titles, titles
        assert "Hangman Expired" in titles, titles
        assert "guess:102" not in rows and "hangman:104" not in rows
        assert 102 not in game.ACTIVE_GUESS_ROUNDS and 104 not in game.ACTIVE_HANGMAN

        # `ended` tombstones are removed without restoring the round.
        assert "guess:105" not in rows
        assert 105 not in game.ACTIVE_GUESS_ROUNDS
    finally:
        for name, value in real.items():
            setattr(game, name, value)
        game.bot.fetch_channel, game.bot.get_channel = real_fetch, real_get
        for task in list(game.ACTIVE_GUESS_TASKS.values()):
            task.cancel()
        game.ACTIVE_GUESS_TASKS.clear()
        game.ACTIVE_GUESS_ROUNDS.clear()
        game.ACTIVE_SCRAMBLE.clear()
        game.ACTIVE_HANGMAN.clear()
        game.ACTIVE_TOWER.clear()


def test_case_drop_flow():
    """Case drop revamp: first click wins a free roll (audit logged at price 0),
    later clicks are rejected, staff can cancel, unclaimed drops expire, and the
    roll pool applies the same role filters as the paid /case flow."""

    class FakeMsg:
        def __init__(self):
            self.edits = []
            self.content = None
            self.embed = None

        async def edit(self, **kwargs):
            self.edits.append(kwargs)
            if "content" in kwargs:
                self.content = kwargs["content"]
            if "embed" in kwargs:
                self.embed = kwargs["embed"]

    class FakeResponse:
        def __init__(self):
            self.sent = []
            self.deferred = 0

        async def defer(self, **kw):
            self.deferred += 1

        async def send_message(self, *a, **kw):
            self.sent.append(a[0] if a else kw.get("content"))

    class FakeFollowup:
        def __init__(self, sink):
            self.sink = sink

        async def send(self, *a, **kw):
            m = FakeMsg()
            self.sink.append(m)
            if a:
                m.content = a[0]
            if "embed" in kw:
                m.embed = kw["embed"]
            return m

    class FakeUser:
        def __init__(self, uid):
            self.id = uid
            self.mention = f"<@{uid}>"

    class FakeChannel:
        def __init__(self, sink):
            self.sink = sink

        async def send(self, *a, **kw):
            m = FakeMsg()
            self.sink.append(m)
            if a:
                m.content = a[0]
            if "embed" in kw:
                m.embed = kw["embed"]
            return m

    class FakeInteraction:
        def __init__(self, uid):
            self.user = FakeUser(uid)
            self.channel_id = 42
            self.response = FakeResponse()
            self._msgs = []
            self.channel = FakeChannel(self._msgs)
            self.followup = FakeFollowup(self._msgs)

    class FakePermissions:
        administrator = False
        manage_guild = False
        manage_roles = False

    class AdminPermissions:
        administrator = True
        manage_guild = False
        manage_roles = False

    class FakeRole:
        def __init__(self, rid, admin=False):
            self.id = rid
            self.mention = f"<@&{rid}>"
            self.managed = False
            self.permissions = AdminPermissions() if admin else FakePermissions()

    def make_view(case_id, channel_id, roles, weights, staff_id=1):
        view = game.CaseDropView(case_id, f"Case {case_id}", "\U0001f381", roles, weights, staff_id, channel_id)
        view.message = FakeMsg()
        return view

    real_async_db, real_animate = game.async_db, game.games_animate

    async def fake_async_db(fn, *a, **kw):
        try:
            return fn() if callable(fn) else None
        except Exception:
            return None

    async def fake_animate(msg, frames, delay=0.45):
        return None

    async def _flow():
        # Roll pool: elevated roles are filtered, nothing-filler completes odds.
        guild = types.SimpleNamespace(
            get_role=lambda rid: {1: FakeRole(1), 2: FakeRole(2), 99: FakeRole(99, admin=True)}.get(rid))
        pool = game.games_case_roll_pool(
            guild, {"contents": [(1, 30.0), (2, 20.0), (99, 50.0)]})
        assert pool is not None
        roles, weights, _lines = pool
        assert [r.id for r in roles if r is not None] == [1, 2]
        assert weights == [30.0, 20.0, 50.0]
        assert game.games_case_roll_pool(guild, {"contents": [(99, 10.0)]}) is None
        assert game.games_case_roll_pool(guild, {"contents": [(1, 150.0)]}) is None
        assert game.games_case_roll_pool(guild, {"contents": []}) is None

        # Claim: first click wins the roll, disables buttons, frees the channel.
        role1 = FakeRole(1)
        view = make_view(7, 42, [role1], [100.0])
        game.ACTIVE_CASE_DROPS[42] = view.message
        inter1 = FakeInteraction(111)
        await view.claim_btn.callback(inter1)
        assert view.claimed
        assert all(child.disabled for child in view.children)
        assert 42 not in game.ACTIVE_CASE_DROPS
        assert inter1._msgs and inter1._msgs[0].embed is not None
        assert "Free Drop" in str(inter1._msgs[0].embed.title)

        # A second click cannot win.
        inter2 = FakeInteraction(222)
        await view.claim_btn.callback(inter2)
        assert inter2.response.sent and "already claimed" in inter2.response.sent[0]

        # Cancel: non-staff rejected, staff cancels.
        view2 = make_view(8, 43, [role1], [100.0])
        inter3 = FakeInteraction(999)
        await view2.cancel_btn.callback(inter3)
        assert view2.cancelled is False
        assert "only the staff member" in inter3.response.sent[0].lower()
        inter4 = FakeInteraction(1)
        await view2.cancel_btn.callback(inter4)
        assert view2.cancelled
        assert "cancelled" in view2.message.content.lower()

        # Unclaimed drops expire.
        view3 = make_view(9, 44, [role1], [100.0])
        await view3.on_timeout()
        assert "expired" in view3.message.content.lower()
        assert all(child.disabled for child in view3.children)

        # Nothing-fillers reveal a (free) loss, not a crash.
        view4 = make_view(10, 45, [None], [100.0])
        inter5 = FakeInteraction(333)
        await view4.claim_btn.callback(inter5)
        assert inter5._msgs and inter5._msgs[0].embed is not None
        assert "nothing" in str(inter5._msgs[0].embed.description)

    game.async_db = fake_async_db
    game.games_animate = fake_animate
    try:
        asyncio.run(_flow())
    finally:
        game.async_db = real_async_db
        game.games_animate = real_animate
        game.ACTIVE_CASE_DROPS.clear()


def test_spawn_case_drop_posts_publicly():
    """/spawn case must post the drop as a normal public channel message (the
    command only triggers it), with a staff-only ephemeral confirmation.
    Discord makes the FIRST followup of a deferred interaction inherit the
    defer's ephemeral state (the `ephemeral` flag is ignored), so the drop
    cannot be a followup of the /spawn interaction at all."""
    class FakeMsg:
        def __init__(self):
            self.content = None
            self.embed = None
            self.view = None

        async def edit(self, **kwargs):
            if "content" in kwargs:
                self.content = kwargs["content"]
            if "embed" in kwargs:
                self.embed = kwargs["embed"]
            if "view" in kwargs:
                self.view = kwargs["view"]

    class FakeChannel:
        def __init__(self, cid):
            self.id = cid
            self.sent = []

        async def send(self, *args, **kwargs):
            m = FakeMsg()
            self.sent.append(m)
            if args:
                m.content = args[0]
            if "embed" in kwargs:
                m.embed = kwargs["embed"]
            if "view" in kwargs:
                m.view = kwargs["view"]
            return m

    class FakeUser:
        def __init__(self, uid, roles=()):
            self.id = uid
            self.mention = f"<@{uid}>"
            self.roles = roles

    class FakeRole:
        def __init__(self, rid):
            self.id = rid
            self.name = f"Role {rid}"
            self.mention = f"<@&{rid}>"
            self.managed = False
            self.permissions = types.SimpleNamespace(
                administrator=False, manage_guild=False, manage_roles=False)

    class FakeGuild:
        def __init__(self, roles):
            self._roles = roles

        def get_role(self, rid):
            return self._roles.get(rid)

    class FakeResponse:
        def __init__(self):
            self.defer_kwargs = None

        async def defer(self, **kw):
            self.defer_kwargs = kw

    class FakeFollowup:
        def __init__(self, sink):
            self.sink = sink

        async def send(self, *a, **kw):
            self.sink.append((a, kw))
            return FakeMsg()

    real_cache, real_catalog = game._db_setting_cache, list(game._games_case_catalog_cache)
    now = game.time.monotonic()

    async def _flow():
        game._db_setting_cache = {
            game.GAMES_SETTING_ENABLED: (now + 60, "1"),
            game.GAMES_SETTING_STAFF_ROLES: (now + 60, game.json.dumps([777])),
        }
        game._games_case_catalog_cache = [{
            "id": 77, "name": "Drop Test Case", "price": 100,
            "emoji": "\U0001f381", "enabled": True, "contents": [(555, 100.0)],
        }]
        followups = []
        inter = types.SimpleNamespace(
            user=FakeUser(1, roles=[FakeRole(777)]),
            channel=FakeChannel(4242),
            channel_id=4242,
            guild=FakeGuild({555: FakeRole(555)}),
            response=FakeResponse(),
            followup=FakeFollowup(followups),
        )
        try:
            choice = game.app_commands.Choice(name="Free case drop", value="case")
            await game.games_spawn.callback(inter, choice)
            assert inter.response.defer_kwargs == {"ephemeral": True}
            assert len(inter.channel.sent) == 1
            drop = inter.channel.sent[0]
            assert drop.embed is not None and "FREE CASE DROP" in str(drop.embed.title)
            assert isinstance(drop.view, game.CaseDropView)
            assert game.ACTIVE_CASE_DROPS[4242] is drop
            # The command's own response is only the staff confirmation (ephemeral).
            assert len(followups) == 1 and followups[0][1].get("ephemeral") is True
            assert "drop is live" in followups[0][0][0]
        finally:
            game._db_setting_cache = real_cache
            game._games_case_catalog_cache = real_catalog
            game.ACTIVE_CASE_DROPS.clear()

    asyncio.run(_flow())


if __name__ == "__main__":
    test_registration_and_health()
    test_connection_errors_do_not_log_secrets()
    test_guess_pure_logic_and_images()
    test_async_lifecycle()
    test_every_slash_command_defers_first()
    test_no_blocking_db_on_event_loop()
    test_session_restart_recovery()
    test_case_drop_flow()
    test_spawn_case_drop_posts_publicly()
    print("standalone checks passed")
