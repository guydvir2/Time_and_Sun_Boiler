"""Update check: pure summary logic (no network)."""
from boiler import updates

B = [{"name": "master", "sha": "a" * 40, "date": "2026-10-01T10:00:00Z", "message": "old"},
     {"name": "feature/open-controls", "sha": "b" * 40, "date": "2026-11-11T16:00:00Z", "message": "new"},
     {"name": "fix/x", "sha": "c" * 40, "date": "2026-11-05T10:00:00Z", "message": "mid"}]


def test_up_to_date_on_newest():
    r = updates.summarize(B, {"branch": "feature/open-controls", "commit": "bbbbbbb"})
    assert r["level"] == "green" and r["suggested"] == "feature/open-controls"
    assert [x["name"] for x in r["branches"]][0] == "feature/open-controls"      # newest first


def test_update_available_on_same_branch():
    r = updates.summarize(B, {"branch": "feature/open-controls", "commit": "1234567"})
    assert r["level"] == "yellow" and "Update available" in r["text"]


def test_other_branch_is_newer():
    r = updates.summarize(B, {"branch": "fix/x", "commit": "ccccccc"})
    assert r["level"] == "yellow" and "newer branch" in r["text"]


def test_unknown_running_version_and_command_is_quoted():
    r = updates.summarize(B + [{"name": "a b;rm", "sha": "d" * 40, "date": "", "message": ""}], {"branch": "", "commit": ""})
    assert r["level"] == "yellow" and "unknown" in r["text"].lower()
    assert [x["command"] for x in r["branches"] if x["name"] == "a b;rm"][0].endswith("./deploy.sh 'a b;rm'")


def test_same_flag_and_no_update_when_on_latest(monkeypatch):
    run = {"branch": "feature/open-controls", "commit": "bbbbbbb"}
    rows = {x["name"]: x for x in updates.summarize(B, run)["branches"]}
    assert rows["feature/open-controls"]["same"] and not rows["master"]["same"]
    monkeypatch.setenv("UPDATER_TOKEN", "t")
    monkeypatch.setattr(updates, "running", lambda: run)
    monkeypatch.setitem(updates._cache, "branches", B)
    monkeypatch.setitem(updates._cache, "at", __import__("time").time())
    called = []
    monkeypatch.setattr(updates, "_updater", lambda *a, **k: called.append(a))
    ok, msg = updates.start_update("feature/open-controls")
    assert not ok and "Nothing to update" in msg and not called            # the updater is never asked
