"""Update check: which versions (branches) exist on GitHub, which one is running, and the command to update.

Read-only on purpose: the app never changes the server. It only shows the one command to paste on the VM
(deploy.sh does the real work). The running version comes from BUILD_BRANCH / BUILD_COMMIT, which deploy.sh
bakes into the image at build time.
"""
import os
import shlex
import threading
import time

import requests

REPO = os.environ.get("UPDATE_REPO", "guydvir2/Time_and_Sun_Boiler")
APP_FOLDER = os.environ.get("UPDATE_APP_FOLDER", "~/pythons/dockers/Time_and_Sun_Boiler")
CACHE_SECONDS = 600            # GitHub allows 60 anonymous calls/hour; a check costs ~1 per branch
MAX_BRANCHES = 20
UPDATER_URL = os.environ.get("UPDATER_URL", "http://host.docker.internal:8099")   # the host updater (updater/install.sh)

_lock = threading.Lock()
_cache = {"at": 0.0, "branches": None}


def running():
    """The version this container was built from ('' = unknown, e.g. built before this feature)."""
    def clean(v):
        v = (v or "").strip()
        return "" if v.lower() == "unknown" else v
    return {"branch": clean(os.environ.get("BUILD_BRANCH")), "commit": clean(os.environ.get("BUILD_COMMIT"))}


def _get(path):
    r = requests.get(f"https://api.github.com/repos/{REPO}/{path}", timeout=8,
                     headers={"Accept": "application/vnd.github+json", "User-Agent": "boilermind-update-check"})
    r.raise_for_status()
    return r.json()


def fetch_branches():
    """[{'name','sha','date','message'}] for every branch (one GitHub call for the list + one per branch)."""
    out = []
    for b in _get(f"branches?per_page={MAX_BRANCHES}"):
        item = {"name": b["name"], "sha": b["commit"]["sha"], "date": "", "message": ""}
        try:
            c = _get(f"commits/{item['sha']}")["commit"]
            item["date"] = (c.get("committer") or {}).get("date", "")
            item["message"] = (c.get("message") or "").split("\n")[0][:90]
        except (requests.RequestException, KeyError, ValueError):
            pass                                          # keep the branch, just without date/message
        out.append(item)
    return out


def command(branch):
    return f"cd {APP_FOLDER} && ./deploy.sh {shlex.quote(branch)}"


def summarize(branches, run):
    """Pure function (easy to test): newest-first list, the suggestion, and one plain-language status line."""
    bs = sorted(branches, key=lambda b: b["date"], reverse=True)
    suggested = bs[0]["name"] if bs else ""
    cur = next((b for b in bs if b["name"] == run["branch"]), None)
    on_this = bool(cur and run["commit"] and cur["sha"].startswith(run["commit"]))
    if not run["commit"]:
        level, text = "yellow", "Running version unknown (built before update checks). Pick a branch below."
    elif on_this and cur["name"] == suggested:
        level, text = "green", f"Up to date: {cur['name']} @ {run['commit'][:7]}"
    elif on_this:
        level, text = "yellow", f"You run {cur['name']} @ {run['commit'][:7]}. A newer branch exists: {suggested}"
    elif cur:
        level, text = "yellow", f"Update available on {cur['name']} (running {run['commit'][:7]}, latest {cur['sha'][:7]})"
    else:
        level, text = "yellow", f"Your branch '{run['branch']}' is not on GitHub any more. Latest is {suggested}"
    rows = [{"name": b["name"], "sha": b["sha"][:7], "date": b["date"][:10], "message": b["message"],
             "current": b is cur, "command": command(b["name"])} for b in bs]
    return {"ok": True, "running": run, "branches": rows, "suggested": suggested, "level": level, "text": text,
            "updater": updater_configured()}


def check(refresh=False):
    with _lock:
        if refresh or _cache["branches"] is None or time.time() - _cache["at"] > CACHE_SECONDS:
            _cache["branches"], _cache["at"] = fetch_branches(), time.time()
        return summarize(_cache["branches"], running())


# ---- one-click update: ask the host updater service (the app itself has no Docker access) ----
def updater_configured():
    return bool(os.environ.get("UPDATER_TOKEN", ""))


def _updater(method, path, **kw):
    return requests.request(method, UPDATER_URL + path, headers={"X-Updater-Token": os.environ.get("UPDATER_TOKEN", "")},
                            timeout=8, **kw)


def start_update(branch):
    """(ok, message). Only branches GitHub listed in the last check are accepted."""
    if not updater_configured():
        return False, "One-click update is not installed on the server (run updater/install.sh)"
    known = {b["name"] for b in (_cache["branches"] or [])}
    if branch not in known:
        return False, "Unknown branch. Press 'Check for updates' first."
    try:
        r = _updater("POST", "/update", json={"branch": branch})
        return r.status_code == 202, (r.json().get("message") if r.headers.get("content-type", "").startswith("application/json") else f"Updater error {r.status_code}")
    except requests.RequestException as e:
        return False, f"Updater service not reachable ({type(e).__name__}). Is boilermind-updater running?"


def update_status():
    try:
        r = _updater("GET", "/status")
        return r.json() if r.ok else {"ok": False, "state": "unreachable", "message": f"Updater error {r.status_code}"}
    except (requests.RequestException, ValueError) as e:
        return {"ok": False, "state": "unreachable", "message": f"Updater service not reachable ({type(e).__name__})"}
