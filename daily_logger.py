import os
import json
from datetime import datetime, date

LOG_DIR = "logs"

class DailyLogger:
    def __init__(self, log_dir=LOG_DIR):
        os.makedirs(log_dir, exist_ok=True)
        self.date = date.today().isoformat()
        self.log_dir = log_dir
        self.log_path = os.path.join(log_dir, f"dailyoutput_{self.date}.log")
        self.json_path = os.path.join(log_dir, f"dailyoutput_{self.date}.json")
        self.html_path = os.path.join(log_dir, f"dailyoutput_{self.date}.html")
        self.events = []
        # header
        with open(self.log_path, "a") as f:
            f.write(f"===== DAILY LOG {self.date} START =====\n")

    def log(self, level: str, message: str, code: str = None, data: dict = None):
        ts = datetime.now().isoformat()
        entry = {"ts": ts, "level": level, "code": code, "message": message, "data": data}
        self.events.append(entry)
        line = f"[{ts}] {level}"
        if code:
            line += f" ({code})"
        line += f": {message}"
        if data is not None:
            try:
                line += " | " + json.dumps(data, default=str)
            except Exception:
                line += " | (data omitted)"
        line += "\n"
        with open(self.log_path, "a") as f:
            f.write(line)

    def save_summary(self, status: str = "unknown", error: str = None):
        summary = {
            "date": self.date,
            "generated_at": datetime.now().isoformat(),
            "status": status,
            "error": error,
            "events": self.events
        }
        # JSON
        with open(self.json_path, "w") as jf:
            json.dump(summary, jf, indent=2, default=str)
        # Simple HTML summary
        html_lines = [
            "<!doctype html>",
            "<html><head><meta charset='utf-8'><title>Daily Output Summary</title>",
            "<style>body{font-family:Arial,Helvetica,sans-serif;padding:20px} .ok{color:green}.err{color:red}</style>",
            "</head><body>"
        ]
        html_lines.append(f"<h1>Daily Summary for {self.date}</h1>")
        html_lines.append(f"<p>Generated: {summary['generated_at']}</p>")
        status_class = "ok" if status == "success" else "err"
        html_lines.append(f"<p>Status: <strong class='{status_class}'>{status}</strong></p>")
        if error:
            html_lines.append(f"<p>Error: <pre>{error}</pre></p>")
        html_lines.append("<h2>Events</h2><ul>")
        for e in self.events:
            msg = (e.get("message") or "").replace("<", "&lt;").replace(">", "&gt;")
            html_lines.append(f"<li><strong>[{e['ts']}] {e['level']}</strong> {msg}")
            if e.get("code"):
                html_lines.append(f" &nbsp; <em>code:</em> {e['code']}")
            if e.get("data") is not None:
                html_lines.append(f"<pre>{json.dumps(e['data'], default=str)}</pre>")
            html_lines.append("</li>")
        html_lines.append("</ul></body></html>")
        with open(self.html_path, "w") as hf:
            hf.write("\n".join(html_lines))

    def get_html_path(self):
        return self.html_path

# Module-level convenience
_global_logger = None

def init_logger(log_dir: str = LOG_DIR):
    global _global_logger
    _global_logger = DailyLogger(log_dir)
    return _global_logger

def log_event(level: str, message: str, code: str = None, data: dict = None):
    if _global_logger is None:
        init_logger()
    _global_logger.log(level, message, code, data)

def save_summary(status: str = "unknown", error: str = None):
    if _global_logger is None:
        init_logger()
    _global_logger.save_summary(status, error)

def get_summary_html_path():
    if _global_logger is None:
        init_logger()
    return _global_logger.get_html_path()
