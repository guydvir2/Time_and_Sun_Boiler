"""
Config Tab — read and edit config.ini directly from the UI.
Shows the raw INI file in a styled editor; Save writes it back to disk.
A restart banner reminds the user that some changes need a restart.
"""
import tkinter as tk
from tkinter import ttk
from gui.dialogs import show_info, show_error, ask_yes_no

ACC_BLUE   = "#3b82f6"
ACC_GREEN  = "#22c55e"
ACC_RED    = "#ef4444"
ACC_ORANGE = "#f59e0b"
CARD_BG    = "#1e2130"
CARD_BDR   = "#2d3450"
INPUT_BG   = "#252b3b"
TEXT_FG    = "#e2e8f0"
TEXT_DIM   = "#94a3b8"
EDITOR_BG  = "#0f1520"
EDITOR_FG  = "#e2e8f0"
COMMENT_FG = "#4b5563"
SECTION_FG = "#60a5fa"
KEY_FG     = "#94a3b8"
VAL_FG     = "#f1f5f9"


class ConfigTab:
    def __init__(self, parent, config, colors):
        self.parent  = parent
        self.config  = config          # AppConfig instance — has .config_file path
        self.clr     = colors
        self._path   = config.config_file
        self._dirty  = False

        self._create_ui()
        self._load()

    # ─────────────────────────────────────────────────────────
    # LAYOUT
    # ─────────────────────────────────────────────────────────
    def _create_ui(self):
        root = tk.Frame(self.parent, bg=self.clr["BG"])
        root.pack(fill="both", expand=True, padx=10, pady=10)

        # ── Header bar ───────────────────────────────────────
        hdr = tk.Frame(root, bg=self.clr["BG"])
        hdr.pack(fill="x", pady=(0, 6))

        tk.Label(hdr, text="config.ini", bg=self.clr["BG"], fg=TEXT_FG,
                 font=("Segoe UI", 11, "bold")).pack(side="left")

        self._path_lbl = tk.Label(hdr, text=str(self._path),
                                  bg=self.clr["BG"], fg=TEXT_DIM,
                                  font=("Consolas", 8))
        self._path_lbl.pack(side="left", padx=(8, 0))

        # Buttons on the right
        btn_frame = tk.Frame(hdr, bg=self.clr["BG"])
        btn_frame.pack(side="right")
        self._btn(btn_frame, "⟳  Reload", self._reload, INPUT_BG).pack(side="left", padx=(0, 6))
        self._btn(btn_frame, "💾  Save",   self._save,   ACC_BLUE ).pack(side="left")

        # ── Restart warning banner (hidden initially) ────────
        self._banner = tk.Frame(root, bg="#7c2d12", pady=4)
        tk.Label(self._banner,
                 text="⚠  Some changes require a restart to take effect  (HA, MQTT, location, weather URL)",
                 bg="#7c2d12", fg="#fed7aa",
                 font=("Segoe UI", 9)).pack(padx=10)

        # ── Editor area ──────────────────────────────────────
        editor_outer = tk.Frame(root, bg=ACC_BLUE, pady=1)
        editor_outer.pack(fill="both", expand=True)
        editor_inner = tk.Frame(editor_outer, bg=EDITOR_BG)
        editor_inner.pack(fill="both", expand=True)

        # Line numbers
        self._lines = tk.Text(
            editor_inner, width=4,
            bg="#0a0f1a", fg="#3d4f6e",
            font=("Consolas", 10), state="disabled",
            relief="flat", bd=0, pady=6, padx=4,
            selectbackground=EDITOR_BG,
            highlightthickness=0)
        self._lines.pack(side="left", fill="y")

        tk.Frame(editor_inner, bg=CARD_BDR, width=1).pack(side="left", fill="y")

        # Scrollbar
        sb = ttk.Scrollbar(editor_inner, orient="vertical")
        sb.pack(side="right", fill="y")

        # Main editor
        self._text = tk.Text(
            editor_inner,
            bg=EDITOR_BG, fg=EDITOR_FG,
            font=("Consolas", 10),
            insertbackground=TEXT_FG,
            selectbackground=ACC_BLUE,
            relief="flat", bd=0,
            pady=6, padx=10,
            yscrollcommand=sb.set,
            undo=True, maxundo=200,
            wrap="none",
            highlightthickness=0)
        self._text.pack(side="left", fill="both", expand=True)
        sb.config(command=self._sync_scroll)

        # Horizontal scrollbar
        hsb = ttk.Scrollbar(root, orient="horizontal",
                             command=self._text.xview)
        hsb.pack(fill="x")
        self._text.config(xscrollcommand=hsb.set)

        # ── Status bar ───────────────────────────────────────
        self._status = tk.Label(root, text="", bg=self.clr["BG"],
                                fg=TEXT_DIM, font=("Segoe UI", 8), anchor="w")
        self._status.pack(fill="x", pady=(4, 0))

        # Syntax highlight tags
        self._text.tag_configure("comment", foreground=COMMENT_FG)
        self._text.tag_configure("section", foreground=SECTION_FG,
                                 font=("Consolas", 10, "bold"))
        self._text.tag_configure("key",     foreground=KEY_FG)
        self._text.tag_configure("value",   foreground=VAL_FG)

        # Bind events
        self._text.bind("<<Modified>>", self._on_modified)
        self._text.bind("<KeyRelease>", self._on_key)
        self._lines.bind("<MouseWheel>", lambda e: self._text.yview_scroll(
            -1*(e.delta//120), "units"))

    # ─────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────
    def _btn(self, parent, text, cmd, bg, fg="#fff"):
        return tk.Button(parent, text=text, command=cmd,
                         bg=bg, fg=fg,
                         font=("Segoe UI", 9, "bold"),
                         relief="flat", padx=14, pady=5,
                         cursor="hand2", bd=0,
                         activebackground=bg, activeforeground=fg)

    def _sync_scroll(self, *args):
        self._text.yview(*args)
        self._update_lines()

    def _update_lines(self, *_):
        """Redraw line numbers to match current text content."""
        count = int(self._text.index("end-1c").split(".")[0])
        self._lines.config(state="normal")
        self._lines.delete("1.0", "end")
        self._lines.insert("end", "\n".join(str(i) for i in range(1, count + 1)))
        self._lines.config(state="disabled")

    def _highlight(self):
        """Apply syntax colouring to the entire editor content."""
        self._text.tag_remove("comment", "1.0", "end")
        self._text.tag_remove("section", "1.0", "end")
        self._text.tag_remove("key",     "1.0", "end")
        self._text.tag_remove("value",   "1.0", "end")

        for i, line in enumerate(self._text.get("1.0", "end").splitlines(), 1):
            stripped = line.strip()
            start = f"{i}.0"
            end   = f"{i}.end"
            if stripped.startswith(";") or stripped.startswith("#"):
                self._text.tag_add("comment", start, end)
            elif stripped.startswith("[") and stripped.endswith("]"):
                self._text.tag_add("section", start, end)
            elif "=" in stripped:
                eq = line.index("=")
                self._text.tag_add("key",   f"{i}.0",      f"{i}.{eq}")
                self._text.tag_add("value", f"{i}.{eq+1}", end)

    # ─────────────────────────────────────────────────────────
    # LOAD / SAVE
    # ─────────────────────────────────────────────────────────
    def _load(self):
        try:
            content = self._path.read_text(encoding="utf-8") \
                      if hasattr(self._path, "read_text") \
                      else open(self._path, encoding="utf-8").read()
            self._text.config(state="normal")
            self._text.delete("1.0", "end")
            self._text.insert("1.0", content)
            self._text.edit_reset()
            self._highlight()
            self._update_lines()
            self._mark_clean()
            self._status.config(text=f"Loaded  {self._path}", fg=TEXT_DIM)
        except Exception as e:
            show_error(self.parent, "Load Error", str(e))

    def _reload(self):
        if self._dirty:
            if not ask_yes_no(self.parent, "Discard changes?",
                              "You have unsaved changes.\nReload from disk and lose them?"):
                return
        self._load()

    def _save(self):
        content = self._text.get("1.0", "end-1c")
        try:
            # Validate: parse as INI before writing
            import configparser, io
            cp = configparser.ConfigParser()
            cp.read_string(content)
        except Exception as e:
            show_error(self.parent, "Invalid INI", f"Cannot parse file:\n{e}")
            return

        try:
            path = self._path if not hasattr(self._path, "write_text") else None
            if path:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
            else:
                self._path.write_text(content, encoding="utf-8")
            self._mark_clean()
            self._banner.pack(fill="x", pady=(0, 6))   # show restart reminder
            self._status.config(text=f"Saved  {self._path}", fg=ACC_GREEN)
        except Exception as e:
            show_error(self.parent, "Save Error", str(e))

    def _mark_clean(self):
        self._dirty = False
        self._text.edit_modified(False)

    # ─────────────────────────────────────────────────────────
    # EVENTS
    # ─────────────────────────────────────────────────────────
    def _on_modified(self, _=None):
        if self._text.edit_modified():
            self._dirty = True
            self._status.config(text="● Unsaved changes", fg=ACC_ORANGE)
        self._text.edit_modified(False)

    def _on_key(self, _=None):
        self._highlight()
        self._update_lines()
