"""
gui/theme.py — single source of palette + widget factories.
Import constants directly: from gui.theme import CARD_BG, ACC_BLUE, ...
"""
import tkinter as tk
from tkinter import ttk

# ── Colours ──────────────────────────────────────────────────
BG         = "#1e1e1e"
BG2        = "#252526"
BG3        = "#3e3e42"
CARD_BG    = "#1e2130"
CARD_BDR   = "#2d3450"
INPUT_BG   = "#252b3b"
TEXT_FG    = "#e2e8f0"
TEXT_DIM   = "#94a3b8"
ACC_BLUE   = "#3b82f6"
ACC_GREEN  = "#22c55e"
ACC_RED    = "#ef4444"
ACC_ORANGE = "#f59e0b"
SEP        = "#555555"

# Legacy dict — passed to LogTab / DataTab that still expect clr{}
CLR = {
    "BG": BG, "BG2": BG2, "BG3": BG3,
    "FG": TEXT_FG, "FG_DIM": TEXT_DIM,
    "SEP": SEP, "ACCENT": ACC_BLUE,
}


# ── Common widget helpers ─────────────────────────────────────

def btn_primary(parent, text, cmd, **kw) -> tk.Button:
    return tk.Button(parent, text=text, command=cmd,
                     bg=ACC_BLUE, fg="#fff",
                     font=("Segoe UI", 9, "bold"),
                     relief="flat", padx=16, pady=6,
                     cursor="hand2", bd=0,
                     activebackground=ACC_BLUE, activeforeground="#fff",
                     **kw)


def btn_secondary(parent, text, cmd, **kw) -> tk.Button:
    return tk.Button(parent, text=text, command=cmd,
                     bg=INPUT_BG, fg=TEXT_FG,
                     font=("Segoe UI", 9),
                     relief="flat", padx=14, pady=5,
                     cursor="hand2", bd=0,
                     activebackground=BG3, activeforeground=TEXT_FG,
                     **kw)


def card(parent, title: str = "", **kw) -> tk.Frame:
    """A titled dark card frame."""
    outer = tk.Frame(parent, bg=CARD_BDR, padx=1, pady=1)
    inner = tk.Frame(outer, bg=CARD_BG, padx=12, pady=10, **kw)
    inner.pack(fill="both", expand=True)
    if title:
        tk.Label(inner, text=title.upper(), bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 7, "bold"), pady=(0, 4)).pack(anchor="w")
    return inner


def dim_label(parent, text: str, **kw) -> tk.Label:
    return tk.Label(parent, text=text, bg=CARD_BG, fg=TEXT_DIM,
                    font=("Segoe UI", 9), **kw)


def val_label(parent, text: str = "—", color: str = TEXT_FG, **kw) -> tk.Label:
    return tk.Label(parent, text=text, bg=CARD_BG, fg=color,
                    font=("Segoe UI", 9, "bold"), **kw)


def sep(parent, vertical=False) -> tk.Frame:
    if vertical:
        return tk.Frame(parent, bg=CARD_BDR, width=1)
    return tk.Frame(parent, bg=CARD_BDR, height=1)


def setup_notebook_style():
    style = ttk.Style()
    style.theme_use("default")
    style.configure("TNotebook",     background=BG, borderwidth=0)
    style.configure("TNotebook.Tab", background=BG2, foreground=TEXT_FG,
                    padding=[15, 8], borderwidth=0, font=("Segoe UI", 10))
    style.map("TNotebook.Tab",
              background=[("selected", BG3)],
              foreground=[("selected", TEXT_FG)],
              expand=[("selected", [1, 1, 1, 0])])
