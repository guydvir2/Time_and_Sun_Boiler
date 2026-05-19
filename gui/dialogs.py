"""
Themed dialog helpers — drop-in replacements for tkinter.messagebox
that match the dark UI palette.
"""
import tkinter as tk

# ── Palette (must match dashboard/settings) ──────────────────────
_CARD_BG  = "#1e2130"
_CARD_BDR = "#2d3450"
_INPUT_BG = "#252b3b"
_TEXT_FG  = "#e2e8f0"
_TEXT_DIM = "#94a3b8"
_ACC_BLUE  = "#3b82f6"
_ACC_GREEN = "#22c55e"
_ACC_RED   = "#ef4444"
_ACC_ORANGE = "#f59e0b"


def _base(parent, title: str, message: str, width=380):
    """Create a styled modal Toplevel, return (dlg, btn_frame)."""
    dlg = tk.Toplevel(parent)
    dlg.title(title)
    dlg.configure(bg=_CARD_BG)
    dlg.resizable(False, False)
    dlg.transient(parent)
    dlg.grab_set()
    dlg.minsize(width, 120)

    # Top accent bar
    tk.Frame(dlg, bg=_ACC_BLUE, height=3).pack(fill="x")

    # Message
    msg_frame = tk.Frame(dlg, bg=_CARD_BG, padx=24, pady=18)
    msg_frame.pack(fill="both", expand=True)
    tk.Label(msg_frame, text=message, bg=_CARD_BG, fg=_TEXT_FG,
             font=("Segoe UI", 10), wraplength=width - 60,
             justify="left").pack(anchor="w")

    # Bottom separator + button row
    tk.Frame(dlg, bg=_CARD_BDR, height=1).pack(fill="x")
    btn_frame = tk.Frame(dlg, bg=_CARD_BG, padx=16, pady=12)
    btn_frame.pack(fill="x")

    return dlg, btn_frame


def _centre(dlg, parent, width):
    """Call AFTER buttons are packed so height is correct."""
    dlg.update_idletasks()
    h = dlg.winfo_reqheight()
    x = parent.winfo_rootx() + parent.winfo_width()  // 2 - width // 2
    y = parent.winfo_rooty() + parent.winfo_height() // 2 - h // 2
    dlg.geometry(f"{width}x{h}+{x}+{y}")


def _btn(parent, text, cmd, bg, fg="#fff", padx=18):
    return tk.Button(parent, text=text, command=cmd,
                     bg=bg, fg=fg, font=("Segoe UI", 9, "bold"),
                     relief="flat", padx=padx, pady=6,
                     cursor="hand2", bd=0,
                     activebackground=bg, activeforeground=fg)


def show_info(parent, title: str, message: str):
    dlg, bf = _base(parent, title, message)
    _btn(bf, "OK", dlg.destroy, _ACC_BLUE).pack(side="right")
    _centre(dlg, parent, 380)
    parent.wait_window(dlg)


def show_error(parent, title: str, message: str):
    dlg, bf = _base(parent, title, message)
    _btn(bf, "OK", dlg.destroy, _ACC_RED).pack(side="right")
    _centre(dlg, parent, 380)
    parent.wait_window(dlg)


def show_warning(parent, title: str, message: str):
    dlg, bf = _base(parent, title, message)
    _btn(bf, "OK", dlg.destroy, _ACC_ORANGE).pack(side="right")
    _centre(dlg, parent, 380)
    parent.wait_window(dlg)


def ask_yes_no(parent, title: str, message: str) -> bool:
    result = [False]

    def _yes():
        result[0] = True
        dlg.destroy()

    dlg, bf = _base(parent, title, message)
    _btn(bf, "Yes", _yes,        _ACC_BLUE ).pack(side="right", padx=(6, 0))
    _btn(bf, "No",  dlg.destroy, _INPUT_BG, fg=_TEXT_FG).pack(side="right")
    _centre(dlg, parent, 380)
    parent.wait_window(dlg)
    return result[0]
