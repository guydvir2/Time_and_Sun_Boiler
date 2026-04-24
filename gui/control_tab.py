"""
Control Tab — hosts two sub-tabs:
  • Dashboard  (landing page — DashboardTab)
  • Weekly     (3 presets + day pills)
Execution Mode moved to Settings tab.
"""
import tkinter as tk
from tkinter import ttk, messagebox

from gui.dashboard_tab import DashboardTab

DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

ACC_BLUE  = "#3b82f6"
ACC_GREEN = "#22c55e"
ACC_RED   = "#ef4444"
ACC_DIM   = "#6b7280"
CARD_BG   = "#1e2130"
CARD_BDR  = "#2d3450"
INPUT_BG  = "#252b3b"
TEXT_FG   = "#e2e8f0"
TEXT_DIM  = "#94a3b8"


class ControlTab:
    def __init__(self, parent, config, runtime_settings, scheduler,
                 ha_service, colors, weather_service=None,
                 data_manager=None, mqtt_service=None,
                 on_boiler_state=None):
        self.parent          = parent
        self.config          = config
        self.rs              = runtime_settings
        self.scheduler       = scheduler
        self.ha_service      = ha_service
        self.weather_service = weather_service
        self.data_manager    = data_manager
        self.mqtt_service    = mqtt_service
        self.clr             = colors
        self.on_boiler_state = on_boiler_state

        self._preset_widgets = []
        self.dashboard       = None   # DashboardTab reference

        self._create_ui()

    # ── Layout ───────────────────────────────────────────────

    def _create_ui(self):
        style = ttk.Style()
        style.configure("Control.TNotebook",
                        background=self.clr["BG"], borderwidth=0)
        style.configure("Control.TNotebook.Tab",
                        background=CARD_BG, foreground=TEXT_DIM,
                        padding=[14, 6], font=("Segoe UI", 9))
        style.map("Control.TNotebook.Tab",
                  background=[("selected", INPUT_BG)],
                  foreground=[("selected", TEXT_FG)])

        nb = ttk.Notebook(self.parent, style="Control.TNotebook")
        nb.pack(fill="both", expand=True, padx=0, pady=0)

        # Sub-tab 1: Dashboard
        dash_frame = tk.Frame(nb, bg=self.clr["BG"])
        nb.add(dash_frame, text="  📊  Dashboard  ")
        self.dashboard = DashboardTab(
            dash_frame,
            self.config,
            self.rs,
            self.scheduler,
            self.ha_service,
            self.weather_service,
            self.data_manager,
            self.clr,
            mqtt_service=self.mqtt_service,
            on_boiler_state=self.on_boiler_state
        )

        # Sub-tab 2: Weekly schedule
        weekly_frame = tk.Frame(nb, bg=self.clr["BG"])
        nb.add(weekly_frame, text="  📅  Weekly  ")
        self._build_weekly_tab(weekly_frame)

    # ── Widget helpers (weekly tab only) ─────────────────────

    def _card(self, parent, title, pady=(0, 10)):
        outer = tk.Frame(parent, bg=ACC_BLUE, pady=1)
        outer.pack(fill="x", pady=pady)
        inner = tk.Frame(outer, bg=CARD_BG)
        inner.pack(fill="both", expand=True)
        tk.Label(inner, text=title.upper(), bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 8, "bold"), padx=10, pady=5).pack(anchor="w")
        tk.Frame(inner, bg=CARD_BDR, height=1).pack(fill="x", padx=10)
        content = tk.Frame(inner, bg=CARD_BG, padx=10, pady=8)
        content.pack(fill="both", expand=True)
        return content

    def _row(self, parent):
        f = tk.Frame(parent, bg=CARD_BG)
        f.pack(fill="x", pady=3)
        return f

    def _lbl(self, parent, text):
        return tk.Label(parent, text=text, bg=CARD_BG, fg=TEXT_DIM,
                        font=("Segoe UI", 9))

    def _inp(self, parent, var, width=6):
        return tk.Entry(parent, textvariable=var, width=width,
                        bg=INPUT_BG, fg=TEXT_FG, insertbackground=TEXT_FG,
                        relief="flat", justify="center", font=("Segoe UI", 9),
                        highlightthickness=1, highlightbackground=CARD_BDR,
                        highlightcolor=ACC_BLUE)

    def _time_widget(self, parent, hv, mv):
        f = tk.Frame(parent, bg=CARD_BG)
        self._inp(f, hv, 3).pack(side="left")
        tk.Label(f, text=":", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 10, "bold")).pack(side="left", padx=1)
        self._inp(f, mv, 3).pack(side="left")
        return f

    def _toggle(self, parent, var, cmd=None):
        W, H = 40, 20
        c = tk.Canvas(parent, width=W, height=H,
                      bg=CARD_BG, highlightthickness=0, cursor="hand2")

        def _draw(*_):
            c.delete("all")
            on  = var.get()
            bg  = ACC_BLUE if on else CARD_BDR
            c.create_arc(0, 0, H, H, start=90, extent=180,
                         style="pieslice", fill=bg, outline="")
            c.create_arc(W-H, 0, W, H, start=270, extent=180,
                         style="pieslice", fill=bg, outline="")
            c.create_rectangle(H//2, 0, W-H//2, H, fill=bg, outline="")
            cx = W-12 if on else 12
            c.create_oval(cx-7, 2, cx+7, H-2, fill="#fff", outline="")

        def _click(_=None):
            var.set(not var.get())
            if cmd: cmd()

        c.bind("<Button-1>", _click)
        var.trace_add("write", _draw)
        _draw()
        return c

    def _day_pill(self, parent, day, var):
        f = tk.Frame(parent, bg=CARD_BG,
                     highlightthickness=1, highlightbackground=CARD_BDR)
        lbl = tk.Label(f, text=day, bg=INPUT_BG, fg=TEXT_DIM,
                       font=("Segoe UI", 9), padx=8, pady=4, cursor="hand2")
        lbl.pack()

        def _upd(*_):
            on = var.get()
            f.config(highlightbackground=ACC_BLUE if on else CARD_BDR)
            lbl.config(bg=ACC_BLUE if on else INPUT_BG,
                       fg="#fff" if on else TEXT_DIM)

        f.bind("<Button-1>", lambda _: var.set(not var.get()))
        lbl.bind("<Button-1>", lambda _: var.set(not var.get()))
        var.trace_add("write", _upd)
        _upd()
        return f

    # ── Weekly tab ───────────────────────────────────────────

    def _build_weekly_tab(self, parent):
        outer = tk.Frame(parent, bg=self.clr["BG"])
        outer.pack(fill="both", expand=True, padx=14, pady=10)

        # Scrollable
        sb     = ttk.Scrollbar(outer, orient="vertical")
        sb.pack(side="right", fill="y")
        canvas = tk.Canvas(outer, bg=self.clr["BG"],
                           highlightthickness=0, yscrollcommand=sb.set)
        canvas.pack(side="left", fill="both", expand=True)
        sb.config(command=canvas.yview)

        inner  = tk.Frame(canvas, bg=self.clr["BG"])
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.bind("<Configure>",
                   lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(win_id, width=e.width))
        canvas.bind_all("<MouseWheel>",
                        lambda e: canvas.yview_scroll(
                            int(-1*(e.delta/120)), "units"))

        box = self._card(inner, "Weekly Schedule")
        self._preset_widgets = []

        for preset in self.rs.get_weekly_presets():
            pid = preset["id"]
            pf  = tk.LabelFrame(
                box, text=f"  Preset {pid}  ",
                bg=CARD_BG, fg=TEXT_DIM,
                font=("Segoe UI", 9, "bold"),
                relief="groove", borderwidth=1,
                padx=8, pady=8)
            pf.pack(fill="x", pady=(0, 10))

            r0 = tk.Frame(pf, bg=CARD_BG)
            r0.pack(fill="x", pady=(0, 6))

            av = tk.BooleanVar(value=preset.get("active", False))
            self._lbl(r0, "Active:").pack(side="left")
            self._toggle(r0, av).pack(side="left", padx=6)

            self._lbl(r0, "Start:").pack(side="left", padx=(12, 0))
            t  = preset.get("start_time", "08:00").split(":")
            hv = tk.StringVar(value=t[0])
            mv = tk.StringVar(value=t[1])
            self._time_widget(pf, hv, mv).pack(in_=r0, side="left", padx=4)

            self._lbl(r0, "Min:").pack(side="left", padx=(12, 0))
            dv = tk.StringVar(value=str(preset.get("duration", 30)))
            self._inp(r0, dv, width=5).pack(side="left", padx=4)

            r1 = tk.Frame(pf, bg=CARD_BG)
            r1.pack(fill="x")
            day_vars = {}
            for d in DAYS:
                dvar = tk.BooleanVar(value=(d in preset.get("days", [])))
                self._day_pill(r1, d, dvar).pack(side="left", padx=2, pady=2)
                day_vars[d] = dvar

            self._preset_widgets.append({
                "id": pid, "active": av,
                "h": hv, "m": mv, "dur": dv, "days": day_vars
            })

        tk.Button(box, text="💾  Save Weekly Schedule",
                  command=self._save_weekly,
                  bg=ACC_BLUE, fg="#fff",
                  font=("Segoe UI", 10, "bold"),
                  relief="flat", padx=0, pady=10,
                  cursor="hand2", bd=0,
                  activebackground="#2563eb"
                  ).pack(fill="x", pady=(6, 0))

    def _save_weekly(self):
        presets = []
        for w in self._preset_widgets:
            h = w["h"].get().strip().zfill(2)
            m = w["m"].get().strip().zfill(2)
            try:
                dur = int(w["dur"].get().strip())
            except ValueError:
                messagebox.showerror(
                    "Invalid", f"Preset {w['id']}: duration must be integer")
                return
            presets.append({
                "id":         w["id"],
                "active":     w["active"].get(),
                "start_time": f"{h}:{m}",
                "duration":   dur,
                "days":       [d for d in DAYS if w["days"][d].get()]
            })
        self.rs.set_weekly_presets(presets)
        messagebox.showinfo("Saved", "Weekly schedule saved")

    # ── Public status updaters (called by main_window fan-out) ──

    def _on_mqtt_power_status(self, status: str):
        if self.dashboard:
            self.dashboard.set_boiler_status(status)

    def _set_boiler_status(self, status: str):
        if self.dashboard:
            self.dashboard.set_boiler_status(status)
