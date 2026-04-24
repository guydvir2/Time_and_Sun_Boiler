"""
Control Tab — operational view.
Weekly schedule, One-Shot, execution mode, boiler status.
"""
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

# Style constants (match settings_tab palette)
ACC_BLUE   = "#3b82f6"
ACC_GREEN  = "#22c55e"
ACC_RED    = "#ef4444"
ACC_DIM    = "#6b7280"
CARD_BG    = "#1e2130"
CARD_BDR   = "#2d3450"
INPUT_BG   = "#252b3b"
TEXT_FG    = "#e2e8f0"
TEXT_DIM   = "#94a3b8"


class ControlTab:
    def __init__(self, parent, config, runtime_settings, scheduler,
                 ha_service, colors, mqtt_service=None,
                 on_boiler_state=None):
        self.parent           = parent
        self.config           = config
        self.rs               = runtime_settings
        self.scheduler        = scheduler
        self.ha_service       = ha_service
        self.mqtt_service     = mqtt_service
        self.clr              = colors
        self.on_boiler_state  = on_boiler_state  # callback(status: str) → status bar

        self._preset_widgets  = []
        self._create_ui()

    # ── Layout ───────────────────────────────────────────────

    def _create_ui(self):
        outer = tk.Frame(self.parent, bg=self.clr["BG"])
        outer.pack(fill="both", expand=True, padx=10, pady=8)

        # Scrollable canvas
        sb = ttk.Scrollbar(outer, orient="vertical")
        sb.pack(side="right", fill="y")
        canvas = tk.Canvas(outer, bg=self.clr["BG"],
                           highlightthickness=0, yscrollcommand=sb.set)
        canvas.pack(side="left", fill="both", expand=True)
        sb.config(command=canvas.yview)

        inner = tk.Frame(canvas, bg=self.clr["BG"])
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        inner.bind("<Configure>",
                   lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(win_id, width=e.width))
        canvas.bind_all("<MouseWheel>",
                        lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        self._build_mode_section(inner)
        self._build_weekly_section(inner)
        self._build_oneshot_section(inner)

    # ── Cards ────────────────────────────────────────────────

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

    def _btn(self, parent, text, cmd, bg=ACC_BLUE, fg="#fff"):
        return tk.Button(parent, text=text, command=cmd,
                         bg=bg, fg=fg, font=("Segoe UI", 9, "bold"),
                         relief="flat", padx=12, pady=5, cursor="hand2", bd=0,
                         activebackground="#2563eb", activeforeground="#fff")

    def _toggle(self, parent, var, cmd=None):
        W, H = 40, 20
        c = tk.Canvas(parent, width=W, height=H,
                      bg=CARD_BG, highlightthickness=0, cursor="hand2")

        def _draw(*_):
            c.delete("all")
            on = var.get()
            bg = ACC_BLUE if on else CARD_BDR
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

    def _radio_pill(self, parent, text, var, color, cmd):
        f = tk.Frame(parent, bg=CARD_BG,
                     highlightthickness=1, highlightbackground=CARD_BDR)
        lbl = tk.Label(f, text=text, bg=INPUT_BG, fg=TEXT_DIM,
                       font=("Segoe UI", 9, "bold"), padx=12, pady=4,
                       cursor="hand2")
        lbl.pack()

        def _upd(*_):
            on = var.get() == text
            f.config(highlightbackground=color if on else CARD_BDR)
            lbl.config(bg=color if on else INPUT_BG,
                       fg="#fff" if on else TEXT_DIM)

        def _click(_=None):
            var.set(text)
            if cmd: cmd()

        f.bind("<Button-1>", _click)
        lbl.bind("<Button-1>", _click)
        var.trace_add("write", _upd)
        _upd()
        return f

    def _day_pill(self, parent, day, var):
        f = tk.Frame(parent, bg=CARD_BG,
                     highlightthickness=1, highlightbackground=CARD_BDR)
        lbl = tk.Label(f, text=day, bg=INPUT_BG, fg=TEXT_DIM,
                       font=("Segoe UI", 8), padx=6, pady=3, cursor="hand2")
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

    # ── Mode section ─────────────────────────────────────────

    def _build_mode_section(self, parent):
        box = self._card(parent, "Execution Mode")

        # Mode pills
        r0 = self._row(box)
        self._lbl(r0, "Route via:").pack(side="left")
        self._exec_mode_var = tk.StringVar(value=self.rs.get_execution_mode())
        for mode, col in (("HA", "#7c3aed"), ("MQTT", ACC_BLUE)):
            self._radio_pill(r0, mode, self._exec_mode_var,
                             col, self._save_exec_mode).pack(side="left", padx=4)

        # MQTT broker active toggle
        r1 = self._row(box)
        self._lbl(r1, "MQTT broker:").pack(side="left")
        self._mqtt_active_var = tk.BooleanVar(value=self.rs.get_mqtt_active())
        _init = self.rs.get_mqtt_active()
        self._toggle(r1, self._mqtt_active_var,
                     self._on_mqtt_active_toggle).pack(side="left", padx=8)
        self._mqtt_active_lbl = tk.Label(
            r1, text="on" if _init else "off",
            bg=CARD_BG, fg=ACC_GREEN if _init else TEXT_DIM,
            font=("Segoe UI", 8))
        self._mqtt_active_lbl.pack(side="left")

        tk.Frame(box, bg=CARD_BDR, height=1).pack(fill="x", pady=6)

        # Boiler status
        r2 = self._row(box)
        self._lbl(r2, "Boiler:").pack(side="left")
        self._boiler_dot = tk.Label(r2, text="●", bg=CARD_BG,
                                    fg=ACC_DIM, font=("Segoe UI", 16))
        self._boiler_dot.pack(side="left", padx=6)
        self._boiler_lbl = tk.Label(r2, text="unknown", bg=CARD_BG,
                                    fg=TEXT_FG, font=("Segoe UI", 10, "bold"))
        self._boiler_lbl.pack(side="left")
        tk.Button(r2, text="⟳", command=self._refresh_boiler_status,
                  bg=INPUT_BG, fg=TEXT_FG, font=("Segoe UI", 9),
                  relief="flat", padx=8, pady=3, cursor="hand2", bd=0,
                  highlightthickness=1, highlightbackground=CARD_BDR
                  ).pack(side="right")

        # NOTE: MQTT callbacks registered centrally in main_window._poll_connections.

    def _save_exec_mode(self):
        mode = self._exec_mode_var.get()
        self.rs.set_execution_mode(mode)
        if mode == "MQTT" and not self._mqtt_active_var.get():
            self._mqtt_active_var.set(True)

    def _on_mqtt_active_toggle(self):
        active = self._mqtt_active_var.get()
        self.rs.set_mqtt_active(active)
        self._mqtt_active_lbl.config(
            text="on" if active else "off",
            fg=ACC_GREEN if active else TEXT_DIM)
        if self.mqtt_service:
            self.mqtt_service.active = active
            if active and not self.mqtt_service.is_connected():
                threading.Thread(
                    target=self.mqtt_service.connect, daemon=True).start()
            elif not active and self.mqtt_service.is_connected():
                self.mqtt_service.disconnect()

    def _refresh_boiler_status(self):
        mode = self.rs.get_execution_mode()
        if mode == "MQTT" and self.mqtt_service:
            s = self.mqtt_service.get_status() or "unknown"
            self._set_boiler_status(s)
        else:
            def _check():
                entity = self.config.config["homeassistant"].get(
                    "BOILER_ENTITY_ID",
                    "switch.water_heater_water_heater_timeoutsw")
                try:
                    s = self.ha_service.get_boiler_state(entity)
                except Exception:
                    s = "unknown"
                self.parent.after(0, lambda: self._set_boiler_status(s))
            threading.Thread(target=_check, daemon=True).start()

    def _on_mqtt_power_status(self, status: str):
        self.parent.after(0, lambda: self._set_boiler_status(status))

    def _set_boiler_status(self, status: str):
        s = status.upper()
        if s == "ON":
            color, text = ACC_GREEN, "ON"
        elif s == "OFF":
            color, text = ACC_RED, "OFF"
        else:
            color, text = ACC_DIM, status
        self._boiler_dot.config(fg=color)
        self._boiler_lbl.config(text=text)
        if self.on_boiler_state:
            self.on_boiler_state(s)

    # ── Weekly section ───────────────────────────────────────

    def _build_weekly_section(self, parent):
        box = self._card(parent, "Weekly Schedule")
        self._preset_widgets = []

        for preset in self.rs.get_weekly_presets():
            pid = preset["id"]
            pf = tk.LabelFrame(box,
                               text=f"  Preset {pid}  ",
                               bg=CARD_BG, fg=TEXT_DIM,
                               font=("Segoe UI", 8, "bold"),
                               relief="groove", borderwidth=1,
                               padx=8, pady=6)
            pf.pack(fill="x", pady=(0, 8))

            r0 = tk.Frame(pf, bg=CARD_BG)
            r0.pack(fill="x", pady=(0, 4))

            av = tk.BooleanVar(value=preset.get("active", False))
            self._lbl(r0, "Active:").pack(side="left")
            self._toggle(r0, av).pack(side="left", padx=6)

            self._lbl(r0, "Start:").pack(side="left", padx=(10, 0))
            t = preset.get("start_time", "08:00").split(":")
            hv = tk.StringVar(value=t[0])
            mv = tk.StringVar(value=t[1])
            self._time_widget(pf, hv, mv).pack(in_=r0, side="left", padx=4)

            self._lbl(r0, "Min:").pack(side="left", padx=(8, 0))
            dv = tk.StringVar(value=str(preset.get("duration", 30)))
            self._inp(r0, dv, width=4).pack(side="left", padx=4)

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

        self._btn(box, "💾  Save Weekly Schedule",
                  self._save_weekly).pack(fill="x", pady=(4, 0))

    def _save_weekly(self):
        presets = []
        for w in self._preset_widgets:
            h = w["h"].get().strip().zfill(2)
            m = w["m"].get().strip().zfill(2)
            try:
                dur = int(w["dur"].get().strip())
            except ValueError:
                messagebox.showerror("Invalid",
                    f"Preset {w['id']}: duration must be integer")
                return
            presets.append({
                "id": w["id"], "active": w["active"].get(),
                "start_time": f"{h}:{m}", "duration": dur,
                "days": [d for d in DAYS if w["days"][d].get()]
            })
        self.rs.set_weekly_presets(presets)
        messagebox.showinfo("Saved", "Weekly schedule saved")

    # ── One-Shot section ─────────────────────────────────────

    def _build_oneshot_section(self, parent):
        box = self._card(parent, "One-Shot  (today only)")
        one_shot = self.rs.get_one_shot()

        r0 = self._row(box)
        self._lbl(r0, "Start time:").pack(side="left")
        t = one_shot.get("start_time", "08:00").split(":")
        self._os_h = tk.StringVar(value=t[0])
        self._os_m = tk.StringVar(value=t[1])
        self._time_widget(box, self._os_h, self._os_m).pack(in_=r0, side="right")

        r1 = self._row(box)
        self._lbl(r1, "Duration (min):").pack(side="left")
        self._os_dur = tk.StringVar(value=str(one_shot.get("duration", 30)))
        self._inp(r1, self._os_dur, width=6).pack(side="right")

        self._btn(box, "🚀  Arm One-Shot",
                  self._arm_oneshot, bg="#0e7490").pack(fill="x", pady=(10, 4))
        tk.Label(box,
                 text="Fires once today at the specified time.\n"
                      "Coexists with Solar & Weekly. Auto-disarms.",
                 bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 8), justify="left").pack(anchor="w")

    def _arm_oneshot(self):
        h = self._os_h.get().strip().zfill(2)
        m = self._os_m.get().strip().zfill(2)
        try:
            dur = int(self._os_dur.get().strip())
            if dur <= 0: raise ValueError
        except ValueError:
            messagebox.showerror("Invalid", "Duration must be positive integer")
            return
        self.rs.set_one_shot(f"{h}:{m}", dur, armed=True)
        messagebox.showinfo("Armed", f"One-shot: {h}:{m}  {dur} min")
