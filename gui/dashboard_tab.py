"""
Dashboard Tab — landing page.
Shows live status, Solar Mode card, weekly schedule, quick actions.
Refreshes every 30s automatically.
"""
import threading
import tkinter as tk
from datetime import date, datetime

from gui.dialogs import ask_yes_no, show_error, show_info
from gui.theme   import (ACC_BLUE, ACC_GREEN, ACC_ORANGE, ACC_RED,
                          CARD_BDR, CARD_BG, INPUT_BG, TEXT_DIM, TEXT_FG)

TEXT_VAL = "#f1f5f9"


class DashboardTab:
    REFRESH_MS = 30_000

    def __init__(self, parent, config, runtime_settings, scheduler,
                 ha_service, weather_service, data_manager,
                 colors, mqtt_service=None, on_boiler_state=None):
        self.parent          = parent
        self.config          = config
        self.rs              = runtime_settings
        self.scheduler       = scheduler
        self.ha_service      = ha_service
        self.weather_service = weather_service
        self.dm              = data_manager
        self.mqtt_service    = mqtt_service
        self.clr             = colors
        self.on_boiler_state = on_boiler_state

        self._prediction    = None
        self._pred_fetching = False

        self._create_ui()
        self._refresh()
        self._schedule_refresh()

    # ─────────────────────────────────────────────────────────
    # WIDGET HELPERS
    # ─────────────────────────────────────────────────────────

    def _card(self, parent, title="", pady=(0, 8)):
        outer = tk.Frame(parent, bg=ACC_BLUE, pady=1)
        outer.pack(fill="x", pady=pady)
        inner = tk.Frame(outer, bg=CARD_BG)
        inner.pack(fill="both", expand=True)
        if title:
            tk.Label(inner, text=title.upper(), bg=CARD_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 8, "bold"), padx=12, pady=5).pack(anchor="w")
            tk.Frame(inner, bg=CARD_BDR, height=1).pack(fill="x", padx=10)
        c = tk.Frame(inner, bg=CARD_BG, padx=12, pady=10)
        c.pack(fill="both", expand=True)
        return c

    def _card_in(self, parent, title="", expand=False):
        outer = tk.Frame(parent, bg=ACC_BLUE, pady=1)
        outer.grid(row=0, column=0, sticky="nsew" if expand else "ew")
        inner = tk.Frame(outer, bg=CARD_BG)
        inner.pack(fill="both", expand=True)
        if title:
            tk.Label(inner, text=title.upper(), bg=CARD_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 8, "bold"), padx=12, pady=5).pack(anchor="w")
            tk.Frame(inner, bg=CARD_BDR, height=1).pack(fill="x", padx=10)
        c = tk.Frame(inner, bg=CARD_BG, padx=12, pady=10)
        c.pack(fill="both", expand=True)
        return c

    def _row(self, parent, pady=3):
        f = tk.Frame(parent, bg=CARD_BG)
        f.pack(fill="x", pady=pady)
        return f

    def _kv(self, parent, key, value="—", value_color=TEXT_VAL):
        r = self._row(parent)
        tk.Label(r, text=key, bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 9), width=18, anchor="w").pack(side="left")
        lbl = tk.Label(r, text=value, bg=CARD_BG, fg=value_color,
                       font=("Segoe UI", 10, "bold"), anchor="w")
        lbl.pack(side="left", padx=(4, 0))
        return lbl

    def _inp(self, parent, var, width=6):
        return tk.Entry(parent, textvariable=var, width=width,
                        bg=INPUT_BG, fg=TEXT_FG, insertbackground=TEXT_FG,
                        relief="flat", justify="center", font=("Segoe UI", 10),
                        highlightthickness=1, highlightbackground=CARD_BDR,
                        highlightcolor=ACC_BLUE)

    def _time_widget(self, parent, hv, mv):
        f = tk.Frame(parent, bg=CARD_BG)
        self._inp(f, hv, 3).pack(side="left")
        tk.Label(f, text=":", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 11, "bold")).pack(side="left", padx=1)
        self._inp(f, mv, 3).pack(side="left")
        return f

    def _big_btn(self, parent, text, cmd, bg=ACC_BLUE):
        b = tk.Button(parent, text=text, command=cmd,
                      bg=bg, fg="#fff", font=("Segoe UI", 10, "bold"),
                      relief="flat", padx=0, pady=10, cursor="hand2", bd=0,
                      activebackground=self._darken(bg))
        b.bind("<Enter>", lambda e: b.config(bg=self._darken(bg)))
        b.bind("<Leave>", lambda e: b.config(bg=bg))
        return b

    @staticmethod
    def _darken(hex_color):
        r = max(0, int(hex_color[1:3], 16) - 20)
        g = max(0, int(hex_color[3:5], 16) - 20)
        b = max(0, int(hex_color[5:7], 16) - 20)
        return f"#{r:02x}{g:02x}{b:02x}"

    def _small_toggle(self, parent, var, cmd=None):
        W, H = 34, 18
        c = tk.Canvas(parent, width=W, height=H,
                      bg=CARD_BG, highlightthickness=0, cursor="hand2")
        def _draw(*_):
            c.delete("all")
            on = var.get()
            bg = ACC_BLUE if on else CARD_BDR
            c.create_arc(0,   0, H,   H, start=90,  extent=180,
                         style="pieslice", fill=bg, outline="")
            c.create_arc(W-H, 0, W,   H, start=270, extent=180,
                         style="pieslice", fill=bg, outline="")
            c.create_rectangle(H//2, 0, W-H//2, H, fill=bg, outline="")
            cx = W-10 if on else 10
            c.create_oval(cx-6, 2, cx+6, H-2, fill="#fff", outline="")
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
                       font=("Segoe UI", 7), padx=4, pady=2, cursor="hand2")
        lbl.pack()
        def _upd(*_):
            on = var.get()
            f.config(highlightbackground=ACC_BLUE if on else CARD_BDR)
            lbl.config(bg=ACC_BLUE if on else INPUT_BG,
                       fg="#fff" if on else TEXT_DIM)
        f.bind("<Button-1>",   lambda _: var.set(not var.get()))
        lbl.bind("<Button-1>", lambda _: var.set(not var.get()))
        var.trace_add("write", _upd)
        _upd()
        return f

    # ─────────────────────────────────────────────────────────
    # BUILD UI
    # ─────────────────────────────────────────────────────────

    def _create_ui(self):
        root = tk.Frame(self.parent, bg=self.clr["BG"])
        root.pack(fill="both", expand=True, padx=14, pady=10)

        cols = tk.Frame(root, bg=self.clr["BG"])
        cols.pack(fill="both", expand=True)
        cols.columnconfigure(0, weight=3)
        cols.columnconfigure(1, weight=2)
        cols.columnconfigure(2, weight=2)
        cols.rowconfigure(0, weight=1)

        left   = tk.Frame(cols, bg=self.clr["BG"])
        middle = tk.Frame(cols, bg=self.clr["BG"])
        right  = tk.Frame(cols, bg=self.clr["BG"])
        left.grid  (row=0, column=0, sticky="nsew", padx=(0, 4))
        middle.grid(row=0, column=1, sticky="nsew", padx=(4, 4))
        right.grid (row=0, column=2, sticky="nsew", padx=(4, 0))

        right.rowconfigure(0, weight=0)
        right.rowconfigure(1, weight=1)
        right.columnconfigure(0, weight=1)

        self._build_solar_card(left)
        self._build_weekly_card(middle)
        self._build_mqtt_topics_card(middle)
        self._build_start_now_card(right)
        self._build_oneshot_card(right)

    # ── SOLAR MODE CARD ──────────────────────────────────────

    def _build_solar_card(self, parent):
        outer = tk.Frame(parent, bg=ACC_BLUE, pady=1)
        outer.pack(fill="both", expand=True)
        inner = tk.Frame(outer, bg=CARD_BG)
        inner.pack(fill="both", expand=True)
        tk.Label(inner, text="SOLAR MODE", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 8, "bold"), padx=12, pady=5).pack(anchor="w")
        tk.Frame(inner, bg=CARD_BDR, height=1).pack(fill="x", padx=10)
        box = tk.Frame(inner, bg=CARD_BG, padx=12, pady=10)
        box.pack(fill="both", expand=True)

        # ── Solar active toggle ───────────────────────────────
        sr = self._row(box)
        tk.Label(sr, text="Solar active:", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 9), width=18, anchor="w").pack(side="left")
        self._solar_active_var = tk.BooleanVar(value=self.rs.get_solar_active())
        self._small_toggle(sr, self._solar_active_var,
                           self._on_solar_active_toggle).pack(side="left", padx=4)

        tk.Frame(box, bg=CARD_BDR, height=1).pack(fill="x", pady=(6, 8))

        # ── Schedule info ─────────────────────────────────────
        self._lbl_sched_status = self._kv(box, "Schedule status")
        self._lbl_trigger      = self._kv(box, "Trigger time")
        self._lbl_1st_dur      = self._kv(box, "1st run duration")
        self._lbl_2nd_time     = self._kv(box, "2nd run time")
        self._lbl_2nd_dur      = self._kv(box, "2nd run duration")
        self._lbl_total        = self._kv(box, "Total ON today",
                                          value_color=ACC_ORANGE)
        self._lbl_oneshot      = self._kv(box, "One-shot today",
                                          value_color=ACC_ORANGE)

        tk.Frame(box, bg=CARD_BDR, height=1).pack(fill="x", pady=(8, 6))

        # ── Prediction sub-section ────────────────────────────
        tk.Label(box, text="WEATHER PREDICTION", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 8, "bold")).pack(anchor="w")

        self._lbl_pred_temp  = self._kv(box, "Avg temperature")
        self._lbl_pred_cloud = self._kv(box, "Cloud cover")
        self._lbl_pred_eff   = self._kv(box, "Effective temp")
        self._lbl_pred_dur   = self._kv(box, "Predicted duration",
                                         value_color=ACC_BLUE)

        self._pred_status_lbl = tk.Label(box, text="", bg=CARD_BG, fg=TEXT_DIM,
                                          font=("Segoe UI", 8), anchor="w")
        self._pred_status_lbl.pack(anchor="w", pady=(4, 0))

        tk.Button(box, text="⟳  Refresh prediction",
                  command=self._fetch_prediction,
                  bg=INPUT_BG, fg=TEXT_DIM,
                  font=("Segoe UI", 8), relief="flat", padx=10, pady=4,
                  cursor="hand2", bd=0,
                  highlightthickness=1, highlightbackground=CARD_BDR
                  ).pack(anchor="w", pady=(4, 0))

    # ── WEEKLY SCHEDULE ──────────────────────────────────────

    def _build_weekly_card(self, parent):
        outer = tk.Frame(parent, bg=ACC_BLUE, pady=1)
        outer.pack(fill="both", expand=True)
        inner = tk.Frame(outer, bg=CARD_BG)
        inner.pack(fill="both", expand=True)
        tk.Label(inner, text="WEEKLY SCHEDULE", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 8, "bold"), padx=12, pady=5).pack(anchor="w")
        tk.Frame(inner, bg=CARD_BDR, height=1).pack(fill="x", padx=10)
        box = tk.Frame(inner, bg=CARD_BG, padx=10, pady=8)
        box.pack(fill="both", expand=True)

        self._weekly_widgets = []
        DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

        for preset in self.rs.get_weekly_presets():
            pid = preset["id"]
            pf  = tk.LabelFrame(box, text=f"  Preset {pid}  ",
                                 bg=CARD_BG, fg=TEXT_DIM,
                                 font=("Segoe UI", 8, "bold"),
                                 relief="groove", borderwidth=1,
                                 padx=6, pady=6)
            pf.pack(fill="x", pady=(0, 8))

            r0 = tk.Frame(pf, bg=CARD_BG)
            r0.pack(fill="x", pady=(0, 4))
            av = tk.BooleanVar(value=preset.get("active", False))
            tk.Label(r0, text="Active:", bg=CARD_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 9)).pack(side="left")
            self._small_toggle(r0, av).pack(side="left", padx=6)
            tk.Label(r0, text="Start:", bg=CARD_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 9)).pack(side="left", padx=(8, 0))
            t  = preset.get("start_time", "08:00").split(":")
            hv = tk.StringVar(value=t[0])
            mv = tk.StringVar(value=t[1])
            self._time_widget(pf, hv, mv).pack(in_=r0, side="left", padx=4)
            tk.Label(r0, text="Min:", bg=CARD_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 9)).pack(side="left", padx=(8, 0))
            dv = tk.StringVar(value=str(preset.get("duration", 30)))
            self._inp(pf, dv, width=4).pack(in_=r0, side="left", padx=4)

            r1 = tk.Frame(pf, bg=CARD_BG)
            r1.pack(fill="x")
            day_vars = {}
            for d in DAYS:
                dvar = tk.BooleanVar(value=(d in preset.get("days", [])))
                self._day_pill(r1, d, dvar).pack(side="left", padx=1, pady=2)
                day_vars[d] = dvar

            self._weekly_widgets.append({
                "id": pid, "active": av,
                "h": hv, "m": mv, "dur": dv, "days": day_vars,
            })

        tk.Button(box, text="💾  Save",
                  command=self._save_weekly,
                  bg=ACC_BLUE, fg="#fff",
                  font=("Segoe UI", 9, "bold"),
                  relief="flat", padx=0, pady=8, cursor="hand2", bd=0,
                  activebackground="#2563eb").pack(fill="x", pady=(4, 0))

    # ── MQTT TOPICS ──────────────────────────────────────────

    def _build_mqtt_topics_card(self, parent):
        outer = tk.Frame(parent, bg=ACC_BLUE, pady=1)
        outer.pack(fill="x", pady=(6, 0))
        inner = tk.Frame(outer, bg=CARD_BG)
        inner.pack(fill="both", expand=True)
        tk.Label(inner, text="MQTT TOPICS", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 8, "bold"), padx=12, pady=5).pack(anchor="w")
        tk.Frame(inner, bg=CARD_BDR, height=1).pack(fill="x", padx=10)
        box = tk.Frame(inner, bg=CARD_BG, padx=12, pady=8)
        box.pack(fill="x")

        ms = self.mqtt_service
        rows = []
        if ms:
            rows = [
                ("cmd",   getattr(ms, "cmd_topic",  "—")),
                ("stat",  getattr(ms, "stat_topic", "—")),
                ("tele",  getattr(ms, "tele_sub",   "—")),
            ]
            if getattr(ms, "cmd_enabled", False):
                for lbl, attr in [("adhoc",   "cmd_adhoc"),
                                   ("oneshot", "cmd_oneshot"),
                                   ("weekly",  "cmd_weekly")]:
                    v = getattr(ms, attr, "")
                    if v:
                        rows.append((lbl, v))
        else:
            rows = [("MQTT", "not configured")]

        for lbl, val in rows:
            r = tk.Frame(box, bg=CARD_BG)
            r.pack(fill="x", pady=1)
            tk.Label(r, text=f"{lbl}:", bg=CARD_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 8), width=8, anchor="w").pack(side="left")
            tk.Label(r, text=val, bg=CARD_BG, fg=TEXT_FG,
                     font=("Consolas", 8), anchor="w").pack(side="left", padx=(2, 0))

    # ── START NOW ────────────────────────────────────────────

    def _build_start_now_card(self, parent):
        outer = tk.Frame(parent, bg=self.clr["BG"])
        outer.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        outer.columnconfigure(0, weight=1)
        box = self._card_in(outer, "Start Now")

        r = self._row(box)
        tk.Label(r, text="Duration (min):", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 9)).pack(side="left")
        self._now_dur = tk.StringVar(value="45")
        self._inp(r, self._now_dur, width=6).pack(side="right")

        self._now_slider = tk.Scale(
            box, from_=5, to=180, orient="horizontal",
            variable=tk.IntVar(), showvalue=False,
            bg=CARD_BG, fg=TEXT_DIM, troughcolor=INPUT_BG,
            highlightthickness=0, bd=0, sliderrelief="flat",
            activebackground=ACC_BLUE,
            command=lambda v: self._now_dur.set(str(int(float(v)))))
        self._now_slider.set(45)
        self._now_slider.pack(fill="x", pady=(2, 8))

        def _sync_entry(*_):
            try:
                self._now_slider.set(int(self._now_dur.get()))
            except ValueError:
                pass
        self._now_dur.trace_add("write", _sync_entry)

        mode = self.rs.get_execution_mode()
        tk.Label(box, text=f"via  {mode}", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 8)).pack(anchor="w", pady=(0, 6))
        self._big_btn(box, "⚡  Start Now", self._start_now,
                      bg=ACC_GREEN).pack(fill="x")

    # ── ONE-SHOT ─────────────────────────────────────────────

    def _build_oneshot_card(self, parent):
        outer = tk.Frame(parent, bg=self.clr["BG"])
        outer.grid(row=1, column=0, sticky="nsew")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)
        box = self._card_in(outer, "Schedule One-Shot (today)", expand=True)

        r0 = self._row(box)
        tk.Label(r0, text="Time:", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 9)).pack(side="left")
        os = self.rs.get_one_shot()
        t  = os.get("start_time", "20:00").split(":")
        self._os_h = tk.StringVar(value=t[0])
        self._os_m = tk.StringVar(value=t[1])
        self._time_widget(box, self._os_h, self._os_m).pack(in_=r0, side="right")

        r1 = self._row(box)
        tk.Label(r1, text="Duration (min):", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 9)).pack(side="left")
        self._os_dur = tk.StringVar(value=str(os.get("duration", 30)))
        self._inp(r1, self._os_dur, width=6).pack(side="right")

        self._os_status_lbl = tk.Label(
            box, text=self._oneshot_status_text(),
            bg=CARD_BG, fg=ACC_ORANGE,
            font=("Segoe UI", 8, "bold"))
        self._os_status_lbl.pack(anchor="w", pady=(4, 6))

        self._big_btn(box, "🕐  Schedule", self._arm_oneshot,
                      bg="#0e7490").pack(fill="x", pady=(0, 4))
        self._big_btn(box, "✕  Disarm",   self._disarm_oneshot,
                      bg=INPUT_BG).pack(fill="x")

    def _oneshot_status_text(self):
        os = self.rs.get_one_shot()
        if os.get("armed"):
            return (f"⏳ Armed  {os.get('start_time','?')}  "
                    f"{os.get('duration','?')} min")
        return "Not armed"

    # ─────────────────────────────────────────────────────────
    # REFRESH
    # ─────────────────────────────────────────────────────────

    def _schedule_refresh(self):
        self.parent.after(self.REFRESH_MS, self._auto_refresh)

    def _auto_refresh(self):
        self._refresh()
        self._schedule_refresh()

    def _refresh(self):
        sch       = self.scheduler
        today_str = str(date.today())
        records   = self.dm.all_records()
        today_rec = next((r for r in records if r.get("date") == today_str), None)

        # ── Schedule status ───────────────────────────────────
        state = getattr(sch, "_current_state", None) or "—"
        self._lbl_sched_status.config(text=state)

        # ── Accumulate extra minutes (manual + oneshot) ───────
        manual_min  = getattr(sch, "_manual_minutes_today",  0)
        oneshot_min = getattr(sch, "_oneshot_minutes_today", 0)
        extra_min   = manual_min + oneshot_min

        # ── Trigger / run info ────────────────────────────────
        if today_rec:
            # Solar already fired — show actuals
            fired_at = today_rec.get("trigger_time", "?")
            dur1     = today_rec.get("first_run",    "?")
            dur2     = today_rec.get("second_run",   0)
            status   = today_rec.get("ha_status",    "?")
            self._lbl_trigger.config(
                text=f"Fired at {fired_at}  [{status}]",
                fg=ACC_GREEN if status == "OK" else ACC_RED)
            self._lbl_1st_dur.config(text=f"{dur1} min")
            dur2_int = 0
            try:
                dur2_int = int(dur2)
                t2 = sch.rs.get_second_run_time()
                self._lbl_2nd_time.config(text=t2 if dur2_int else "—")
                self._lbl_2nd_dur.config(
                    text=f"{dur2_int} min" if dur2_int else "—")
                total = int(dur1) + dur2_int + extra_min
            except (ValueError, TypeError):
                total = 0
            total_text = f"{total} min"
            if extra_min:
                total_text += f"  (+{extra_min} extra)"
            self._lbl_total.config(
                text=total_text if total else "—", fg=ACC_ORANGE)

        elif getattr(sch, "_trigger_time", None):
            # Calc done, waiting for trigger
            tt = sch._trigger_time.strftime("%H:%M")
            cr = sch._calc_result or {}
            self._lbl_trigger.config(text=tt, fg=TEXT_VAL)
            self._lbl_1st_dur.config(text=f"{cr.get('first_run','?')} min")
            dur2 = cr.get("second_run", 0)
            t2   = sch.rs.get_second_run_time()
            self._lbl_2nd_time.config(text=t2 if dur2 else "—")
            self._lbl_2nd_dur.config(text=f"{dur2} min" if dur2 else "—")
            try:
                total = int(cr.get("first_run", 0)) + int(dur2) + extra_min
            except (ValueError, TypeError):
                total = 0
            total_text = f"{total} min (predicted)"
            if extra_min:
                total_text += f"  (+{extra_min} extra)"
            self._lbl_total.config(
                text=total_text if total else "—", fg=ACC_ORANGE)

        else:
            self._lbl_trigger.config(text="—",  fg=TEXT_DIM)
            self._lbl_1st_dur.config(text="—")
            self._lbl_2nd_time.config(text="—")
            self._lbl_2nd_dur.config(text="—")
            if extra_min:
                self._lbl_total.config(
                    text=f"{extra_min} min (extra only)", fg=ACC_ORANGE)
            else:
                self._lbl_total.config(text="—", fg=TEXT_DIM)

        # ── Prediction ────────────────────────────────────────
        pred = getattr(sch, "_calc_result", None) or self._prediction
        if pred:
            self._apply_prediction(pred)
        elif not self._pred_fetching:
            self._fetch_prediction()

        # ── One-shot status label ─────────────────────────────
        self._os_status_lbl.config(text=self._oneshot_status_text())
        if oneshot_min:
            self._lbl_oneshot.config(text=f"{oneshot_min} min", fg=ACC_ORANGE)
        else:
            os_cfg = self.rs.get_one_shot()
            if os_cfg.get("armed"):
                t = os_cfg.get("start_time", "?")
                d = os_cfg.get("duration", "?")
                self._lbl_oneshot.config(text=f"Armed {t}  {d}min", fg=ACC_BLUE)
            else:
                self._lbl_oneshot.config(text="—", fg=TEXT_DIM)

        # ── Solar active toggle sync ──────────────────────────
        self._solar_active_var.set(self.rs.get_solar_active())

    # ─────────────────────────────────────────────────────────
    # PREDICTION
    # ─────────────────────────────────────────────────────────

    def _fetch_prediction(self):
        if self._pred_fetching:
            return
        self._pred_fetching = True
        self._pred_status_lbl.config(text="Fetching weather…")

        def _work():
            try:
                sunrise, sunset = self.weather_service.get_sun_times()
                result = self.weather_service.calculate_all(sunrise, sunset)
                self._prediction = result
                self.parent.after(0, lambda: self._apply_prediction(result))
            except Exception as ex:
                self.parent.after(0, lambda: self._pred_status_lbl.config(
                    text=f"Fetch failed: {ex}"))
            finally:
                self._pred_fetching = False

        threading.Thread(target=_work, daemon=True).start()

    def _apply_prediction(self, pred):
        self._lbl_pred_temp.config( text=f"{pred.get('avg_temp','?')} °C")
        self._lbl_pred_cloud.config(text=f"{pred.get('avg_cloud','?')} %")
        self._lbl_pred_eff.config(  text=f"{pred.get('effective_temp','?')} °C")
        self._lbl_pred_dur.config(  text=f"{pred.get('duration','?')} min",
                                    fg=ACC_BLUE)
        self._pred_status_lbl.config(
            text=f"Last updated {datetime.now().strftime('%H:%M')}", fg=TEXT_DIM)

    # ─────────────────────────────────────────────────────────
    # STATUS (called by main_window fan-out)
    # ─────────────────────────────────────────────────────────

    def set_boiler_status(self, status: str):
        self.parent.after(0, lambda: self._set_boiler_status(status))

    def set_ha_status(self, ok: bool):
        pass   # status bar handles this

    def set_mqtt_status(self, connected: bool):
        pass   # status bar handles this

    def _set_boiler_status(self, status: str):
        if self.on_boiler_state:
            self.on_boiler_state(str(status).upper())

    # ─────────────────────────────────────────────────────────
    # ACTIONS
    # ─────────────────────────────────────────────────────────

    def _on_solar_active_toggle(self):
        self.rs.set_solar_active(self._solar_active_var.get())

    def _start_now(self):
        try:
            dur = int(self._now_dur.get().strip())
            if dur <= 0: raise ValueError
        except ValueError:
            show_error(self.parent, "Invalid", "Duration must be a positive integer")
            return
        mode = self.rs.get_execution_mode()
        if not ask_yes_no(self.parent, "Confirm",
                          f"Start boiler now for {dur} min via {mode}?"):
            return

        def _run():
            ok  = self.scheduler.manual_run(dur)
            msg = f"Started {dur} min via {mode}" if ok else "Start failed — check log"
            self.parent.after(0, lambda: self._set_boiler_status("ON" if ok else "unknown"))
            fn = show_info if ok else show_error
            self.parent.after(0, lambda: fn(self.parent, "Done" if ok else "Error", msg))

        threading.Thread(target=_run, daemon=True).start()

    def _arm_oneshot(self):
        h = self._os_h.get().strip().zfill(2)
        m = self._os_m.get().strip().zfill(2)
        try:
            dur = int(self._os_dur.get().strip())
            if dur <= 0: raise ValueError
        except ValueError:
            show_error(self.parent, "Invalid", "Duration must be a positive integer")
            return
        self.rs.set_one_shot(f"{h}:{m}", dur, armed=True)
        self._os_status_lbl.config(
            text=f"⏳ Armed  {h}:{m}  {dur} min", fg=ACC_ORANGE)

    def _disarm_oneshot(self):
        os = self.rs.get_one_shot()
        self.rs.set_one_shot(os.get("start_time", "20:00"),
                             int(os.get("duration", 30)), armed=False)
        self._os_status_lbl.config(text="Not armed", fg=TEXT_DIM)

    def _save_weekly(self):
        DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        presets = []
        for w in self._weekly_widgets:
            h = w["h"].get().strip().zfill(2)
            m = w["m"].get().strip().zfill(2)
            try:
                dur = int(w["dur"].get().strip())
            except ValueError:
                show_error(self.parent, "Invalid",
                           f"Preset {w['id']}: duration must be integer")
                return
            presets.append({
                "id":         w["id"],
                "active":     w["active"].get(),
                "start_time": f"{h}:{m}",
                "duration":   dur,
                "days":       [d for d in DAYS if w["days"][d].get()],
            })
        self.rs.set_weekly_presets(presets)
        show_info(self.parent, "Saved", "Weekly schedule saved")

    def _refresh_weekly(self):
        DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        for w in self._weekly_widgets:
            pid     = w["id"]
            presets = self.rs.get_weekly_presets()
            p       = next((x for x in presets if x["id"] == pid), None)
            if not p: continue
            t = p.get("start_time", "08:00").split(":")
            w["active"].set(p.get("active", False))
            w["h"].set(t[0]);  w["m"].set(t[1])
            w["dur"].set(str(p.get("duration", 30)))
            for d in DAYS:
                w["days"][d].set(d in p.get("days", []))
