"""
Settings Tab — Left: LUT + Settings  |  Right: Graph (top) + Control Panel (bottom)
Style: dark cards, blue accent borders, clean typography inspired by modern dark UI.
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

ACC_BLUE   = "#3b82f6"
ACC_GREEN  = "#22c55e"
ACC_RED    = "#ef4444"
ACC_ORANGE = "#f59e0b"
ACC_DIM    = "#6b7280"
CARD_BG    = "#1e2130"
CARD_BDR   = "#2d3450"
INPUT_BG   = "#252b3b"
TEXT_FG    = "#e2e8f0"
TEXT_DIM   = "#94a3b8"


class SettingsTab:
    def __init__(self, parent, config, runtime_settings, scheduler,
                 weather_service, colors, mqtt_service=None, ha_service=None):
        self.parent           = parent
        self.config           = config
        self.runtime_settings = runtime_settings
        self.scheduler        = scheduler
        self.weather_service  = weather_service
        self.mqtt_service     = mqtt_service
        self.ha_service       = ha_service
        self.clr              = colors

        self.lut_tree          = None
        self.lbl_calc_result   = None
        self.graph_container   = None
        self.lut_graph_canvas  = None
        self.target_hour_var   = None
        self.target_min_var    = None
        self.cloud_penalty_var = None
        self.example_temp_var  = None
        self.example_cloud_var = None
        self.temp_lut          = dict(self.weather_service.temp_lut)

        self._preset_widgets   = []
        self._status_dot       = None
        self._status_lbl       = None
        self._tele_text        = None

        self._create_ui()

    # ─────────────────────────────────────────────────────────
    # TOP-LEVEL LAYOUT
    # ─────────────────────────────────────────────────────────
    def _create_ui(self):
        self._apply_styles()

        main = tk.Frame(self.parent, bg=self.clr["BG"])
        main.pack(fill="both", expand=True, padx=8, pady=8)

        # LEFT (fixed 300 px)
        left = tk.Frame(main, bg=self.clr["BG"], width=300)
        left.pack(side="left", fill="y", padx=(0, 8))
        left.pack_propagate(False)
        left.grid_columnconfigure(0, weight=1)
        for r, w in [(0,0),(1,0),(2,0),(3,0),(4,0),(5,1),(6,0)]:
            left.grid_rowconfigure(r, weight=w)

        self._build_lut_box(left)
        self._build_lut_buttons(left)
        self._build_settings_box(left)
        self._build_test_box(left)
        self._build_solar_mode_box(left)
        tk.Frame(left, bg=self.clr["BG"]).grid(row=5, column=0, sticky="nsew")
        self._build_save_button(left)

        # RIGHT outer frame (expands)
        right = tk.Frame(main, bg=self.clr["BG"])
        right.pack(side="right", fill="both", expand=True)

        # RIGHT is split horizontally:
        #   graph_area  (expands) | mqtt_area (fixed 280 px)
        graph_area = tk.Frame(right, bg=self.clr["BG"])
        graph_area.pack(side="left", fill="both", expand=True)

        # Vertical separator
        tk.Frame(right, bg=CARD_BDR, width=1).pack(side="left", fill="y", padx=(4, 0))

        # MQTT side panel — fixed width, plain (no scrollbar)
        mqtt_outer = tk.Frame(right, bg=self.clr["BG"], width=280)
        mqtt_outer.pack(side="left", fill="y")
        mqtt_outer.pack_propagate(False)

        self.graph_container = graph_area
        self._build_solar_panel(mqtt_outer)
        self._update_lut_graph()

    # ─────────────────────────────────────────────────────────
    # STYLES
    # ─────────────────────────────────────────────────────────
    def _apply_styles(self):
        s = ttk.Style()
        s.theme_use("default")
        s.configure("DarkLUT.Treeview",
                    background=INPUT_BG, foreground=TEXT_FG,
                    fieldbackground=INPUT_BG, rowheight=22, borderwidth=0)
        s.configure("DarkLUT.Treeview.Heading",
                    background=CARD_BG, foreground=TEXT_DIM,
                    font=("Segoe UI", 9, "bold"), relief="flat")
        s.map("DarkLUT.Treeview",
              background=[("selected", ACC_BLUE)],
              foreground=[("selected", "#fff")])
        s.configure("TNotebook", background=self.clr["BG"], borderwidth=0)
        s.configure("TNotebook.Tab",
                    background=CARD_BG, foreground=TEXT_DIM,
                    font=("Segoe UI", 9), padding=[10, 4])
        s.map("TNotebook.Tab",
              background=[("selected", INPUT_BG)],
              foreground=[("selected", TEXT_FG)])
        s.configure("Thin.Vertical.TScrollbar",
                    troughcolor=CARD_BG, background=CARD_BDR,
                    borderwidth=0, arrowsize=10)

    # ─────────────────────────────────────────────────────────
    # WIDGET HELPERS
    # ─────────────────────────────────────────────────────────
    def _card(self, parent, title="", padx=8, pady=(6,0)):
        """Card with blue top-accent. Packs itself into parent."""
        outer = tk.Frame(parent, bg=ACC_BLUE, pady=1)
        outer.pack(fill="x", padx=padx, pady=pady)
        inner = tk.Frame(outer, bg=CARD_BG)
        inner.pack(fill="both", expand=True)
        if title:
            tk.Label(inner, text=title.upper(), bg=CARD_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 8, "bold"), padx=10, pady=5
                     ).pack(anchor="w")
            tk.Frame(inner, bg=CARD_BDR, height=1).pack(fill="x", padx=10)
        content = tk.Frame(inner, bg=CARD_BG, padx=10, pady=8)
        content.pack(fill="both", expand=True)
        return content

    def _card_grid(self, parent, title, row, pady=(0,6)):
        """Card variant for grid-managed parents (left column)."""
        outer = tk.Frame(parent, bg=ACC_BLUE, pady=1)
        outer.grid(row=row, column=0, sticky="ew", pady=pady)
        inner = tk.Frame(outer, bg=CARD_BG)
        inner.pack(fill="both", expand=True)
        if title:
            tk.Label(inner, text=title.upper(), bg=CARD_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 8, "bold"), padx=10, pady=5
                     ).pack(anchor="w")
            tk.Frame(inner, bg=CARD_BDR, height=1).pack(fill="x", padx=10)
        content = tk.Frame(inner, bg=CARD_BG, padx=10, pady=8)
        content.pack(fill="both", expand=True)
        return content

    def _row(self, parent):
        f = tk.Frame(parent, bg=CARD_BG)
        f.pack(fill="x", pady=3)
        return f

    def _dim_lbl(self, parent, text, **kw):
        return tk.Label(parent, text=text, bg=CARD_BG, fg=TEXT_DIM,
                        font=("Segoe UI", 9), **kw)

    def _inp(self, parent, var, width=8):
        return tk.Entry(parent, textvariable=var, width=width,
                        bg=INPUT_BG, fg=TEXT_FG, insertbackground=TEXT_FG,
                        relief="flat", justify="center",
                        font=("Segoe UI", 9),
                        highlightthickness=1,
                        highlightbackground=CARD_BDR,
                        highlightcolor=ACC_BLUE)

    def _time_widget(self, parent, hv, mv):
        f = tk.Frame(parent, bg=CARD_BG)
        self._inp(f, hv, width=3).pack(side="left")
        tk.Label(f, text=":", bg=CARD_BG, fg=TEXT_DIM,
                 font=("Segoe UI", 10, "bold")).pack(side="left", padx=1)
        self._inp(f, mv, width=3).pack(side="left")
        return f

    def _btn_pri(self, parent, text, cmd, color=ACC_BLUE):
        return tk.Button(parent, text=text, command=cmd,
                         bg=color, fg="#fff",
                         font=("Segoe UI", 9, "bold"),
                         relief="flat", padx=12, pady=6,
                         cursor="hand2", bd=0,
                         activebackground="#2563eb",
                         activeforeground="#fff")

    def _btn_sec(self, parent, text, cmd, width=None):
        kw = {"width": width} if width else {}
        return tk.Button(parent, text=text, command=cmd,
                         bg=INPUT_BG, fg=TEXT_FG,
                         font=("Segoe UI", 9), relief="flat",
                         padx=8, pady=4, cursor="hand2", bd=0,
                         highlightthickness=1,
                         highlightbackground=CARD_BDR,
                         activebackground=CARD_BDR,
                         activeforeground=TEXT_FG, **kw)

    def _toggle(self, parent, var, cmd=None):
        W, H, R = 40, 20, 10
        c = tk.Canvas(parent, width=W, height=H,
                      bg=CARD_BG, highlightthickness=0, cursor="hand2")

        def _draw(*_):
            c.delete("all")
            on = var.get()
            bg = ACC_BLUE if on else CARD_BDR
            # track
            for kw in [dict(start=90,  extent=180),
                       dict(start=270, extent=180)]:
                c.create_arc(0, 0, H, H, style="pieslice",
                             fill=bg, outline="", **kw)
                c.create_arc(W-H, 0, W, H, style="pieslice",
                             fill=bg, outline="", **kw)
            c.create_rectangle(R, 0, W-R, H, fill=bg, outline="")
            # knob
            cx = W-R-2 if on else R+2
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
                       font=("Segoe UI", 9, "bold"),
                       padx=12, pady=4, cursor="hand2")
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

    # ─────────────────────────────────────────────────────────
    # LEFT COLUMN
    # ─────────────────────────────────────────────────────────
    def _build_lut_box(self, parent):
        box = self._card_grid(parent, "Temperature LUT", row=0)

        cols = ("#", "Temp °C", "Min")
        self.lut_tree = ttk.Treeview(box, columns=cols, show="headings",
                                     height=9, style="DarkLUT.Treeview")
        for col, w in zip(cols, [30, 80, 80]):
            self.lut_tree.heading(col, text=col)
            self.lut_tree.column(col, width=w, anchor="center")
        self.lut_tree.tag_configure("odd",  background="#1a1f2e", foreground=TEXT_FG)
        self.lut_tree.tag_configure("even", background=INPUT_BG,  foreground=TEXT_FG)

        sb = ttk.Scrollbar(box, orient="vertical", command=self.lut_tree.yview,
                           style="Thin.Vertical.TScrollbar")
        self.lut_tree.configure(yscrollcommand=sb.set)
        self.lut_tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")
        self._load_lut_to_tree()

    def _build_lut_buttons(self, parent):
        f = tk.Frame(parent, bg=self.clr["BG"])
        f.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        for txt, cmd in [("＋ Add", self._add_lut_row),
                          ("✎ Edit", self._edit_lut_row),
                          ("✕ Del",  self._delete_lut_row)]:
            self._btn_sec(f, txt, cmd).pack(side="left", fill="x", expand=True, padx=2)

    def _build_settings_box(self, parent):
        box = self._card_grid(parent, "Solar Settings", row=2)

        r0 = self._row(box)
        self._dim_lbl(r0, "Target ready:").pack(side="left")
        p = self.config.first_run_target_time.split(":")
        self.target_hour_var = tk.StringVar(value=p[0])
        self.target_min_var  = tk.StringVar(value=p[1])
        self._time_widget(box, self.target_hour_var,
                          self.target_min_var).pack(in_=r0, side="right")

        r1 = self._row(box)
        self._dim_lbl(r1, "Cloud penalty:").pack(side="left")
        self.cloud_penalty_var = tk.StringVar(
            value=str(self.weather_service.cloud_penalty_factor))
        self._inp(r1, self.cloud_penalty_var, width=7).pack(side="right")

    def _build_test_box(self, parent):
        box = self._card_grid(parent, "Test Calculation", row=3)

        ir = tk.Frame(box, bg=CARD_BG)
        ir.pack(fill="x", pady=(0, 6))
        self._dim_lbl(ir, "Temp:").pack(side="left")
        self.example_temp_var = tk.StringVar(value="12")
        self._inp(ir, self.example_temp_var, width=5).pack(side="left", padx=(3,10))
        self._dim_lbl(ir, "Clouds %:").pack(side="left")
        self.example_cloud_var = tk.StringVar(value="50")
        self._inp(ir, self.example_cloud_var, width=5).pack(side="left", padx=(3,8))
        tk.Button(ir, text="▶", command=self._update_lut_graph,
                  bg=ACC_BLUE, fg="#fff", font=("Segoe UI", 9, "bold"),
                  relief="flat", padx=8, pady=2, cursor="hand2", bd=0
                  ).pack(side="left")

        self.lbl_calc_result = tk.Label(box, text="", bg=CARD_BG, fg=TEXT_DIM,
                                        font=("Consolas", 8),
                                        wraplength=260, justify="left")
        self.lbl_calc_result.pack(fill="x")

    def _build_save_button(self, parent):
        tk.Button(parent, text="💾  Save Settings",
                  command=self._save_settings,
                  bg=ACC_BLUE, fg="#fff",
                  font=("Segoe UI", 10, "bold"),
                  relief="flat", padx=20, pady=10,
                  cursor="hand2", bd=0,
                  activebackground="#2563eb",
                  activeforeground="#fff"
                  ).grid(row=6, column=0, sticky="ew")


    def _build_solar_mode_box(self, parent):
        """Solar Mode card in left column, grid row 4."""
        box = self._card_grid(parent, "Solar Mode", row=4)

        r0 = self._row(box)
        self._dim_lbl(r0, "Active:").pack(side="left")
        self._solar_active_var = tk.BooleanVar(
            value=self.runtime_settings.get_solar_active())
        self._toggle(box, self._solar_active_var,
                     self._save_solar_active).pack(in_=r0, side="left", padx=8)

        r1 = self._row(box)
        self._dim_lbl(r1, "2nd run time:").pack(side="left")
        t2 = self.runtime_settings.get_second_run_time().split(":")
        self._2nd_hour_var = tk.StringVar(value=t2[0])
        self._2nd_min_var  = tk.StringVar(value=t2[1])
        self._time_widget(box, self._2nd_hour_var,
                          self._2nd_min_var).pack(in_=r1, side="right")

        self._btn_pri(box, "Save 2nd Run Time",
                      self._save_2nd_run_time).pack(fill="x", pady=(8, 0))

    # ─────────────────────────────────────────────────────────
    # RIGHT — SOLAR PANEL (below graph)
    # ─────────────────────────────────────────────────────────
    def _build_solar_panel(self, parent):
        """MQTT Direct Control + Telemetry below the graph in the right column."""
        # MQTT card
        mqtt_card = self._card(parent, "MQTT Direct Control")
        mqtt_card.pack(fill="x", padx=4, pady=(6, 6))

        broker   = getattr(self.config, "mqtt_broker_ip", None) or "not configured"
        topic    = getattr(self.config, "mqtt_tasmota_topic", "") or "?"
        enabled  = getattr(self.config, "mqtt_cmd_enabled", False)
        t_adhoc  = getattr(self.config, "mqtt_cmd_adhoc",   "") or "—"
        t_one    = getattr(self.config, "mqtt_cmd_oneshot", "") or "—"
        t_weekly = getattr(self.config, "mqtt_cmd_weekly",  "") or "—"

        tk.Label(mqtt_card, text=f"{broker}  |  {topic}",
                 bg=CARD_BG, fg=TEXT_DIM, font=("Consolas", 8)).pack(anchor="w", pady=(0,2))

        # Command topics info block
        cmd_frame = tk.Frame(mqtt_card, bg=INPUT_BG,
                             highlightthickness=1, highlightbackground=CARD_BDR)
        cmd_frame.pack(fill="x", pady=(0, 6))
        tk.Label(cmd_frame,
                 text=f"  CMD topics  ({'enabled' if enabled else 'disabled'})",
                 bg=INPUT_BG, fg=ACC_BLUE if enabled else ACC_DIM,
                 font=("Segoe UI", 8, "bold"), pady=3).pack(anchor="w")
        for label, val in [("  Ad-hoc:", t_adhoc),
                            ("  One-shot:", t_one),
                            ("  Weekly:", t_weekly)]:
            r = tk.Frame(cmd_frame, bg=INPUT_BG)
            r.pack(fill="x")
            tk.Label(r, text=label, bg=INPUT_BG, fg=TEXT_DIM,
                     font=("Segoe UI", 8), width=10, anchor="w").pack(side="left")
            tk.Label(r, text=val, bg=INPUT_BG, fg=TEXT_FG,
                     font=("Consolas", 8)).pack(side="left", padx=(2, 4))

        # Topic format
        rf = self._row(mqtt_card)
        self._dim_lbl(rf, "Format:").pack(side="left")
        self._topic_fmt_var = tk.StringVar(
            value=getattr(self.config, "mqtt_topic_format", "device_first"))
        for fmt, lbl in (("device_first", f"{topic}/cmnd/…"),
                         ("standard",     f"cmnd/{topic}/…")):
            tk.Radiobutton(rf, text=lbl,
                           variable=self._topic_fmt_var, value=fmt,
                           bg=CARD_BG, fg=TEXT_FG, selectcolor=INPUT_BG,
                           activebackground=CARD_BG, font=("Segoe UI", 8),
                           command=self._save_topic_format
                           ).pack(side="left", padx=4)

        # Broker / device indicators
        rb = self._row(mqtt_card)
        self._dim_lbl(rb, "Broker:").pack(side="left")
        self._mqtt_broker_dot = tk.Label(rb, text="●", bg=CARD_BG,
                                         fg=ACC_DIM, font=("Segoe UI", 14))
        self._mqtt_broker_dot.pack(side="left", padx=6)
        self._mqtt_broker_lbl = tk.Label(rb, text="disconnected",
                                         bg=CARD_BG, fg=TEXT_DIM,
                                         font=("Segoe UI", 9, "bold"))
        self._mqtt_broker_lbl.pack(side="left")

        rd = self._row(mqtt_card)
        self._dim_lbl(rd, "Boiler:").pack(side="left")
        self._mqtt_device_dot = tk.Label(rd, text="●", bg=CARD_BG,
                                         fg=ACC_DIM, font=("Segoe UI", 14))
        self._mqtt_device_dot.pack(side="left", padx=6)
        self._mqtt_device_lbl = tk.Label(rd, text="unknown",
                                         bg=CARD_BG, fg=TEXT_DIM,
                                         font=("Segoe UI", 9, "bold"))
        self._mqtt_device_lbl.pack(side="left")

        # Connect buttons
        cb_row = self._row(mqtt_card)
        tk.Button(cb_row, text="⚡ Connect",
                  command=self._mqtt_connect,
                  bg=ACC_BLUE, fg="#fff", font=("Segoe UI", 9, "bold"),
                  relief="flat", padx=10, pady=4, cursor="hand2", bd=0,
                  activebackground="#2563eb").pack(side="left", padx=(0, 6))
        tk.Button(cb_row, text="✕ Disconnect",
                  command=self._mqtt_disconnect,
                  bg=INPUT_BG, fg=TEXT_FG, font=("Segoe UI", 9),
                  relief="flat", padx=10, pady=4, cursor="hand2", bd=0,
                  highlightthickness=1, highlightbackground=CARD_BDR
                  ).pack(side="left", padx=(0, 6))
        tk.Button(cb_row, text="⟳",
                  command=self._mqtt_refresh_conn,
                  bg=INPUT_BG, fg=TEXT_FG, font=("Segoe UI", 9),
                  relief="flat", padx=8, pady=4, cursor="hand2", bd=0,
                  highlightthickness=1, highlightbackground=CARD_BDR
                  ).pack(side="left")

        # Duration + ON/OFF
        r1m = self._row(mqtt_card)
        self._dim_lbl(r1m, "Duration (min):").pack(side="left")
        self._mqtt_dur_var = tk.StringVar(value="30")
        self._inp(r1m, self._mqtt_dur_var, width=6).pack(side="right")

        br = self._row(mqtt_card)
        tk.Button(br, text="⚡ ON", command=self._mqtt_manual_on,
                  bg=ACC_GREEN, fg="#fff", font=("Segoe UI", 9, "bold"),
                  relief="flat", padx=16, pady=6, cursor="hand2", bd=0,
                  activebackground="#16a34a").pack(side="left", padx=(0, 8))
        tk.Button(br, text="⏹ OFF", command=self._mqtt_manual_off,
                  bg=ACC_RED, fg="#fff", font=("Segoe UI", 9, "bold"),
                  relief="flat", padx=16, pady=6, cursor="hand2", bd=0,
                  activebackground="#b91c1c").pack(side="left")

        # Send CMD
        rc0 = self._row(mqtt_card)
        self._dim_lbl(rc0, "Subtopic:").pack(side="left")
        self._cmd_subtopic_var = tk.StringVar(value="POWER")
        self._inp(rc0, self._cmd_subtopic_var, width=14).pack(side="right")
        rc1 = self._row(mqtt_card)
        self._dim_lbl(rc1, "Payload:").pack(side="left")
        self._cmd_payload_var = tk.StringVar(value="")
        self._inp(rc1, self._cmd_payload_var, width=14).pack(side="right")
        self._btn_sec(mqtt_card, "▶  Send", self._mqtt_send_cmd
                      ).pack(fill="x", pady=(6, 0))

        # Telemetry
        tele_card = self._card(parent, "Telemetry")
        tele_card.pack(fill="x", padx=4, pady=(0, 8))

        self._tele_text = tk.Text(
            tele_card, height=7, bg="#0f1520", fg="#00e5a0",
            font=("Consolas", 8), state="disabled",
            relief="flat", wrap="none", selectbackground=ACC_BLUE)
        self._tele_text.pack(fill="x")

        tr = self._row(tele_card)
        self._btn_sec(tr, "⟳ Refresh", self._refresh_tele).pack(side="left", padx=(0, 4))
        self._btn_sec(tr, "🗑 Clear",   self._clear_tele).pack(side="left")

        # NOTE: MQTT callbacks (on_status_change, on_connect_change, on_tele)
        # are registered centrally in main_window._poll_connections.
        # Do NOT re-register here — it would overwrite the fan-out closures.

    
    def notify_boiler_state(self, status: str):
        """Called by main_window when boiler status changes (any source)."""
        self.parent.after(0, lambda: self._update_mqtt_device_indicator(status))

    def notify_broker_state(self, connected: bool):
        """Called by main_window when MQTT broker connects/disconnects."""
        self.parent.after(0, lambda: self._update_mqtt_broker_indicator(connected))

    # ─────────────────────────────────────────────────────────
    # LUT CRUD
    # ─────────────────────────────────────────────────────────
    def _load_lut_to_tree(self):
        self.lut_tree.delete(*self.lut_tree.get_children())
        for i, t in enumerate(sorted(self.temp_lut), start=1):
            self.lut_tree.insert("", "end",
                                 values=(i, t, self.temp_lut[t]),
                                 tags=("odd" if i%2 else "even",))

    def _add_lut_row(self):
        d = self._lut_dlg("Add Point")
        tv, dv = tk.StringVar(), tk.StringVar()
        self._lut_fields(d, tv, dv)
        def save():
            try:
                self.temp_lut[int(tv.get())] = int(dv.get())
                self._load_lut_to_tree(); self._update_lut_graph(); d.destroy()
            except ValueError as e:
                messagebox.showerror("Error", str(e), parent=d)
        self._lut_btns(d, save)

    def _edit_lut_row(self):
        sel = self.lut_tree.selection()
        if not sel:
            messagebox.showwarning("No Selection", "Select a row"); return
        v = self.lut_tree.item(sel[0], "values"); old_t = int(v[1])
        d = self._lut_dlg("Edit Point")
        tv, dv = tk.StringVar(value=v[1]), tk.StringVar(value=v[2])
        self._lut_fields(d, tv, dv, sel_t=True)
        def save():
            try:
                nt, nd = int(tv.get()), int(dv.get())
                if nt != old_t: del self.temp_lut[old_t]
                self.temp_lut[nt] = nd
                self._load_lut_to_tree(); self._update_lut_graph(); d.destroy()
            except ValueError as e:
                messagebox.showerror("Error", str(e), parent=d)
        self._lut_btns(d, save)

    def _delete_lut_row(self):
        sel = self.lut_tree.selection()
        if not sel:
            messagebox.showwarning("No Selection", "Select a row"); return
        if len(self.temp_lut) <= 2:
            messagebox.showerror("Cannot Delete", "Need ≥ 2 points"); return
        t = int(self.lut_tree.item(sel[0], "values")[1])
        if messagebox.askyesno("Confirm", f"Delete {t}°C?"):
            del self.temp_lut[t]
            self._load_lut_to_tree(); self._update_lut_graph()

    def _lut_dlg(self, title):
        d = tk.Toplevel(self.parent)
        d.title(title); d.configure(bg=self.clr["BG"])
        d.transient(self.parent); d.grab_set(); d.resizable(False, False)
        return d

    def _lut_fields(self, dlg, tv, dv, sel_t=False):
        f = tk.Frame(dlg, bg=self.clr["BG"], padx=16, pady=12)
        f.pack(fill="both", expand=True)
        for lbl_txt, var in (("Temp (°C):", tv), ("Duration (min):", dv)):
            r = tk.Frame(f, bg=self.clr["BG"])
            r.pack(fill="x", pady=4)
            tk.Label(r, text=lbl_txt, bg=self.clr["BG"], fg=TEXT_FG,
                     font=("Segoe UI", 9), width=14, anchor="w").pack(side="left")
            e = tk.Entry(r, textvariable=var, width=8,
                         bg=INPUT_BG, fg=TEXT_FG, font=("Consolas", 10),
                         justify="center", relief="flat",
                         highlightthickness=1,
                         highlightbackground=CARD_BDR,
                         highlightcolor=ACC_BLUE)
            e.pack(side="left"); e.focus()
            if sel_t and lbl_txt.startswith("Temp"):
                e.select_range(0, tk.END)

    def _lut_btns(self, dlg, save_cb):
        f = dlg.winfo_children()[0]
        bf = tk.Frame(f, bg=self.clr["BG"])
        bf.pack(fill="x", pady=(8, 0))
        self._btn_pri(bf, "Save", save_cb).pack(side="left", padx=(0, 8))
        self._btn_sec(bf, "Cancel", dlg.destroy).pack(side="left")
        dlg.update_idletasks()
        w, h = dlg.winfo_reqwidth(), dlg.winfo_reqheight()
        dlg.geometry(f"{w}x{h}+{dlg.winfo_screenwidth()//2-w//2}"
                     f"+{dlg.winfo_screenheight()//2-h//2}")
        dlg.bind("<Return>", lambda e: save_cb())

    # ─────────────────────────────────────────────────────────
    # GRAPH
    # ─────────────────────────────────────────────────────────
    def _update_lut_graph(self):
        if self.lut_graph_canvas:
            self.lut_graph_canvas.get_tk_widget().destroy()
        if not self.temp_lut:
            return
        try:
            et  = float(self.example_temp_var.get())
            ec  = float(self.example_cloud_var.get())
            pen = float(self.cloud_penalty_var.get())
        except (ValueError, AttributeError):
            et, ec, pen = 12.0, 50.0, self.weather_service.cloud_penalty_factor

        base     = self.weather_service.calculate_duration(et)
        with_pen = self.weather_service.calculate_duration(et - (ec/100)*pen)
        diff = with_pen - base
        pct  = (diff/base*100) if base > 0 else 0
        if self.lbl_calc_result:
            self.lbl_calc_result.config(
                text=f"Temp={et:.1f}°C  Clouds={ec:.0f}%   "
                     f"Base: {base}min  →  {with_pen}min  ({diff:+d}, {pct:+.1f}%)")

        BG  = self.clr["BG"]
        fig = plt.Figure(figsize=(8, 4.2), facecolor=BG)
        ax  = fig.add_subplot(111)
        ax.set_facecolor(CARD_BG)

        temps = sorted(self.temp_lut)
        durs  = [self.temp_lut[t] for t in temps]

        if len(temps) >= 2:
            import numpy as np
            tr = np.linspace(min(temps), max(temps), 200)

            def interp(arr, shift=0.0):
                out = []
                for t in arr:
                    ts = t - shift
                    for i in range(len(temps)-1):
                        if temps[i] <= ts <= temps[i+1]:
                            t0,t1=temps[i],temps[i+1]; d0,d1=durs[i],durs[i+1]
                            out.append(d0+(ts-t0)/(t1-t0)*(d1-d0)); break
                    else:
                        out.append(durs[0] if ts<=temps[0] else durs[-1])
                return out

            ax.plot(tr, interp(tr), color=ACC_GREEN, lw=2.5, label="No penalty")
            shift = (ec/100)*pen
            ax.plot(tr, interp(tr, shift), color=ACC_RED, lw=2.5,
                    linestyle="--", label=f"With clouds {ec:.0f}%")
            ax.scatter([et], [base],     color=ACC_GREEN, s=90, zorder=5,
                       edgecolors="#fff", lw=2)
            ax.scatter([et], [with_pen], color=ACC_RED,   s=90, zorder=5,
                       edgecolors="#fff", lw=2, marker="s")
            if abs(diff) > 2:
                ax.annotate("", xy=(et, with_pen), xytext=(et, base),
                            arrowprops=dict(arrowstyle="<->",
                                            color=ACC_ORANGE, lw=2))
                ax.text(et+0.4, (base+with_pen)/2, f"{diff:+d}min",
                        color=ACC_ORANGE, fontweight="bold", fontsize=9,
                        bbox=dict(boxstyle="round,pad=0.3",
                                  facecolor=CARD_BG,
                                  edgecolor=ACC_ORANGE, lw=1.5))

        ax.scatter(temps, durs, color=ACC_BLUE, s=55, zorder=4,
                   edgecolors="#fff", lw=1, alpha=0.8)
        ax.set_xlabel("Temperature (°C)", color=TEXT_DIM, fontsize=9)
        ax.set_ylabel("Duration (min)",   color=TEXT_DIM, fontsize=9)
        ax.set_title("Boiler Duration vs Temperature",
                     color=TEXT_FG, fontweight="bold", fontsize=10)
        ax.tick_params(colors=TEXT_DIM, labelsize=8)
        ax.grid(True, linestyle=":", alpha=0.25, color=CARD_BDR)
        ax.legend(facecolor=CARD_BG, edgecolor=CARD_BDR,
                  labelcolor=TEXT_FG, fontsize=8)
        for sp in ax.spines.values():
            sp.set_edgecolor(CARD_BDR)

        canvas = FigureCanvasTkAgg(fig, master=self.graph_container)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        self.lut_graph_canvas = canvas

    # ─────────────────────────────────────────────────────────
    # SAVE
    # ─────────────────────────────────────────────────────────
    def _save_settings(self):
        try:
            h = self.target_hour_var.get().strip().zfill(2)
            m = self.target_min_var.get().strip().zfill(2)
            ts = f"{h}:{m}"
            if not self.runtime_settings.set_target_time(ts):
                messagebox.showerror("Invalid Time", "Must be HH:MM"); return
            try:
                pen = float(self.cloud_penalty_var.get())
                if not self.runtime_settings.set_cloud_penalty(pen):
                    messagebox.showerror("Invalid", "Penalty must be positive"); return
            except ValueError:
                messagebox.showerror("Invalid", "Penalty must be a number"); return
            if len(self.temp_lut) < 2:
                messagebox.showerror("Invalid LUT", "Need ≥ 2 points"); return
            if not self.runtime_settings.set_temp_lut(self.temp_lut):
                messagebox.showerror("Invalid LUT", "Validation failed"); return

            self.config.first_run_target_time        = ts
            self.config.cloud_penalty_factor          = pen
            self.config.temp_lut                      = self.temp_lut
            self.weather_service.temp_lut              = self.temp_lut
            self.weather_service.cloud_penalty_factor  = pen
            self.scheduler.update_target_time(ts)
            messagebox.showinfo("Saved",
                f"Target: {ts}  |  Penalty: {pen}  |  LUT: {len(self.temp_lut)} pts")
        except Exception as e:
            messagebox.showerror("Save Failed", str(e))

    # ─────────────────────────────────────────────────────────
    # CONTROL ACTIONS
    # ─────────────────────────────────────────────────────────
    def _save_exec_mode(self):
        mode = self._exec_mode_var.get()
        self.runtime_settings.set_execution_mode(mode)
        # MQTT execution mode implies broker must be active
        if mode == "MQTT" and not self._mqtt_active_var.get():
            self._mqtt_active_var.set(True)  # triggers _on_mqtt_active_toggle

    def _on_mqtt_active_toggle(self):
        active = self._mqtt_active_var.get()
        self.runtime_settings.set_mqtt_active(active)
        # Update label
        if hasattr(self, '_mqtt_active_lbl'):
            self._mqtt_active_lbl.config(
                text="on" if active else "off",
                fg=ACC_GREEN if active else TEXT_DIM)
        # Sync to mqtt_service
        if self.mqtt_service:
            self.mqtt_service.active = active
            if active and not self.mqtt_service.is_connected():
                import threading
                threading.Thread(
                    target=self.mqtt_service.connect, daemon=True).start()
            elif not active and self.mqtt_service.is_connected():
                self.mqtt_service.disconnect()

    def _save_solar_active(self):
        self.runtime_settings.set_solar_active(self._solar_active_var.get())

    def _save_2nd_run_time(self):
        h = self._2nd_hour_var.get().strip().zfill(2)
        m = self._2nd_min_var.get().strip().zfill(2)
        t = f"{h}:{m}"
        if self.runtime_settings.set_second_run_time(t):
            messagebox.showinfo("Saved", f"2nd run → {t}")
        else:
            messagebox.showerror("Invalid", "Time must be HH:MM")

    def _save_weekly_presets(self):
        presets = []
        for w in self._preset_widgets:
            h = w["h"].get().strip().zfill(2)
            m = w["m"].get().strip().zfill(2)
            try:
                dur = int(w["dur"].get().strip())
            except ValueError:
                messagebox.showerror("Invalid",
                    f"Preset {w['id']}: duration must be integer"); return
            presets.append({
                "id": w["id"], "active": w["active"].get(),
                "start_time": f"{h}:{m}", "duration": dur,
                "days": [d for d in DAYS if w["days"][d].get()]
            })
        self.runtime_settings.set_weekly_presets(presets)
        messagebox.showinfo("Saved", "Weekly presets saved")

    def _arm_one_shot(self):
        h = self._os_hour_var.get().strip().zfill(2)
        m = self._os_min_var.get().strip().zfill(2)
        try:
            dur = int(self._os_dur_var.get().strip())
            if dur <= 0: raise ValueError
        except ValueError:
            messagebox.showerror("Invalid", "Duration must be positive integer"); return
        self.runtime_settings.settings["one_shot"] = {
            "start_time": f"{h}:{m}", "duration": dur, "armed": True}
        self.runtime_settings.save()
        messagebox.showinfo("Armed", f"One-shot: {h}:{m}  {dur} min")

    def _refresh_status(self):
        mode = self.runtime_settings.get_execution_mode()
        if mode == "MQTT":
            s = (self.mqtt_service.get_status()
                 if self.mqtt_service else None) or "unknown"
            self._update_status(s)
        else:
            try:
                entity = self.config.config["homeassistant"].get(
                    "BOILER_ENTITY_ID",
                    "switch.water_heater_water_heater_timeoutsw")
                self._update_status(
                    self.ha_service.get_boiler_state(entity))
            except Exception:
                self._update_status("unknown")

    def _on_mqtt_power_status(self, status):
        self.parent.after(0, lambda: self._update_mqtt_device_indicator(status))

    def _update_status(self, status):
        s = status.upper()
        color = ACC_GREEN if s=="ON" else (ACC_RED if s=="OFF" else ACC_DIM)
        text  = s if s in ("ON","OFF") else status
        if self._status_dot: self._status_dot.config(fg=color)
        if self._status_lbl: self._status_lbl.config(text=text)

    def _save_topic_format(self):
        fmt = self._topic_fmt_var.get()
        if self.mqtt_service:
            self.mqtt_service.set_topic_format(fmt)
        try:
            self.config.config["mqtt"]["topic_format"] = fmt
            with open(self.config.config_file, "w") as f:
                self.config.config.write(f)
        except Exception as e:
            import logging; logging.getLogger(__name__).error(str(e))

    def _ensure_mqtt(self):
        if not self.mqtt_service:
            messagebox.showerror("MQTT", "Not configured — check config.ini [mqtt]")
            return False
        if not self.mqtt_service.is_connected():
            if not self.mqtt_service.connect():
                messagebox.showerror("MQTT",
                    f"Cannot connect to {self.config.mqtt_broker_ip}")
                return False
        return True

    def _mqtt_manual_on(self):
        if not self._ensure_mqtt(): return
        try:
            dur = int(self._mqtt_dur_var.get().strip())
            if dur <= 0: raise ValueError
        except ValueError:
            messagebox.showerror("Invalid", "Duration must be positive integer"); return
        if self.mqtt_service.turn_on_for(dur):
            self._update_status("ON")
        else:
            messagebox.showerror("MQTT", "Command failed")

    def _mqtt_manual_off(self):
        if not self._ensure_mqtt(): return
        if self.mqtt_service.turn_off():
            self._update_status("OFF")
        else:
            messagebox.showerror("MQTT", "Command failed")

    def _mqtt_send_cmd(self):
        if not self._ensure_mqtt(): return
        sub = self._cmd_subtopic_var.get().strip()
        pay = self._cmd_payload_var.get().strip()
        if not sub:
            messagebox.showerror("Invalid", "Subtopic cannot be empty"); return
        if not self.mqtt_service.send_cmd(sub, pay):
            messagebox.showerror("MQTT", "Command failed")

    def _on_tele_message(self, subtopic, payload):
        self.parent.after(0, lambda: self._append_tele(subtopic, payload))
        # LWT carries device online/offline status
        if subtopic.upper() == "LWT":
            self.parent.after(0, lambda: self._update_mqtt_device_indicator(payload))

    def _append_tele(self, subtopic, payload):
        ts = datetime.now().strftime("%H:%M:%S")
        self._tele_text.config(state="normal")
        self._tele_text.insert("end", f"[{ts}] {subtopic}: {payload}\n")
        self._tele_text.see("end")
        self._tele_text.config(state="disabled")

    def _refresh_tele(self):
        if not self.mqtt_service: return
        self._tele_text.config(state="normal")
        self._tele_text.delete("1.0", "end")
        for sub, pay in self.mqtt_service.get_tele_log():
            self._tele_text.insert("end", f"{sub}: {pay}\n")
        self._tele_text.see("end")
        self._tele_text.config(state="disabled")

    def _clear_tele(self):
        if self.mqtt_service: self.mqtt_service.clear_tele_log()
        self._tele_text.config(state="normal")
        self._tele_text.delete("1.0", "end")
        self._tele_text.config(state="disabled")

    # ─────────────────────────────────────────────────────────
    # MQTT CONNECTIVITY ACTIONS
    # ─────────────────────────────────────────────────────────
    def _mqtt_connect(self):
        if not self.mqtt_service:
            messagebox.showerror("MQTT", "Not configured — check config.ini [mqtt]")
            return
        if self.mqtt_service.is_connected():
            self._update_mqtt_broker_indicator(True)
            return
        ok = self.mqtt_service.connect()
        self._update_mqtt_broker_indicator(ok)
        if not ok:
            messagebox.showerror("MQTT", f"Cannot connect to broker\n"
                                 f"{getattr(self.config,'mqtt_broker_ip','?')}")

    def _mqtt_disconnect(self):
        if self.mqtt_service:
            self.mqtt_service.disconnect()
        self._update_mqtt_broker_indicator(False)
        self._update_mqtt_device_indicator("unknown")

    def _on_mqtt_connect_change(self, connected: bool):
        """Called from MQTT thread on connect/disconnect."""
        self.parent.after(0, lambda: self._update_mqtt_broker_indicator(connected))
        if not connected:
            self.parent.after(0, lambda: self._update_mqtt_device_indicator("unknown"))

    def _mqtt_refresh_conn(self):
        if not self.mqtt_service:
            self._update_mqtt_broker_indicator(False)
            self._update_mqtt_device_indicator("unknown")
            return
        broker_ok = self.mqtt_service.is_connected()
        self._update_mqtt_broker_indicator(broker_ok)
        if broker_ok:
            s = self.mqtt_service.get_status() or "unknown"
            self._update_mqtt_device_indicator(s)
        else:
            self._update_mqtt_device_indicator("unknown")

    def _update_mqtt_broker_indicator(self, connected: bool):
        if not hasattr(self, '_mqtt_broker_dot'): return
        if connected:
            self._mqtt_broker_dot.config(fg=ACC_GREEN)
            self._mqtt_broker_lbl.config(text="connected", fg=TEXT_FG)
        else:
            self._mqtt_broker_dot.config(fg=ACC_RED)
            self._mqtt_broker_lbl.config(text="disconnected", fg=TEXT_DIM)

    def _update_mqtt_device_indicator(self, status: str):
        if not hasattr(self, '_mqtt_device_dot'): return
        s = status.upper()
        if s == "ON":
            color, text = ACC_GREEN, "ON"
        elif s == "OFF":
            color, text = "#f59e0b", "OFF"   # amber — device is reachable but off
        elif s in ("ONLINE",):
            color, text = ACC_GREEN, "online"
        elif s in ("OFFLINE", "LWT"):
            color, text = ACC_RED, "offline"
        else:
            color, text = ACC_DIM, status
        self._mqtt_device_dot.config(fg=color)
        self._mqtt_device_lbl.config(text=text, fg=TEXT_FG if s in ("ON","OFF","ONLINE") else TEXT_DIM)
