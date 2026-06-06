"""
Main Window - Boiler Control GUI
Themed notebook tabs + status bar
"""

import tkinter as tk
from tkinter import ttk
from datetime import datetime
import threading

from config    import AppConfig
from gui.data_tab      import DataTab
from gui.log_tab       import LogTab
from gui.settings_tab  import SettingsTab
from gui.dashboard_tab import DashboardTab
from gui.config_tab    import ConfigTab
from gui.dialogs       import show_info, show_error
from gui.theme         import setup_notebook_style, CLR, BG, BG2, BG3, TEXT_FG, TEXT_DIM, SEP, ACC_BLUE


class BoilerApp:
    """Main application window"""
    
    def __init__(self, root, config, weather_service, ha_service, data_manager, scheduler, mqtt_service=None):
        self.root = root
        self.config = config
        self.weather_service = weather_service
        self.ha_service = ha_service
        self.dm = data_manager
        self.scheduler = scheduler
        self.mqtt_service = mqtt_service
        
        self.startup_time = datetime.now()
        self._state_var = tk.StringVar(value="💤 INITIALIZING")
        
        # Dark color scheme
        self._clr = {
            "BG":     "#1e1e1e",
            "BG2":    "#252526",
            "BG3":    "#3e3e42",
            "FG":     "#e0e0e0",
            "FG_DIM": "#a0a0a0",
            "SEP":    "#555555",
            "ACCENT": "#007acc"
        }
        
        self.root.title("Boiler Control System v2.4")
        self.root.geometry("1400x720")
        self.root.resizable(True, True)   # allow resize but start at right size
        self.root.configure(bg=self._clr["BG"])
        
        self._create_status_bar()   # pack bottom first — guaranteed space
        self._setup_themed_notebook()
        self._create_tabs()
        
        # Load initial data
        self.data_tab_widget.load_data()
        
        # Start clock
        self._tick()
        # One-shot connection check on startup
        self.root.after(2000, self._poll_connections)
    
    def _setup_themed_notebook(self):
        """Configure notebook style to match app theme"""
        style = ttk.Style()
        style.theme_use('default')
        
        # Configure notebook tabs to match dark theme
        style.configure(
            "TNotebook",
            background=self._clr["BG"],
            borderwidth=0
        )
        
        style.configure(
            "TNotebook.Tab",
            background=self._clr["BG2"],
            foreground=self._clr["FG"],
            padding=[15, 8],
            borderwidth=0,
            font=("Segoe UI", 10)
        )
        
        style.map(
            "TNotebook.Tab",
            background=[("selected", self._clr["BG3"])],
            foreground=[("selected", self._clr["FG"])],
            expand=[("selected", [1, 1, 1, 0])]
        )
        
        # Create notebook
        self.notebook = ttk.Notebook(self.root, style="TNotebook")
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)
    
    def _create_tabs(self):
        # Tab 1: Dashboard (operational)
        self.tab_control = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_control, text="  ⚡  Control  ")
        self.control_tab_widget = DashboardTab(
            self.tab_control,
            self.config,
            self.config.runtime_settings,
            self.scheduler,
            self.ha_service,
            self.weather_service,
            self.dm,
            self._clr,
            mqtt_service=self.mqtt_service,
            on_boiler_state=self._on_boiler_state
        )
        # Alias so fan-out callbacks still work
        self.control_tab_widget.dashboard = self.control_tab_widget

        # Tab 2: Log
        self.tab_log = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_log, text="  📝  Log  ")
        self.log_tab_widget = LogTab(self.tab_log, self._clr)

        # Tab 3: Daily Records
        self.tab_data = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_data, text="  📊  Records  ")
        self.data_tab_widget = DataTab(
            self.tab_data, self.dm, self.ha_service, self._clr)

        # Tab 4: Settings
        self.tab_settings = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_settings, text="  ⚙️  Settings  ")
        self.settings_tab_widget = SettingsTab(
            self.tab_settings,
            self.config,
            self.config.runtime_settings,
            self.scheduler,
            self.weather_service,
            self._clr,
            mqtt_service=self.mqtt_service,
            ha_service=self.ha_service
        )

        # Tab 5: Config editor
        self.tab_config = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_config, text="  🗄  Config  ")
        self.config_tab_widget = ConfigTab(
            self.tab_config,
            self.config,
            self._clr
        )

        # Wire all MQTT callbacks now that every tab widget exists
        self._wire_mqtt_callbacks()
    
    def _create_status_bar(self):
        """Create bottom status bar"""
        c = self._clr
        bar = tk.Frame(self.root, bg=c["BG2"])
        bar.pack(side="bottom", fill="x")
        self._status_bar   = bar
        self._bar_bg_labels = []   # Labels/Frames whose bg changes on vacation

        tk.Frame(bar, bg=c["SEP"], height=1).place(relx=0, rely=0, relwidth=1)

        def _sep():
            w = tk.Label(bar, text="│", bg=c["BG2"], fg=c["SEP"],
                         font=("Segoe UI", 11))
            self._bar_bg_labels.append(w)
            return w

        # ── Clock ────────────────────────────────────────────
        self.lbl_clock = tk.Label(bar, text="", bg=c["BG2"], fg=c["FG"],
                                  font=("Segoe UI", 10, "bold"), padx=12)
        self.lbl_clock.pack(side="left", pady=4)
        self._bar_bg_labels.append(self.lbl_clock)

        _sep().pack(side="left")

        self.lbl_uptime = tk.Label(bar, text="", bg=c["BG2"], fg=c["FG_DIM"],
                                   font=("Segoe UI", 10), padx=12)
        self.lbl_uptime.pack(side="left", pady=4)
        self._bar_bg_labels.append(self.lbl_uptime)

        _sep().pack(side="left")

        self.lbl_state = tk.Label(bar, textvariable=self._state_var,
                                  bg=c["BG2"], fg="#f0c060",
                                  font=("Segoe UI", 10, "bold"), padx=12)
        self.lbl_state.pack(side="left", pady=4)
        self._bar_bg_labels.append(self.lbl_state)

        _sep().pack(side="left")

        # ── Execution mode pill (HA/MQTT) — only shown when HA is configured ──
        if self.config.ha_configured:
            self._mode_btn = tk.Button(
                bar,
                text=f"  {self.config.runtime_settings.get_execution_mode()}  ",
                command=self._toggle_exec_mode,
                bg="#1e3a5f", fg="#93c5fd",
                font=("Segoe UI", 9, "bold"),
                relief="flat", padx=6, pady=3,
                cursor="hand2", bd=0,
                activebackground="#1e40af", activeforeground="#bfdbfe",
                highlightthickness=1, highlightbackground="#3b82f6"
            )
            self._mode_btn.pack(side="left", padx=(0, 6), pady=4)
            _sep().pack(side="left")
        else:
            self._mode_btn = None

        # ── HA indicator (hidden when HA not configured) ──────
        if self.config.ha_configured:
            self._ha_dot = tk.Label(bar, text="●", bg=c["BG2"], fg="#555555",
                                    font=("Segoe UI", 13), padx=4)
            self._ha_dot.pack(side="left", pady=4)
            self._bar_bg_labels.append(self._ha_dot)
            self._ha_lbl = tk.Label(bar, text="HA", bg=c["BG2"], fg=c["FG_DIM"],
                                    font=("Segoe UI", 9), padx=2)
            self._ha_lbl.pack(side="left", pady=4)
            self._bar_bg_labels.append(self._ha_lbl)
            _sep().pack(side="left")
        else:
            self._ha_dot = None
            self._ha_lbl = None

        # ── MQTT indicator ───────────────────────────────────
        self._mqtt_dot = tk.Label(bar, text="●", bg=c["BG2"], fg="#555555",
                                  font=("Segoe UI", 13), padx=4)
        self._mqtt_dot.pack(side="left", pady=4)
        self._bar_bg_labels.append(self._mqtt_dot)
        self._mqtt_lbl = tk.Label(bar, text="MQTT", bg=c["BG2"], fg=c["FG_DIM"],
                                  font=("Segoe UI", 9), padx=2)
        self._mqtt_lbl.pack(side="left", pady=4)
        self._bar_bg_labels.append(self._mqtt_lbl)

        _sep().pack(side="left")

        # ── Boiler indicator ─────────────────────────────────
        self._boiler_dot = tk.Label(bar, text="●", bg=c["BG2"], fg="#555555",
                                    font=("Segoe UI", 13), padx=4)
        self._boiler_dot.pack(side="left", pady=4)
        self._bar_bg_labels.append(self._boiler_dot)
        self._boiler_lbl = tk.Label(bar, text="Boiler", bg=c["BG2"], fg=c["FG_DIM"],
                                    font=("Segoe UI", 9), padx=2)
        self._boiler_lbl.pack(side="left", pady=4)
        self._bar_bg_labels.append(self._boiler_lbl)

        # ── Right side ───────────────────────────────────────
        btn_style = {
            "bg": c["BG3"], "fg": c["FG"],
            "activebackground": "#4e5254", "activeforeground": "#ffffff",
            "relief": "flat", "font": ("Segoe UI", 10),
            "padx": 14, "pady": 5, "cursor": "hand2", "bd": 0
        }

        # Designer credit
        tk.Label(bar, text="Designed by Guy Dvir",
                 bg=c["BG2"], fg=c["FG_DIM"],
                 font=("Segoe UI", 8, "italic"), padx=15
                 ).pack(side="right")

        # Vacation toggle
        self._vacation_btn = tk.Button(
            bar, text="⏸  Vacation",
            command=self._toggle_vacation,
            **btn_style
        )
        self._vacation_btn.pack(side="right", padx=(4, 8), pady=4)

        # Retry HA (hidden when HA not configured)
        if self.config.ha_configured:
            self._retry_btn = tk.Button(
                bar, text="⚡  Retry HA (today)",
                command=self._retry_ha, **btn_style
            )
            self._retry_btn.pack(side="right", padx=(4, 4), pady=4)
        else:
            self._retry_btn = None

        # Apply vacation visual on startup if already set
        if self.config.runtime_settings.get_vacation_mode():
            self.root.after(200, lambda: self._set_vacation_visual(True))
    
    def _retry_ha(self):
        """Retry HA button handler"""
        def _done(ok, status):
            self._set_retry_btn_state("normal")
            if ok:
                show_info(self.root, "Retry HA", f"HA updated successfully.\nStatus: {status}")
            else:
                show_error(self.root, "Retry HA", f"HA call failed.\nStatus: {status}\nCheck boiler.log.")
            self._refresh_data()

        self._set_retry_btn_state("disabled")
        self.data_tab_widget.retry_ha(_done)

    def _refresh_data(self):
        """Refresh data table"""
        self.data_tab_widget.load_data()

    def _set_retry_btn_state(self, state):
        """Enable/disable retry button"""
        if self._retry_btn:
            self._retry_btn.config(state=state)

    # ── Vacation mode ─────────────────────────────────────────

    def _toggle_vacation(self):
        rs = self.config.runtime_settings
        new_state = not rs.get_vacation_mode()
        rs.set_vacation_mode(new_state)
        self._set_vacation_visual(new_state)
        if new_state:
            self._state_var.set("⏸  VACATION")
        if self.mqtt_service and self.mqtt_service.is_connected():
            self.mqtt_service.publish_state("vacation", "ON" if new_state else "OFF")

    def _set_vacation_visual(self, on: bool):
        AMBER   = "#92400e"
        NORMAL  = self._clr["BG2"]
        bg = AMBER if on else NORMAL
        self._status_bar.config(bg=bg)
        for w in self._bar_bg_labels:
            try:
                w.config(bg=bg)
            except Exception:
                pass
        self._vacation_btn.config(
            text="▶  Resume" if on else "⏸  Vacation",
            bg="#b45309" if on else self._clr["BG3"],
            fg="#fef3c7" if on else self._clr["FG"]
        )

    # ── Execution mode pill ───────────────────────────────────

    def _toggle_exec_mode(self):
        rs = self.config.runtime_settings
        current  = rs.get_execution_mode()
        new_mode = "MQTT" if current == "HA" else "HA"
        rs.set_execution_mode(new_mode)
        if self._mode_btn:
            self._mode_btn.config(text=f"  {new_mode}  ")
        # Switching to MQTT: ensure service connects if active
        if new_mode == "MQTT" and self.mqtt_service:
            rs.set_mqtt_active(True)
            if not self.mqtt_service.is_connected():
                import threading
                threading.Thread(target=self.mqtt_service.connect,
                                 daemon=True).start()
        if self.mqtt_service and self.mqtt_service.is_connected():
            self.mqtt_service.publish_state("mode", new_mode)
    
    def on_scheduler_state(self, state):
        """Called from Scheduler thread"""
        state_icons = {
            "WAIT_FIRST_CHECK": "💤  WAIT CHECK",
            "FETCH_SUN":        "🌅  FETCH SUN",
            "CALCULATING":      "🧮  CALCULATING",
            "WAIT_TRIGGER":     "⏳  WAIT TRIGGER",
            "EXECUTE":          "⚡  EXECUTING",
            "WAIT_NEXT_DAY":    "😴  DONE TODAY",
        }
        self.root.after(0, lambda: self._state_var.set(state_icons.get(state, state)))
        # Auto-refresh after execution
        if state == "WAIT_NEXT_DAY":
            self.root.after(500, self._refresh_data)
        # Publish solar state
        if self.mqtt_service and self.mqtt_service.is_connected():
            self.mqtt_service.publish_state(
                "solar", BoilerApp._solar_state_for_mqtt(state))
    
    def _on_boiler_state(self, status: str):
        """Called from ControlTab when boiler status changes."""
        self.root.after(0, lambda: self._update_boiler_dot(status))

    def _update_boiler_dot(self, status: str):
        GREEN, RED, DIM = "#22c55e", "#ef4444", "#555555"
        s = status.upper()
        if s == "ON":
            self._boiler_dot.config(fg=GREEN)
            self._boiler_lbl.config(fg="#e2e8f0", text="Boiler  ON")
        elif s == "OFF":
            self._boiler_dot.config(fg=RED)
            self._boiler_lbl.config(fg="#94a3b8", text="Boiler  OFF")
        else:
            self._boiler_dot.config(fg=DIM)
            self._boiler_lbl.config(fg="#94a3b8", text="Boiler  —")


    def _wire_mqtt_callbacks(self):
        """Register MQTT fan-out callbacks. Called once after all tabs are built.
        Single source of truth — settings_tab and control_tab must NOT register
        their own on_status_change / on_connect_change / on_tele callbacks."""
        if not self.mqtt_service:
            return

        def _on_status(status: str):
            """Tasmota POWER response → update every boiler indicator."""
            # Status bar dot
            self.root.after(0, lambda: self._on_boiler_state(status))
            # Control tab boiler dot
            if hasattr(self, 'control_tab_widget'):
                self.root.after(0, lambda s=status:
                    self.control_tab_widget._set_boiler_status(s))
            # Settings tab MQTT device dot
            if hasattr(self, 'settings_tab_widget'):
                self.root.after(0, lambda s=status:
                    self.settings_tab_widget._update_mqtt_device_indicator(s))
            # Publish boiler state (ON/OFF only — skip unknown/transient)
            s = status.upper()
            if s in ("ON", "OFF"):
                self.mqtt_service.publish_state("boiler", s)

        def _on_connect(connected: bool):
            """Broker connect/disconnect → update broker indicators + clear boiler on disc."""
            # Status bar MQTT dot
            self.root.after(0, lambda: self.notify_mqtt_state(connected))
            # Settings tab broker dot
            if hasattr(self, 'settings_tab_widget'):
                self.root.after(0, lambda c=connected:
                    self.settings_tab_widget._update_mqtt_broker_indicator(c))
            if connected:
                # Publish full state immediately, then start heartbeat
                import threading
                threading.Thread(target=self._publish_full_snapshot,
                                 daemon=True).start()
                self.mqtt_service.start_heartbeat(300, self._build_state_snapshot)
            else:
                self.mqtt_service.stop_heartbeat()
                _on_status("unknown")

        def _on_tele(subtopic, payload):
            """Telemetry forwarded to settings tab only."""
            if hasattr(self, 'settings_tab_widget'):
                self.settings_tab_widget._on_tele_message(subtopic, payload)

        def _on_command(cmd_type: str, payload: str):
            """Inbound command from Telegram/MQTT → validate then execute."""
            import threading, re
            log = __import__('logging').getLogger(__name__)

            def _dashboard():
                if hasattr(self, 'control_tab_widget') and                    self.control_tab_widget.dashboard:
                    return self.control_tab_widget.dashboard
                return None

            try:
                if cmd_type == "adhoc":
                    dur = int(payload.strip())
                    if dur <= 0 or dur > 300:
                        raise ValueError(f"Duration out of range: {dur}")
                    log.info(f"MQTT cmd: adhoc {dur} min")
                    threading.Thread(
                        target=lambda: self.scheduler.manual_run(dur),
                        daemon=True).start()

                elif cmd_type == "oneshot":
                    parts    = [p.strip() for p in payload.strip().split(",")]
                    if len(parts) != 2:
                        raise ValueError(f"Expected 'HH:MM,minutes' got: {payload}")
                    time_str = parts[0]
                    if not re.match(r"^\d{2}:\d{2}$", time_str):
                        raise ValueError(f"Invalid time format: {time_str}")
                    dur = int(parts[1])
                    if dur <= 0 or dur > 300:
                        raise ValueError(f"Duration out of range: {dur}")
                    log.info(f"MQTT cmd: oneshot {time_str} {dur} min")
                    self.config.runtime_settings.set_one_shot(
                        time_str, dur, armed=True)
                    d = _dashboard()
                    if d: self.root.after(0, d._refresh)

                elif cmd_type == "weekly":
                    # format: "<id>,<HH:MM>,<dur>,<day1>,<day2>..."
                    parts = [p.strip() for p in payload.strip().split(",")]
                    if len(parts) < 3:
                        raise ValueError(f"Expected id,HH:MM,dur[,days] got: {payload}")
                    pid   = int(parts[0])
                    t_str = parts[1]
                    if not re.match(r"^\d{2}:\d{2}$", t_str):
                        raise ValueError(f"Invalid time format: {t_str}")
                    dur  = int(parts[2])
                    if dur <= 0 or dur > 300:
                        raise ValueError(f"Duration out of range: {dur}")
                    valid_days = {"Sun","Mon","Tue","Wed","Thu","Fri","Sat"}
                    days = [d for d in parts[3:] if d in valid_days]
                    presets = self.config.runtime_settings.get_weekly_presets()
                    matched = False
                    for p in presets:
                        if p["id"] == pid:
                            p["start_time"] = t_str
                            p["duration"]   = dur
                            p["days"]       = days
                            p["active"]     = True
                            matched         = True
                    if not matched:
                        raise ValueError(f"No preset with id={pid}")
                    self.config.runtime_settings.set_weekly_presets(presets)
                    log.info(f"MQTT cmd: weekly preset {pid} updated → {t_str} {dur}min {days}")
                    d = _dashboard()
                    if d: self.root.after(0, d._refresh_weekly)

                else:
                    log.warning(f"MQTT on_command: unknown type '{cmd_type}'")

            except (ValueError, IndexError) as e:
                log.error(f"MQTT cmd rejected ({cmd_type}={payload!r}): {e}")
            except Exception as e:
                log.error(f"MQTT on_command unexpected error: {e}", exc_info=True)

        self.mqtt_service.on_status_change  = _on_status
        self.mqtt_service.on_connect_change = _on_connect
        self.mqtt_service.on_tele           = _on_tele
        self.mqtt_service.on_command        = _on_command

    # ── MQTT state publishing ────────────────────────────────────

    @staticmethod
    def _solar_state_for_mqtt(state: str) -> str:
        return {
            "SOLAR_INACTIVE":   "INACTIVE",
            "VACATION":         "VACATION",
            "WAIT_FIRST_CHECK": "SCHEDULED",
            "FETCH_SUN":        "SCHEDULED",
            "CALCULATING":      "SCHEDULED",
            "WAIT_TRIGGER":     "SCHEDULED",
            "EXECUTE":          "RUNNING",
            "WAIT_NEXT_DAY":    "FIRED",
            "ERROR":            "ERROR",
        }.get(state, state)

    def _build_state_snapshot(self) -> dict:
        """Return full app state as {suffix: payload} for heartbeat / on-connect publish."""
        import json
        rs = self.config.runtime_settings
        snap = {}
        snap["vacation"] = "ON" if rs.get_vacation_mode() else "OFF"
        snap["mode"]     = rs.get_execution_mode()
        snap["solar"]    = self._solar_state_for_mqtt(
            getattr(self.scheduler, "_current_state", "UNKNOWN"))
        # Boiler: last known from MQTT stat
        last = (self.mqtt_service.get_last_status() or "UNKNOWN") if self.mqtt_service else "UNKNOWN"
        snap["boiler"] = last.upper()
        # One-shot
        os_cfg = rs.get_one_shot()
        snap["oneshot"] = json.dumps({
            "armed":    os_cfg.get("armed", False),
            "time":     os_cfg.get("start_time", ""),
            "duration": os_cfg.get("duration", 0),
        }, separators=(",", ":"))
        # Weekly presets
        for p in rs.get_weekly_presets():
            pid = p.get("id", "?")
            snap[f"weekly/{pid}"] = json.dumps({
                "active":   p.get("active", False),
                "time":     p.get("start_time", ""),
                "duration": p.get("duration", 0),
                "days":     p.get("days", []),
            }, separators=(",", ":"))
        return snap

    def _publish_full_snapshot(self):
        """Publish every state topic. Called in a background thread on connect."""
        if not self.mqtt_service:
            return
        for suffix, payload in self._build_state_snapshot().items():
            self.mqtt_service.publish_state(suffix, payload)

    def _tick(self):
        """Update clock and uptime"""
        now = datetime.now()
        elapsed = now - self.startup_time
        h, rem = divmod(int(elapsed.total_seconds()), 3600)
        m, s = divmod(rem, 60)
        self.lbl_clock.config(text=f"🕐  {now.strftime('%H:%M:%S')}")
        self.lbl_uptime.config(text=f"Uptime: {h:02d}:{m:02d}:{s:02d}")
        self.root.after(1000, self._tick)

    def _poll_connections(self):
        """Startup check: ping HA, connect MQTT if active, update all indicators.
        Also wire MQTT callbacks so all three consumers stay in sync:
          • status bar dots (main_window)
          • Control tab boiler dot
          • Settings tab MQTT dots
        """
        import threading

        def _check():
            # HA reachability
            try:
                self.ha_service.check_reachable()
                ha_ok = True
            except Exception:
                ha_ok = False
            self.root.after(0, lambda: self.notify_ha_state(ha_ok))

            # MQTT — connect if active flag is set
            if self.mqtt_service:
                mqtt_active = getattr(self.mqtt_service, 'active', False)
                if mqtt_active and not self.mqtt_service.is_connected():
                    self.mqtt_service.connect()   # triggers _on_connect callback
                else:
                    connected = self.mqtt_service.is_connected()
                    self.root.after(0, lambda: self.notify_mqtt_state(connected))
                    if hasattr(self, 'settings_tab_widget'):
                        self.settings_tab_widget.notify_broker_state(connected)

        threading.Thread(target=_check, daemon=True).start()

    def notify_ha_state(self, ok: bool):
        """Call this whenever an HA operation succeeds or fails."""
        if self._ha_dot is None:
            return
        GREEN, RED = "#22c55e", "#ef4444"
        self._ha_dot.config(fg=GREEN if ok else RED)
        self._ha_lbl.config(fg="#e2e8f0" if ok else "#94a3b8")
        if hasattr(self, 'control_tab_widget') and            self.control_tab_widget.dashboard:
            self.control_tab_widget.dashboard.set_ha_status(ok)

    def notify_mqtt_state(self, connected: bool):
        """Call this when MQTT connects or disconnects."""
        GREEN, RED, DIM = "#22c55e", "#ef4444", "#555555"
        self._mqtt_dot.config(fg=GREEN if connected else (RED if self.mqtt_service else DIM))
        self._mqtt_lbl.config(fg="#e2e8f0" if connected else "#94a3b8")
        if hasattr(self, 'control_tab_widget') and            self.control_tab_widget.dashboard:
            self.control_tab_widget.dashboard.set_mqtt_status(connected)

    def _update_conn_indicators(self, ha_ok: bool, mqtt_ok: bool):
        GREEN, RED, DIM = "#22c55e", "#ef4444", "#555555"
        self._ha_dot.config(fg=GREEN if ha_ok else RED)
        self._ha_lbl.config(fg="#e2e8f0" if ha_ok else "#94a3b8")
        self._mqtt_dot.config(fg=GREEN if mqtt_ok else (RED if self.mqtt_service else DIM))
        self._mqtt_lbl.config(fg="#e2e8f0" if mqtt_ok else "#94a3b8")
