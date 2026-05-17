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

        # Weekly schedule moved to Dashboard middle column


    # ── Public status updaters (called by main_window fan-out) ──

    def _on_mqtt_power_status(self, status: str):
        if self.dashboard:
            self.dashboard.set_boiler_status(status)

    def _set_boiler_status(self, status: str):
        if self.dashboard:
            self.dashboard.set_boiler_status(status)
