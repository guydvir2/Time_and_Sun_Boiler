"""
Main Window - Boiler Control GUI
Themed notebook tabs + status bar
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import threading

from gui.data_tab import DataTab
from gui.log_tab import LogTab
from gui.settings_tab import SettingsTab


class BoilerApp:
    """Main application window"""
    
    def __init__(self, root, config, weather_service, ha_service, data_manager, scheduler):
        self.root = root
        self.config = config
        self.weather_service = weather_service
        self.ha_service = ha_service
        self.dm = data_manager
        self.scheduler = scheduler
        
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
        
        self.root.title("Boiler Control System v2.1")
        self.root.geometry("1400x800")  # Larger window for better visibility
        self.root.configure(bg=self._clr["BG"])
        
        self._setup_themed_notebook()
        self._create_tabs()
        self._create_status_bar()
        
        # Load initial data
        self.data_tab_widget.load_data()
        
        # Start clock
        self._tick()
    
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
        """Create all tabs"""
        # Tab 1: Data table
        self.tab_data = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_data, text="  📊  Daily Records  ")
        
        self.data_tab_widget = DataTab(
            self.tab_data,
            self.dm,
            self.ha_service,
            self._clr
        )
        
        # Tab 2: Settings
        self.tab_settings = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_settings, text="  ⚙️  Settings  ")
        
        self.settings_tab_widget = SettingsTab(
            self.tab_settings,
            self.config,
            self.config.runtime_settings,
            self.scheduler,
            self.weather_service,
            self._clr
        )
        
        # Tab 3: Log viewer
        self.tab_log = tk.Frame(self.notebook, bg=self._clr["BG"])
        self.notebook.add(self.tab_log, text="  📝  Log  ")
        
        self.log_tab_widget = LogTab(
            self.tab_log,
            self._clr
        )
    
    def _create_status_bar(self):
        """Create bottom status bar"""
        c = self._clr
        bar = tk.Frame(self.root, bg=c["BG2"], height=36)
        bar.pack(side="bottom", fill="x")
        bar.pack_propagate(False)
        
        tk.Frame(bar, bg=c["SEP"], height=1).place(relx=0, rely=0, relwidth=1)
        
        self.lbl_clock = tk.Label(
            bar, text="", bg=c["BG2"], fg=c["FG"],
            font=("Segoe UI", 10, "bold"), padx=12
        )
        self.lbl_clock.pack(side="left", pady=4)
        
        tk.Label(bar, text="│", bg=c["BG2"], fg=c["SEP"],
                 font=("Segoe UI", 11)).pack(side="left")
        
        self.lbl_uptime = tk.Label(
            bar, text="", bg=c["BG2"], fg=c["FG_DIM"],
            font=("Segoe UI", 10), padx=12
        )
        self.lbl_uptime.pack(side="left", pady=4)
        
        tk.Label(bar, text="│", bg=c["BG2"], fg=c["SEP"],
                 font=("Segoe UI", 11)).pack(side="left")
        
        self.lbl_state = tk.Label(
            bar, textvariable=self._state_var,
            bg=c["BG2"], fg="#f0c060",
            font=("Segoe UI", 10, "bold"), padx=12
        )
        self.lbl_state.pack(side="left", pady=4)
        
        btn_style = {
            "bg": c["BG3"], "fg": c["FG"],
            "activebackground": "#4e5254", "activeforeground": "#ffffff",
            "relief": "flat", "font": ("Segoe UI", 10),
            "padx": 14, "pady": 5, "cursor": "hand2", "bd": 0
        }
        
        self._retry_btn = tk.Button(
            bar, text="⚡  Retry HA (today)",
            command=self._retry_ha, **btn_style
        )
        self._retry_btn.pack(side="right", padx=(4, 12), pady=4)
        
        tk.Button(
            bar, text="⟳  Refresh",
            command=self._refresh_data, **btn_style
        ).pack(side="right", padx=4, pady=4)
        
        # Designer credit
        tk.Label(
            bar, text="Designed by Guy Dvir",
            bg=c["BG2"], fg=c["FG_DIM"],
            font=("Segoe UI", 8, "italic"), padx=15
        ).pack(side="right")
    
    def _retry_ha(self):
        """Retry HA button handler"""
        def _done(ok, status):
            self._set_retry_btn_state("normal")
            if ok:
                messagebox.showinfo("Retry HA", f"HA updated successfully.\nStatus: {status}")
            else:
                messagebox.showerror("Retry HA", f"HA call failed.\nStatus: {status}\nCheck boiler.log.")
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
    
    def _tick(self):
        """Update clock and uptime"""
        now = datetime.now()
        elapsed = now - self.startup_time
        h, rem = divmod(int(elapsed.total_seconds()), 3600)
        m, s = divmod(rem, 60)
        self.lbl_clock.config(text=f"🕐  {now.strftime('%H:%M:%S')}")
        self.lbl_uptime.config(text=f"Uptime: {h:02d}:{m:02d}:{s:02d}")
        self.root.after(1000, self._tick)
