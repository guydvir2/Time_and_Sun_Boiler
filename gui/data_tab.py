"""
Data Tab - Daily Records Table
Shows daily boiler execution history
"""

import tkinter as tk
from tkinter import ttk, messagebox
import threading
from datetime import datetime


class DataTab:
    """Daily records table tab"""
    
    def __init__(self, parent, data_manager, ha_service, colors):
        self.parent = parent
        self.dm = data_manager
        self.ha_service = ha_service
        self.clr = colors
        
        self.tree = None
        self._retry_btn = None
        
        self._create_ui()
    
    def _create_ui(self):
        """Create data table UI"""
        # Treeview
        cols = (
            "#", "Date", "Dawn", "Dusk", "Avg Temp", "Avg Cloud", "Eff Temp",
            "Duration", "1st Run", "2nd Run", "Trigger", "HA Status"
        )
        
        self.tree = ttk.Treeview(self.parent, columns=cols, show="headings", height=20)
        
        widths = [40, 90, 60, 60, 80, 80, 80, 80, 70, 70, 80, 90]
        for col, w in zip(cols, widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=w, anchor="center")
        
        # Scrollbar
        vsb = ttk.Scrollbar(self.parent, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        
        self.tree.pack(side="left", fill="both", expand=True, padx=5, pady=5)
        vsb.pack(side="right", fill="y", pady=5)
        
        # Row colors
        self.tree.tag_configure("ha_ok_odd",  background="#1e2130", foreground="#e2e8f0")
        self.tree.tag_configure("ha_ok_even", background="#252b3b", foreground="#e2e8f0")
        self.tree.tag_configure("ha_failed",  background="#3a1e1e", foreground="#f5a0a0")
        self.tree.tag_configure("ha_unknown", background="#2a2a2e", foreground="#aaaaaa")
    
    def load_data(self, force: bool = False):
        """Load data into table — skips re-render if CSV unchanged since last load."""
        csv_path = self.dm.DAILY_LOG_FILE
        try:
            import os
            mtime = os.path.getmtime(csv_path) if os.path.exists(csv_path) else 0
            if not force and hasattr(self, "_last_csv_mtime") and mtime == self._last_csv_mtime:
                return
            self._last_csv_mtime = mtime
        except Exception:
            pass

        self.dm.invalidate()
        self.tree.delete(*self.tree.get_children())
        
        for i, row in enumerate(self.dm.all_records(), 1):
            status = row.get("ha_status", "—")
            if   status == "FAILED": tag = "ha_failed"
            elif status == "—":      tag = "ha_unknown"
            elif i % 2 == 0:         tag = "ha_ok_even"
            else:                    tag = "ha_ok_odd"
            
            self.tree.insert("", "end", values=(
                i,
                row.get("date", ""),
                row.get("dawn", ""),
                row.get("dusk", ""),
                row.get("avg_temp", ""),
                row.get("avg_cloud", ""),
                row.get("effective_temp", ""),
                row.get("duration", ""),
                row.get("first_run", ""),
                row.get("second_run", ""),
                row.get("trigger_time", ""),
                status
            ), tags=(tag,))
    
    def retry_ha(self, callback):
        """Retry HA in background thread"""
        def _run():
            ok, status = self.dm.retry_ha(self.ha_service)
            callback(ok, status)
        
        threading.Thread(target=_run, daemon=True).start()
