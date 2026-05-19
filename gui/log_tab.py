"""
Log Tab - Real-time log viewer
Displays boiler.log with auto-refresh
"""

import tkinter as tk
from tkinter import ttk
import os


class LogTab:
    """Log file viewer tab"""
    
    def __init__(self, parent, colors, log_file=None):
        self.parent = parent
        self.clr = colors
        
        if log_file is None:
            # Use data directory
            try:
                from config import DataDirectoryManager
                log_file = DataDirectoryManager.get_log_path()
            except ImportError:
                log_file = "boiler.log"  # Fallback
        
        self.log_file = log_file
        
        self.text_widget = None
        self.auto_scroll_var = None
        self.refresh_job = None
        
        self._create_ui()
        self._start_auto_refresh()
    
    def _create_ui(self):
        """Create log viewer UI"""
        # Top controls
        top_frame = tk.Frame(self.parent, bg=self.clr["BG2"], height=40)
        top_frame.pack(side="top", fill="x")
        top_frame.pack_propagate(False)
        
        tk.Label(
            top_frame, text="📝 Boiler Log Viewer",
            bg=self.clr["BG2"], fg=self.clr["FG"],
            font=("Segoe UI", 10, "bold"), padx=10
        ).pack(side="left", pady=8)
        
        # Auto-scroll checkbox
        self.auto_scroll_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            top_frame, text="Auto-scroll",
            variable=self.auto_scroll_var,
            bg=self.clr["BG2"], fg=self.clr["FG"],
            selectcolor=self.clr["BG3"],
            font=("Segoe UI", 9)
        ).pack(side="left", padx=10)
        
        # Refresh button
        tk.Button(
            top_frame, text="⟳ Refresh",
            command=self._load_log,
            bg=self.clr["BG3"], fg=self.clr["FG"],
            font=("Segoe UI", 9), relief="flat",
            padx=10, pady=4, cursor="hand2"
        ).pack(side="left", padx=5)
        
        # Clear button
        tk.Button(
            top_frame, text="🗑️ Clear Display",
            command=self._clear_display,
            bg=self.clr["BG3"], fg=self.clr["FG"],
            font=("Segoe UI", 9), relief="flat",
            padx=10, pady=4, cursor="hand2"
        ).pack(side="left", padx=5)
        
        # Text widget with scrollbar
        text_frame = tk.Frame(self.parent, bg=self.clr["BG"])
        text_frame.pack(side="bottom", fill="both", expand=True, padx=5, pady=5)
        
        self.text_widget = tk.Text(
            text_frame,
            bg=self.clr["BG2"], fg=self.clr["FG"],
            font=("Consolas", 9),
            wrap="none",
            state="disabled"
        )
        
        vsb = ttk.Scrollbar(text_frame, orient="vertical", command=self.text_widget.yview)
        hsb = ttk.Scrollbar(text_frame, orient="horizontal", command=self.text_widget.xview)
        
        self.text_widget.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.text_widget.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        text_frame.grid_rowconfigure(0, weight=1)
        text_frame.grid_columnconfigure(0, weight=1)
        
        # Color tags for log levels
        self.text_widget.tag_configure("ERROR", foreground="#ff6b6b")
        self.text_widget.tag_configure("WARNING", foreground="#ffaa00")
        self.text_widget.tag_configure("INFO", foreground="#a0e8a0")
        self.text_widget.tag_configure("DEBUG", foreground="#a0a0a0")
    
    def _load_log(self):
        """Load log file content"""
        if not os.path.exists(self.log_file):
            self._update_text("Log file not found: " + self.log_file)
            return
        
        try:
            with open(self.log_file, 'r') as f:
                lines = f.readlines()
            
            # Keep last 500 lines to avoid memory issues
            lines = lines[-500:]
            
            self._update_text("".join(lines))
            
            # Apply syntax highlighting
            self._highlight_log_levels()
            
            # Auto-scroll to bottom
            if self.auto_scroll_var.get():
                self.text_widget.see("end")
                
        except Exception as e:
            self._update_text(f"Error reading log: {e}")
    
    def _update_text(self, content):
        """Update text widget content"""
        self.text_widget.configure(state="normal")
        self.text_widget.delete("1.0", "end")
        self.text_widget.insert("1.0", content)
        self.text_widget.configure(state="disabled")
    
    def _highlight_log_levels(self):
        """Apply color highlighting to log levels"""
        self.text_widget.configure(state="normal")
        
        for level, tag in [("ERROR", "ERROR"), ("WARNING", "WARNING"), 
                          ("INFO", "INFO"), ("DEBUG", "DEBUG")]:
            start = "1.0"
            while True:
                pos = self.text_widget.search(f"[{level}]", start, stopindex="end")
                if not pos:
                    break
                end = f"{pos}+{len(level)+2}c"
                self.text_widget.tag_add(tag, pos, end)
                start = end
        
        self.text_widget.configure(state="disabled")
    
    def _clear_display(self):
        """Clear display (doesn't delete log file)"""
        self._update_text("")
    
    def _start_auto_refresh(self):
        """Start auto-refresh timer (every 5 seconds)"""
        self._load_log()
        self.refresh_job = self.parent.after(5000, self._start_auto_refresh)
    
    def stop_auto_refresh(self):
        """Stop auto-refresh (called on tab close)"""
        if self.refresh_job:
            self.parent.after_cancel(self.refresh_job)
