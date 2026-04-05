"""
Settings Tab - Two Column Layout
Left: Narrow LUT table with buttons
Right: Wide graph area with controls
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


class SettingsTab:
    """Settings tab with 2-column layout"""
    
    def __init__(self, parent, config, runtime_settings, scheduler, weather_service, colors):
        self.parent = parent
        self.config = config
        self.runtime_settings = runtime_settings
        self.scheduler = scheduler
        self.weather_service = weather_service
        self.clr = colors
        
        # UI elements
        self.lut_tree = None
        self.target_hour_var = None
        self.target_min_var = None
        self.cloud_penalty_var = None
        self.example_temp_var = None
        self.example_cloud_var = None
        self.lbl_calc_result = None
        self.graph_container = None
        self.lut_graph_canvas = None
        
        # Get current LUT and settings
        self.temp_lut = self.weather_service.temp_lut
        
        self._create_ui()
    
    def _create_ui(self):
        """Create 2-column layout"""
        # Main container
        main_frame = tk.Frame(self.parent, bg=self.clr["BG"])
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # ===== LEFT COLUMN: LUT Table (Narrow) =====
        left_frame = tk.Frame(main_frame, bg=self.clr["BG"], width=220)
        left_frame.pack(side="left", fill="y", padx=(0, 10))
        left_frame.pack_propagate(False)
        
        tk.Label(
            left_frame, text="Temperature LUT",
            bg=self.clr["BG"], fg=self.clr["FG"],
            font=("Segoe UI", 10, "bold")
        ).pack(anchor="w", pady=(0, 5))
        
        # LUT Treeview (10 rows)
        lut_cols = ("Temp (°C)", "Duration (min)")
        self.lut_tree = ttk.Treeview(
            left_frame, columns=lut_cols, show="headings", height=10
        )
        
        self.lut_tree.heading("Temp (°C)", text="Temp (°C)")
        self.lut_tree.heading("Duration (min)", text="Duration (min)")
        self.lut_tree.column("Temp (°C)", width=70, anchor="center")
        # self.lut_tree.column("Duration (min)", text="Duration (min)")
        self.lut_tree.heading("Duration (min)", text="Duration (min)")
        self.lut_tree.column("Duration (min)", width=90, anchor="center")
        
        lut_scroll = ttk.Scrollbar(left_frame, orient="vertical", command=self.lut_tree.yview)
        self.lut_tree.configure(yscrollcommand=lut_scroll.set)
        
        self.lut_tree.pack(side="left", fill="both", expand=True)
        lut_scroll.pack(side="right", fill="y")
        
        # Load LUT data
        self._load_lut_to_tree()
        
        # Buttons (vertical stack)
        btn_frame = tk.Frame(left_frame, bg=self.clr["BG"])
        btn_frame.pack(fill="x", pady=(10, 0))
        
        btn_style = {
            "bg": self.clr["BG3"], "fg": self.clr["FG"],
            "font": ("Segoe UI", 9), "relief": "flat",
            "padx": 8, "pady": 5, "cursor": "hand2"
        }
        
        tk.Button(btn_frame, text="➕ Add Row", command=self._add_lut_row, **btn_style).pack(fill="x", pady=2)
        tk.Button(btn_frame, text="✏️ Edit Selected", command=self._edit_lut_row, **btn_style).pack(fill="x", pady=2)
        tk.Button(btn_frame, text="🗑️ Delete Selected", command=self._delete_lut_row, **btn_style).pack(fill="x", pady=2)
        
        # ===== RIGHT COLUMN: Graph Area (Wide) =====
        right_frame = tk.Frame(main_frame, bg=self.clr["BG"])
        right_frame.pack(side="right", fill="both", expand=True)
        
        # Example inputs (compact, top)
        example_frame = tk.Frame(right_frame, bg=self.clr["BG2"], relief="solid", borderwidth=1)
        example_frame.pack(fill="x", pady=(0, 8))
        
        tk.Label(
            example_frame, text="Test:",
            bg=self.clr["BG2"], fg=self.clr["FG"],
            font=("Segoe UI", 8, "bold")
        ).pack(side="left", padx=(8, 5), pady=6)
        
        # Temp
        tk.Label(example_frame, text="Temp:", bg=self.clr["BG2"], fg=self.clr["FG"],
                font=("Segoe UI", 8)).pack(side="left", padx=(0, 2))
        self.example_temp_var = tk.StringVar(value="12")
        tk.Entry(example_frame, textvariable=self.example_temp_var, width=4,
                font=("Consolas", 9), justify="center",
                bg=self.clr["BG3"], fg=self.clr["FG"]).pack(side="left")
        tk.Label(example_frame, text="°C", bg=self.clr["BG2"], fg=self.clr["FG_DIM"],
                font=("Segoe UI", 8)).pack(side="left", padx=(1, 10))
        
        # Cloud
        tk.Label(example_frame, text="Clouds:", bg=self.clr["BG2"], fg=self.clr["FG"],
                font=("Segoe UI", 8)).pack(side="left", padx=(0, 2))
        self.example_cloud_var = tk.StringVar(value="50")
        tk.Entry(example_frame, textvariable=self.example_cloud_var, width=4,
                font=("Consolas", 9), justify="center",
                bg=self.clr["BG3"], fg=self.clr["FG"]).pack(side="left")
        tk.Label(example_frame, text="%", bg=self.clr["BG2"], fg=self.clr["FG_DIM"],
                font=("Segoe UI", 8)).pack(side="left", padx=(1, 10))
        
        # Update button
        tk.Button(
            example_frame, text="📊 Update",
            command=self._update_lut_graph,
            bg="#007acc", fg="#ffffff",
            font=("Segoe UI", 8, "bold"),
            relief="flat", padx=8, pady=2, cursor="hand2"
        ).pack(side="left", padx=5)
        
        # Calculation results (compact)
        calc_frame = tk.Frame(right_frame, bg=self.clr["BG2"], relief="solid", borderwidth=1)
        calc_frame.pack(fill="x", pady=(0, 8))
        
        self.lbl_calc_result = tk.Label(
            calc_frame, text="Click 'Update' to see calculation",
            bg=self.clr["BG2"], fg=self.clr["FG_DIM"],
            font=("Consolas", 8), justify="left", padx=8, pady=5
        )
        self.lbl_calc_result.pack(fill="x")
        
        # Graph container
        self.graph_container = tk.Frame(right_frame, bg=self.clr["BG"])
        self.graph_container.pack(fill="both", expand=True, pady=(0, 10))
        
        self._update_lut_graph()
        
        # Bottom controls (Target time + Cloud penalty)
        bottom_frame = tk.Frame(right_frame, bg=self.clr["BG2"], relief="solid", borderwidth=1)
        bottom_frame.pack(fill="x", pady=(0, 10))
        
        # Target time
        tk.Label(
            bottom_frame, text="First run ready by:",
            bg=self.clr["BG2"], fg=self.clr["FG"],
            font=("Segoe UI", 9)
        ).grid(row=0, column=0, sticky="e", padx=(15, 5), pady=10)
        
        target_parts = self.config.first_run_target_time.split(":")
        self.target_hour_var = tk.StringVar(value=target_parts[0])
        self.target_min_var = tk.StringVar(value=target_parts[1])
        
        time_frame = tk.Frame(bottom_frame, bg=self.clr["BG2"])
        time_frame.grid(row=0, column=1, sticky="w", pady=10)
        
        tk.Entry(
            time_frame, textvariable=self.target_hour_var, width=3,
            font=("Consolas", 11), justify="center",
            bg=self.clr["BG3"], fg=self.clr["FG"]
        ).pack(side="left")
        tk.Label(time_frame, text=":", bg=self.clr["BG2"], fg=self.clr["FG"],
                font=("Consolas", 11, "bold")).pack(side="left", padx=2)
        tk.Entry(
            time_frame, textvariable=self.target_min_var, width=3,
            font=("Consolas", 11), justify="center",
            bg=self.clr["BG3"], fg=self.clr["FG"]
        ).pack(side="left")
        
        # Separator
        tk.Label(bottom_frame, text="│", bg=self.clr["BG2"], fg=self.clr["SEP"],
                font=("Segoe UI", 14)).grid(row=0, column=2, padx=20)
        
        # Cloud penalty
        tk.Label(
            bottom_frame, text="Cloud penalty:",
            bg=self.clr["BG2"], fg=self.clr["FG"],
            font=("Segoe UI", 9)
        ).grid(row=0, column=3, sticky="e", padx=(0, 5), pady=10)
        
        self.cloud_penalty_var = tk.StringVar(value=str(self.weather_service.cloud_penalty_factor))
        tk.Entry(
            bottom_frame, textvariable=self.cloud_penalty_var, width=5,
            font=("Consolas", 11), justify="center",
            bg=self.clr["BG3"], fg=self.clr["FG"]
        ).grid(row=0, column=4, sticky="w", pady=10, padx=(0, 15))
        
        # Save button
        save_frame = tk.Frame(right_frame, bg=self.clr["BG"])
        save_frame.pack(fill="x")
        
        tk.Button(
            save_frame, text="💾  Save All Settings",
            command=self._save_settings,
            bg="#007acc", fg="#ffffff",
            font=("Segoe UI", 11, "bold"),
            relief="flat", padx=30, pady=10, cursor="hand2"
        ).pack(side="left")
        
        tk.Label(
            save_frame, text="Changes apply immediately",
            bg=self.clr["BG"], fg=self.clr["FG_DIM"],
            font=("Segoe UI", 9, "italic")
        ).pack(side="left", padx=15)
    
    # ===== LUT Management =====
    def _load_lut_to_tree(self):
        """Load LUT into treeview"""
        self.lut_tree.delete(*self.lut_tree.get_children())
        for temp in sorted(self.temp_lut.keys()):
            duration = self.temp_lut[temp]
            self.lut_tree.insert("", "end", values=(temp, duration))
    
    def _add_lut_row(self):
        """Add new LUT point"""
        dialog = self._create_lut_dialog("Add Temperature Point")
        
        temp_var = tk.StringVar()
        dur_var = tk.StringVar()
        
        self._add_dialog_fields(dialog, temp_var, dur_var)
        
        def save():
            try:
                temp = int(temp_var.get())
                duration = int(dur_var.get())
                if duration <= 0:
                    raise ValueError("Duration must be positive")
                
                self.temp_lut[temp] = duration
                self._load_lut_to_tree()
                self._update_lut_graph()
                dialog.destroy()
            except ValueError as e:
                messagebox.showerror("Invalid Input", f"Error: {e}", parent=dialog)
        
        self._add_dialog_buttons(dialog, save)
    
    def _edit_lut_row(self):
        """Edit selected LUT point"""
        selected = self.lut_tree.selection()
        if not selected:
            messagebox.showwarning("No Selection", "Please select a row to edit")
            return
        
        item = selected[0]
        values = self.lut_tree.item(item, 'values')
        old_temp = int(values[0])
        old_duration = int(values[1])
        
        dialog = self._create_lut_dialog("Edit Temperature Point")
        
        temp_var = tk.StringVar(value=str(old_temp))
        dur_var = tk.StringVar(value=str(old_duration))
        
        self._add_dialog_fields(dialog, temp_var, dur_var, select_temp=True)
        
        def save():
            try:
                new_temp = int(temp_var.get())
                new_duration = int(dur_var.get())
                if new_duration <= 0:
                    raise ValueError("Duration must be positive")
                
                if new_temp != old_temp:
                    del self.temp_lut[old_temp]
                
                self.temp_lut[new_temp] = new_duration
                self._load_lut_to_tree()
                self._update_lut_graph()
                dialog.destroy()
            except ValueError as e:
                messagebox.showerror("Invalid Input", f"Error: {e}", parent=dialog)
        
        self._add_dialog_buttons(dialog, save)
    
    def _delete_lut_row(self):
        """Delete selected LUT point"""
        selected = self.lut_tree.selection()
        if not selected:
            messagebox.showwarning("No Selection", "Please select a row to delete")
            return
        
        if len(self.temp_lut) <= 2:
            messagebox.showerror("Cannot Delete", "LUT must have at least 2 points")
            return
        
        item = selected[0]
        values = self.lut_tree.item(item, 'values')
        temp = int(values[0])
        
        if messagebox.askyesno("Confirm Delete", f"Delete temperature point {temp}°C?"):
            del self.temp_lut[temp]
            self._load_lut_to_tree()
            self._update_lut_graph()
    
    def _create_lut_dialog(self, title):
        """Create dialog window"""
        dialog = tk.Toplevel(self.parent)
        dialog.title(title)
        dialog.geometry("320x180")
        dialog.configure(bg=self.clr["BG"])
        dialog.transient(self.parent)
        dialog.grab_set()
        dialog.resizable(False, False)
        
        # Center
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (320 // 2)
        y = (dialog.winfo_screenheight() // 2) - (180 // 2)
        dialog.geometry(f"+{x}+{y}")
        
        return dialog
    
    def _add_dialog_fields(self, dialog, temp_var, dur_var, select_temp=False):
        """Add input fields to dialog"""
        main_frame = tk.Frame(dialog, bg=self.clr["BG"], padx=20, pady=15)
        main_frame.pack(fill="both", expand=True)
        
        # Temperature
        tk.Label(main_frame, text="Temperature (°C):", bg=self.clr["BG"],
                fg=self.clr["FG"], font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 5))
        temp_entry = tk.Entry(main_frame, textvariable=temp_var, width=15,
                             font=("Consolas", 11), bg=self.clr["BG3"], fg=self.clr["FG"])
        temp_entry.pack(fill="x", pady=(0, 15))
        temp_entry.focus()
        if select_temp:
            temp_entry.select_range(0, tk.END)
        
        # Duration
        tk.Label(main_frame, text="Duration (minutes):", bg=self.clr["BG"],
                fg=self.clr["FG"], font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 5))
        dur_entry = tk.Entry(main_frame, textvariable=dur_var, width=15,
                            font=("Consolas", 11), bg=self.clr["BG3"], fg=self.clr["FG"])
        dur_entry.pack(fill="x", pady=(0, 20))
        
        dialog._temp_entry = temp_entry
        dialog._dur_entry = dur_entry
    
    def _add_dialog_buttons(self, dialog, save_callback):
        """Add buttons to dialog"""
        main_frame = dialog.winfo_children()[0]
        
        btn_frame = tk.Frame(main_frame, bg=self.clr["BG"])
        btn_frame.pack(fill="x")
        
        tk.Button(btn_frame, text="Save", command=save_callback,
                 bg="#007acc", fg="#ffffff", font=("Segoe UI", 10, "bold"),
                 relief="flat", padx=20, pady=8, cursor="hand2", width=10).pack(side="left", padx=(0, 10))
        tk.Button(btn_frame, text="Cancel", command=dialog.destroy,
                 bg=self.clr["BG3"], fg=self.clr["FG"], font=("Segoe UI", 10),
                 relief="flat", padx=20, pady=8, cursor="hand2", width=10).pack(side="left")
        
        dialog.bind('<Return>', lambda e: save_callback())
    
    # ===== Graph =====
    def _update_lut_graph(self):
        """Update graph with example calculation"""
        if self.lut_graph_canvas:
            self.lut_graph_canvas.get_tk_widget().destroy()
        
        if not self.temp_lut:
            return
        
        # Get values
        try:
            example_temp = float(self.example_temp_var.get())
            example_cloud = float(self.example_cloud_var.get())
            penalty = float(self.cloud_penalty_var.get())
        except (ValueError, AttributeError):
            example_temp = 12.0
            example_cloud = 50.0
            penalty = self.weather_service.cloud_penalty_factor
        
        # Calculate
        base_duration = self.weather_service.calculate_duration(example_temp)
        effective_temp = example_temp - (example_cloud / 100) * penalty
        duration_with_penalty = self.weather_service.calculate_duration(effective_temp)
        
        diff = duration_with_penalty - base_duration
        diff_percent = (diff / base_duration * 100) if base_duration > 0 else 0
        
        calc_text = (
            f"Temp={example_temp:.1f}°C, Clouds={example_cloud:.0f}%  →  "
            f"Without: {base_duration}min | With: {duration_with_penalty}min | "
            f"Diff: +{diff}min ({diff_percent:+.1f}%)"
        )
        self.lbl_calc_result.config(text=calc_text)
        
        # Create graph
        fig = plt.Figure(figsize=(7, 5), facecolor=self.clr["BG"])
        ax = fig.add_subplot(111)
        ax.set_facecolor(self.clr["BG2"])
        
        temps = sorted(self.temp_lut.keys())
        durations = [self.temp_lut[t] for t in temps]
        
        if len(temps) >= 2:
            import numpy as np
            temp_range = np.linspace(min(temps), max(temps), 100)
            duration_curve_base = []
            
            for t in temp_range:
                for i in range(len(temps) - 1):
                    if temps[i] <= t <= temps[i + 1]:
                        t0, t1 = temps[i], temps[i + 1]
                        d0, d1 = durations[i], durations[i + 1]
                        d = d0 + (t - t0) / (t1 - t0) * (d1 - d0)
                        duration_curve_base.append(d)
                        break
                else:
                    if t <= temps[0]:
                        duration_curve_base.append(durations[0])
                    else:
                        duration_curve_base.append(durations[-1])
            
            # Base curve
            ax.plot(temp_range, duration_curve_base, color="#00cc88", linewidth=3,
                   label="WITHOUT Cloud Penalty", zorder=3)
            
            # With penalty curve
            temp_range_shifted = temp_range - (example_cloud / 100) * penalty
            duration_curve_penalty = []
            
            for t_shifted in temp_range_shifted:
                for i in range(len(temps) - 1):
                    if temps[i] <= t_shifted <= temps[i + 1]:
                        t0, t1 = temps[i], temps[i + 1]
                        d0, d1 = durations[i], durations[i + 1]
                        d = d0 + (t_shifted - t0) / (t1 - t0) * (d1 - d0)
                        duration_curve_penalty.append(d)
                        break
                else:
                    if t_shifted <= temps[0]:
                        duration_curve_penalty.append(durations[0])
                    else:
                        duration_curve_penalty.append(durations[-1])
            
            ax.plot(temp_range, duration_curve_penalty, color="#ff6b6b", linewidth=3,
                   linestyle="--", label=f"WITH Cloud Penalty ({example_cloud:.0f}% clouds)", zorder=3)
            
            # Example points
            ax.scatter([example_temp], [base_duration], color="#00cc88", s=200,
                      zorder=5, edgecolors="#ffffff", linewidths=2.5, marker='o')
            ax.scatter([example_temp], [duration_with_penalty], color="#ff6b6b", s=200,
                      zorder=5, edgecolors="#ffffff", linewidths=2.5, marker='s')
            
            # Arrow
            if abs(diff) > 2:
                ax.annotate('', xy=(example_temp, duration_with_penalty),
                           xytext=(example_temp, base_duration),
                           arrowprops=dict(arrowstyle='<->', color='#ffaa00', lw=2.5))
                
                mid_y = (base_duration + duration_with_penalty) / 2
                ax.text(example_temp + 0.5, mid_y, f'+{diff}min',
                       color='#ffaa00', fontweight='bold', fontsize=10,
                       bbox=dict(boxstyle='round,pad=0.4', facecolor=self.clr["BG2"],
                                edgecolor='#ffaa00', linewidth=2))
        
        # LUT points
        ax.scatter(temps, durations, color="#007acc", s=70, zorder=4,
                  edgecolors="#ffffff", linewidths=1, alpha=0.7)
        
        # Styling
        ax.set_xlabel("Temperature (°C)", color=self.clr["FG"], fontweight='bold', fontsize=11)
        ax.set_ylabel("Duration (minutes)", color=self.clr["FG"], fontweight='bold', fontsize=11)
        ax.set_title("Cloud Penalty Impact", color=self.clr["FG"], fontweight="bold", fontsize=12)
        ax.tick_params(colors=self.clr["FG_DIM"], labelsize=9)
        ax.grid(True, linestyle=":", alpha=0.3, color=self.clr["SEP"])
        ax.legend(facecolor=self.clr["BG2"], edgecolor=self.clr["SEP"],
                 labelcolor=self.clr["FG"], fontsize=9, loc='best')
        
        for spine in ax.spines.values():
            spine.set_edgecolor(self.clr["SEP"])
        
        # Embed
        canvas = FigureCanvasTkAgg(fig, master=self.graph_container)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        self.lut_graph_canvas = canvas
    
    # ===== Save =====
    def _save_settings(self):
        """Save all settings"""
        try:
            # Validate target time
            hour = self.target_hour_var.get().strip()
            minute = self.target_min_var.get().strip()
            time_str = f"{hour.zfill(2)}:{minute.zfill(2)}"
            
            if not self.runtime_settings.set_target_time(time_str):
                messagebox.showerror("Invalid Time", "Time must be HH:MM format (00:00-23:59)")
                return
            
            # Validate cloud penalty
            try:
                penalty = float(self.cloud_penalty_var.get())
                if not self.runtime_settings.set_cloud_penalty(penalty):
                    messagebox.showerror("Invalid Penalty", "Must be positive number")
                    return
            except ValueError:
                messagebox.showerror("Invalid Penalty", "Must be a number")
                return
            
            # Validate LUT
            if len(self.temp_lut) < 2:
                messagebox.showerror("Invalid LUT", "Need at least 2 points")
                return
            
            if not self.runtime_settings.set_temp_lut(self.temp_lut):
                messagebox.showerror("Invalid LUT", "Validation failed")
                return
            
            # Update services
            self.config.first_run_target_time = time_str
            self.config.cloud_penalty_factor = penalty
            self.config.temp_lut = self.temp_lut
            
            self.weather_service.temp_lut = self.temp_lut
            self.weather_service.cloud_penalty_factor = penalty
            
            self.scheduler.update_target_time(time_str)
            
            messagebox.showinfo(
                "Settings Saved",
                f"Settings saved!\n\n"
                f"Target: {time_str}\n"
                f"Check: {self.scheduler.check_hour:02d}:{self.scheduler.check_minute:02d}\n"
                f"Cloud penalty: {penalty}\n"
                f"LUT points: {len(self.temp_lut)}"
            )
            
        except Exception as e:
            messagebox.showerror("Save Failed", f"Error: {e}")
