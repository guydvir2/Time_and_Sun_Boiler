"""
Settings Tab - FINAL (Left fixed, Right untouched)
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


class SettingsTab:
    def __init__(self, parent, config, runtime_settings, scheduler, weather_service, colors):
        self.parent = parent
        self.config = config
        self.runtime_settings = runtime_settings
        self.scheduler = scheduler
        self.weather_service = weather_service
        self.clr = colors

        # UI
        self.lut_tree = None
        self.lbl_calc_result = None
        self.graph_container = None
        self.lut_graph_canvas = None

        self.target_hour_var = None
        self.target_min_var = None
        self.cloud_penalty_var = None
        self.example_temp_var = None
        self.example_cloud_var = None

        self.temp_lut = self.weather_service.temp_lut

        self._create_ui()

    # ==========================================================
    # UI
    # ==========================================================
    def _create_ui(self):
        # Apply dark theme to Treeview (headers + rows)
        style = ttk.Style()
        style.theme_use("default")
        style.configure(
            "DarkLUT.Treeview",
            background="#1e1e1e",
            foreground="#e0e0e0",
            fieldbackground="#1e1e1e",
            rowheight=22,
            borderwidth=0,
        )
        style.configure(
            "DarkLUT.Treeview.Heading",
            background="#2d2d30",
            foreground="#cccccc",
            font=("Segoe UI", 9, "bold"),
            relief="flat",
            borderwidth=1,
        )
        style.map(
            "DarkLUT.Treeview",
            background=[("selected", "#005f87")],
            foreground=[("selected", "#ffffff")],
        )
        style.map(
            "DarkLUT.Treeview.Heading",
            background=[("active", "#3e3e42")],
        )

        main_frame = tk.Frame(self.parent, bg=self.clr["BG"])
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # ================= LEFT COLUMN =================
        left_frame = tk.Frame(main_frame, bg=self.clr["BG"], width=280)
        left_frame.pack(side="left", fill="y", padx=(0, 15))
        left_frame.pack_propagate(False)

        left_frame.grid_rowconfigure(0, weight=0)  # LUT box
        left_frame.grid_rowconfigure(1, weight=0)  # buttons
        left_frame.grid_rowconfigure(2, weight=0)  # settings
        left_frame.grid_rowconfigure(3, weight=0)  # test calc
        left_frame.grid_rowconfigure(4, weight=1)  # spacer
        left_frame.grid_rowconfigure(5, weight=0)  # save
        left_frame.grid_columnconfigure(0, weight=1)

        # ─── LUT ───
        lut_box = tk.LabelFrame(
            left_frame, text="  Temperature LUT  ",
            bg=self.clr["BG2"], fg=self.clr["FG"],
            font=("Segoe UI", 10, "bold"),
            relief="ridge", borderwidth=2,
            highlightbackground="#555555", highlightthickness=1,
            padx=8, pady=8
        )
        lut_box.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        cols = ("#", "Temp (°C)", "Duration (min)")
        self.lut_tree = ttk.Treeview(
            lut_box, columns=cols, show="headings", height=10,
            style="DarkLUT.Treeview"
        )

        self.lut_tree.heading("#", text="#")
        self.lut_tree.heading("Temp (°C)", text="Temp (°C)")
        self.lut_tree.heading("Duration (min)", text="Duration (min)")

        self.lut_tree.column("#", width=35, anchor="center")
        self.lut_tree.column("Temp (°C)", width=75, anchor="center")
        self.lut_tree.column("Duration (min)", width=100, anchor="center")

        self.lut_tree.tag_configure("oddrow",  background="#2a2a2e", foreground="#e0e0e0")
        self.lut_tree.tag_configure("evenrow", background="#1e1e1e", foreground="#e0e0e0")

        # Scrollbar
        lut_scroll = ttk.Scrollbar(lut_box, orient="vertical", command=self.lut_tree.yview)
        self.lut_tree.configure(yscrollcommand=lut_scroll.set)
        self.lut_tree.pack(side="left", fill="both", expand=True)
        lut_scroll.pack(side="right", fill="y")

        self._load_lut_to_tree()

        # ─── BUTTONS ───
        btn_frame = tk.Frame(left_frame, bg=self.clr["BG"])
        btn_frame.grid(row=1, column=0, sticky="ew", pady=(0, 8))

        btn_style = {
            "bg": self.clr["BG3"], "fg": self.clr["FG"],
            "font": ("Segoe UI", 9), "relief": "flat",
            "padx": 8, "pady": 5, "cursor": "hand2"
        }

        tk.Button(btn_frame, text="➕ Add Row",       command=self._add_lut_row,    **btn_style).pack(fill="x", pady=2)
        tk.Button(btn_frame, text="✏️ Edit Selected",  command=self._edit_lut_row,   **btn_style).pack(fill="x", pady=2)
        tk.Button(btn_frame, text="🗑️ Delete Selected", command=self._delete_lut_row, **btn_style).pack(fill="x", pady=2)

        # ─── SETTINGS ───
        settings_box = tk.LabelFrame(
            left_frame, text="  Settings  ",
            bg=self.clr["BG2"], fg=self.clr["FG"],
            font=("Segoe UI", 10, "bold"),
            relief="ridge", borderwidth=2,
            highlightbackground="#555555", highlightthickness=1,
            padx=12, pady=10
        )
        settings_box.grid(row=2, column=0, sticky="ew", pady=(0, 8))

        tk.Label(settings_box, text="Target ready:", bg=self.clr["BG2"], fg=self.clr["FG"]).grid(row=0, column=0, sticky="w")

        parts = self.config.first_run_target_time.split(":")
        self.target_hour_var = tk.StringVar(value=parts[0])
        self.target_min_var  = tk.StringVar(value=parts[1])

        tf = tk.Frame(settings_box, bg=self.clr["BG2"])
        tf.grid(row=0, column=1, sticky="e")
        tk.Entry(tf, textvariable=self.target_hour_var, width=3,
                 bg=self.clr["BG3"], fg=self.clr["FG"], justify="center").pack(side="left")
        tk.Label(tf, text=":", bg=self.clr["BG2"], fg=self.clr["FG"]).pack(side="left")
        tk.Entry(tf, textvariable=self.target_min_var, width=3,
                 bg=self.clr["BG3"], fg=self.clr["FG"], justify="center").pack(side="left")

        tk.Label(settings_box, text="Cloud penalty:",
                 bg=self.clr["BG2"], fg=self.clr["FG"]).grid(row=1, column=0, sticky="w", pady=(8, 0))

        self.cloud_penalty_var = tk.StringVar(value=str(self.weather_service.cloud_penalty_factor))
        tk.Entry(settings_box, textvariable=self.cloud_penalty_var, width=6,
                 bg=self.clr["BG3"], fg=self.clr["FG"], justify="center").grid(row=1, column=1, sticky="e", pady=(8, 0))

        # ─── TEST CALCULATION (left column) ───
        test_box = tk.LabelFrame(
            left_frame, text="  Test Calculation  ",
            bg=self.clr["BG2"], fg=self.clr["FG"],
            font=("Segoe UI", 9, "bold"),
            relief="ridge", borderwidth=2,
            highlightbackground="#555555", highlightthickness=1,
            padx=10, pady=8
        )
        test_box.grid(row=3, column=0, sticky="ew", pady=(0, 8))

        # Row 1: inputs + button
        input_row = tk.Frame(test_box, bg=self.clr["BG2"])
        input_row.pack(fill="x", pady=(0, 6))

        tk.Label(input_row, text="Temp:", bg=self.clr["BG2"], fg=self.clr["FG"],
                 font=("Segoe UI", 9)).pack(side="left")
        self.example_temp_var = tk.StringVar(value="12")
        tk.Entry(input_row, textvariable=self.example_temp_var, width=5,
                 bg=self.clr["BG3"], fg=self.clr["FG"], justify="center").pack(side="left", padx=(3, 8))

        tk.Label(input_row, text="Clouds:", bg=self.clr["BG2"], fg=self.clr["FG"],
                 font=("Segoe UI", 9)).pack(side="left")
        self.example_cloud_var = tk.StringVar(value="50")
        tk.Entry(input_row, textvariable=self.example_cloud_var, width=5,
                 bg=self.clr["BG3"], fg=self.clr["FG"], justify="center").pack(side="left", padx=(3, 8))

        tk.Button(input_row, text="📊", command=self._update_lut_graph,
                  bg=self.clr["BG3"], fg=self.clr["FG"],
                  font=("Segoe UI", 9), relief="flat",
                  padx=6, pady=3, cursor="hand2").pack(side="left")

        # Row 2: result text (wraps inside the box)
        self.lbl_calc_result = tk.Label(
            test_box, text="",
            bg=self.clr["BG2"], fg=self.clr["FG_DIM"],
            font=("Consolas", 8),
            wraplength=240, justify="left", anchor="w"
        )
        self.lbl_calc_result.pack(fill="x")

        # ─── SPACER ───
        tk.Frame(left_frame, bg=self.clr["BG"]).grid(row=4, column=0, sticky="nsew")

        # ─── SAVE ───
        tk.Button(
            left_frame,
            text="💾  Save All Settings",
            command=self._save_settings,
            bg="#007acc", fg="#ffffff",
            font=("Segoe UI", 10, "bold"),
            relief="flat", padx=20, pady=10, cursor="hand2"
        ).grid(row=5, column=0, sticky="ew")

        # ================= RIGHT COLUMN =================
        right_frame = tk.Frame(main_frame, bg=self.clr["BG"])
        right_frame.pack(side="right", fill="both", expand=True)

        self.graph_container = tk.Frame(right_frame, bg=self.clr["BG"])
        self.graph_container.pack(fill="both", expand=True)

        # FINAL call (after everything exists)
        self._update_lut_graph()

    # ==========================================================
    def _load_lut_to_tree(self):
        self.lut_tree.delete(*self.lut_tree.get_children())
        for i, temp in enumerate(sorted(self.temp_lut.keys()), start=1):
            duration = self.temp_lut[temp]
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            self.lut_tree.insert("", "end", values=(i, temp, duration), tags=(tag,))

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
        old_temp = int(values[1])
        old_duration = int(values[2])

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
        temp = int(values[1])

        if messagebox.askyesno("Confirm Delete", f"Delete temperature point {temp}°C?"):
            del self.temp_lut[temp]
            self._load_lut_to_tree()
            self._update_lut_graph()

    def _create_lut_dialog(self, title):
        dialog = tk.Toplevel(self.parent)
        dialog.title(title)
        dialog.configure(bg=self.clr["BG"])
        dialog.transient(self.parent)
        dialog.grab_set()
        dialog.resizable(False, False)
        return dialog

    def _add_dialog_fields(self, dialog, temp_var, dur_var, select_temp=False):
        main_frame = tk.Frame(dialog, bg=self.clr["BG"], padx=14, pady=10)
        main_frame.pack(fill="both", expand=True)

        # Row: Temp label + entry
        row1 = tk.Frame(main_frame, bg=self.clr["BG"])
        row1.pack(fill="x", pady=(0, 6))
        tk.Label(row1, text="Temp (°C):", bg=self.clr["BG"],
                 fg=self.clr["FG"], font=("Segoe UI", 9), width=13, anchor="w").pack(side="left")
        temp_entry = tk.Entry(row1, textvariable=temp_var, width=7,
                              font=("Consolas", 10), bg=self.clr["BG3"], fg=self.clr["FG"],
                              justify="center")
        temp_entry.pack(side="left")
        temp_entry.focus()
        if select_temp:
            temp_entry.select_range(0, tk.END)

        # Row: Duration label + entry
        row2 = tk.Frame(main_frame, bg=self.clr["BG"])
        row2.pack(fill="x", pady=(0, 10))
        tk.Label(row2, text="Duration (min):", bg=self.clr["BG"],
                 fg=self.clr["FG"], font=("Segoe UI", 9), width=13, anchor="w").pack(side="left")
        dur_entry = tk.Entry(row2, textvariable=dur_var, width=7,
                             font=("Consolas", 10), bg=self.clr["BG3"], fg=self.clr["FG"],
                             justify="center")
        dur_entry.pack(side="left")

        dialog._temp_entry = temp_entry
        dialog._dur_entry  = dur_entry

    def _add_dialog_buttons(self, dialog, save_callback):
        main_frame = dialog.winfo_children()[0]

        btn_frame = tk.Frame(main_frame, bg=self.clr["BG"])
        btn_frame.pack(fill="x")

        tk.Button(btn_frame, text="Save", command=save_callback,
                  bg="#007acc", fg="#ffffff", font=("Segoe UI", 9, "bold"),
                  relief="flat", padx=14, pady=5, cursor="hand2").pack(side="left", padx=(0, 8))
        tk.Button(btn_frame, text="Cancel", command=dialog.destroy,
                  bg=self.clr["BG3"], fg=self.clr["FG"], font=("Segoe UI", 9),
                  relief="flat", padx=14, pady=5, cursor="hand2").pack(side="left")

        # Let tkinter size the dialog to fit content, then center it
        dialog.update_idletasks()
        w = dialog.winfo_reqwidth()
        h = dialog.winfo_reqheight()
        x = (dialog.winfo_screenwidth()  // 2) - (w // 2)
        y = (dialog.winfo_screenheight() // 2) - (h // 2)
        dialog.geometry(f"{w}x{h}+{x}+{y}")

        dialog.bind('<Return>', lambda e: save_callback())

    def _update_lut_graph(self):
        """Update graph with example calculation"""
        if self.lut_graph_canvas:
            self.lut_graph_canvas.get_tk_widget().destroy()

        if not self.temp_lut:
            return

        try:
            example_temp  = float(self.example_temp_var.get())
            example_cloud = float(self.example_cloud_var.get())
            penalty       = float(self.cloud_penalty_var.get())
        except (ValueError, AttributeError):
            example_temp  = 12.0
            example_cloud = 50.0
            penalty       = self.weather_service.cloud_penalty_factor

        base_duration        = self.weather_service.calculate_duration(example_temp)
        effective_temp       = example_temp - (example_cloud / 100) * penalty
        duration_with_penalty = self.weather_service.calculate_duration(effective_temp)

        diff         = duration_with_penalty - base_duration
        diff_percent = (diff / base_duration * 100) if base_duration > 0 else 0

        calc_text = (
            f"Temp={example_temp:.1f}°C  Clouds={example_cloud:.0f}%\n"
            f"Without: {base_duration}min  With: {duration_with_penalty}min\n"
            f"Diff: +{diff}min ({diff_percent:+.1f}%)"
        )
        self.lbl_calc_result.config(text=calc_text)

        fig = plt.Figure(figsize=(8, 5.5), facecolor=self.clr["BG"])
        ax  = fig.add_subplot(111)
        ax.set_facecolor(self.clr["BG2"])

        temps     = sorted(self.temp_lut.keys())
        durations = [self.temp_lut[t] for t in temps]

        if len(temps) >= 2:
            import numpy as np
            temp_range          = np.linspace(min(temps), max(temps), 100)
            duration_curve_base = []

            for t in temp_range:
                for i in range(len(temps) - 1):
                    if temps[i] <= t <= temps[i + 1]:
                        t0, t1 = temps[i], temps[i + 1]
                        d0, d1 = durations[i], durations[i + 1]
                        duration_curve_base.append(d0 + (t - t0) / (t1 - t0) * (d1 - d0))
                        break
                else:
                    duration_curve_base.append(durations[0] if t <= temps[0] else durations[-1])

            ax.plot(temp_range, duration_curve_base, color="#00cc88", linewidth=3,
                    label="WITHOUT Cloud Penalty", zorder=3)

            temp_range_shifted    = temp_range - (example_cloud / 100) * penalty
            duration_curve_penalty = []

            for t_shifted in temp_range_shifted:
                for i in range(len(temps) - 1):
                    if temps[i] <= t_shifted <= temps[i + 1]:
                        t0, t1 = temps[i], temps[i + 1]
                        d0, d1 = durations[i], durations[i + 1]
                        duration_curve_penalty.append(d0 + (t_shifted - t0) / (t1 - t0) * (d1 - d0))
                        break
                else:
                    duration_curve_penalty.append(durations[0] if t_shifted <= temps[0] else durations[-1])

            ax.plot(temp_range, duration_curve_penalty, color="#ff6b6b", linewidth=3,
                    linestyle="--", label=f"WITH Cloud Penalty ({example_cloud:.0f}% clouds)", zorder=3)

            ax.scatter([example_temp], [base_duration], color="#00cc88", s=200,
                       zorder=5, edgecolors="#ffffff", linewidths=2.5, marker='o')
            ax.scatter([example_temp], [duration_with_penalty], color="#ff6b6b", s=200,
                       zorder=5, edgecolors="#ffffff", linewidths=2.5, marker='s')

            if abs(diff) > 2:
                ax.annotate('', xy=(example_temp, duration_with_penalty),
                            xytext=(example_temp, base_duration),
                            arrowprops=dict(arrowstyle='<->', color='#ffaa00', lw=2.5))
                mid_y = (base_duration + duration_with_penalty) / 2
                ax.text(example_temp + 0.5, mid_y, f'+{diff}min',
                        color='#ffaa00', fontweight='bold', fontsize=10,
                        bbox=dict(boxstyle='round,pad=0.4', facecolor=self.clr["BG2"],
                                  edgecolor='#ffaa00', linewidth=2))

        ax.scatter(temps, durations, color="#007acc", s=70, zorder=4,
                   edgecolors="#ffffff", linewidths=1, alpha=0.7)

        ax.set_xlabel("Temperature (°C)", color=self.clr["FG"], fontweight='bold', fontsize=11)
        ax.set_ylabel("Duration (minutes)", color=self.clr["FG"], fontweight='bold', fontsize=11)
        ax.set_title("Cloud Penalty Impact", color=self.clr["FG"], fontweight="bold", fontsize=12)
        ax.tick_params(colors=self.clr["FG_DIM"], labelsize=9)
        ax.grid(True, linestyle=":", alpha=0.3, color=self.clr["SEP"])
        ax.legend(facecolor=self.clr["BG2"], edgecolor=self.clr["SEP"],
                  labelcolor=self.clr["FG"], fontsize=9, loc='best')

        for spine in ax.spines.values():
            spine.set_edgecolor(self.clr["SEP"])

        canvas = FigureCanvasTkAgg(fig, master=self.graph_container)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        self.lut_graph_canvas = canvas

    # ==========================================================
    # Save
    # ==========================================================
    def _save_settings(self):
        try:
            hour   = self.target_hour_var.get().strip()
            minute = self.target_min_var.get().strip()
            time_str = f"{hour.zfill(2)}:{minute.zfill(2)}"

            if not self.runtime_settings.set_target_time(time_str):
                messagebox.showerror("Invalid Time", "Time must be HH:MM format (00:00-23:59)")
                return

            try:
                penalty = float(self.cloud_penalty_var.get())
                if not self.runtime_settings.set_cloud_penalty(penalty):
                    messagebox.showerror("Invalid Penalty", "Must be positive number")
                    return
            except ValueError:
                messagebox.showerror("Invalid Penalty", "Must be a number")
                return

            if len(self.temp_lut) < 2:
                messagebox.showerror("Invalid LUT", "Need at least 2 points")
                return

            if not self.runtime_settings.set_temp_lut(self.temp_lut):
                messagebox.showerror("Invalid LUT", "Validation failed")
                return

            self.config.first_run_target_time = time_str
            self.config.cloud_penalty_factor  = penalty
            self.config.temp_lut              = self.temp_lut

            self.weather_service.temp_lut             = self.temp_lut
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
