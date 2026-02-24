import os
import csv
import json
import requests
import configparser
from datetime import datetime, timedelta, date
from statistics import mean
import tkinter as tk
from tkinter import ttk
from tkinter import font
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


# ==========================================================
# CONFIG
# ==========================================================
config = configparser.ConfigParser()
config.read("config.ini")

LAT = float(config["location"]["latitude"])
LON = float(config["location"]["longitude"])
WEATHER_URL = config["weather"]["weather_url"]

TEMP_LUT = {int(k): v for k, v in json.loads(
    config["temperature_duration_lut"]["temp_lut"]
).items()}

DATA_FOLDER = "stored_data"
os.makedirs(DATA_FOLDER, exist_ok=True)

# ==========================================================
# WEATHER + CALC
# ==========================================================
def get_weather():

    r = requests.get(
        WEATHER_URL,
        params={
            "latitude": LAT,
            "longitude": LON,
            "hourly": "temperature_2m,cloudcover",
            "daily": "sunrise,sunset",
            "timezone": "auto"
        },
        timeout=10
    )
    r.raise_for_status()
    data = r.json()

    today = date.today()

    sunrise = datetime.fromisoformat(data["daily"]["sunrise"][0])
    sunset = datetime.fromisoformat(data["daily"]["sunset"][0])

    weather = []
    for t, temp, clouds in zip(
        data["hourly"]["time"],
        data["hourly"]["temperature_2m"],
        data["hourly"]["cloudcover"]
    ):
        ts = datetime.fromisoformat(t)
        if ts.date() == today:
            weather.append({"time": ts, "temp": temp, "clouds": clouds})

    return sunrise, sunset, weather


def weighted_average(weather, sunrise, sunset):

    total_weight = 0
    temp_sum = 0
    cloud_sum = 0

    for w in weather:
        t = w["time"]

        if sunrise <= t < sunrise + timedelta(hours=3):
            weight = 0.5
        elif sunrise + timedelta(hours=3) <= t <= sunset - timedelta(hours=2):
            weight = 1.0
        elif sunset - timedelta(hours=2) < t <= sunset:
            weight = 0.5
        else:
            weight = 0

        if weight > 0:
            temp_sum += w["temp"] * weight
            cloud_sum += w["clouds"] * weight
            total_weight += weight

    return temp_sum/total_weight, cloud_sum/total_weight


def calculate_duration(temp):

    temps = sorted(TEMP_LUT.keys())

    if temp <= temps[0]:
        return TEMP_LUT[temps[0]]
    if temp >= temps[-1]:
        return TEMP_LUT[temps[-1]]

    for i in range(len(temps)-1):
        t0, t1 = temps[i], temps[i+1]
        if t0 <= temp <= t1:
            v0, v1 = TEMP_LUT[t0], TEMP_LUT[t1]
            ratio = (temp - t0)/(t1 - t0)
            return int(round(v0 + ratio*(v1-v0)))


def save_daily_summary():

    sunrise, sunset, weather = get_weather()
    avg_temp, avg_cloud = weighted_average(weather, sunrise, sunset)

    effective_temp = avg_temp - (avg_cloud/100)*3
    duration = calculate_duration(effective_temp)

    filename = os.path.join(DATA_FOLDER, f"{date.today()}.csv")

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date","sunrise","sunset",
            "avg_temp","avg_cloud","duration"
        ])
        writer.writerow([
            date.today(),
            sunrise.strftime("%H:%M"),
            sunset.strftime("%H:%M"),
            round(avg_temp,2),
            round(avg_cloud,2),
            duration
        ])


# ==========================================================
# GUI
# ==========================================================
class BoilerApp:

    def __init__(self, root):

        self.root = root
        self.root.title("Boiler Control Dashboard")
        self.root.geometry("1100x600")

        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Treeview",
                        rowheight=28,
                        font=("Segoe UI", 10))

        style.configure("Treeview.Heading",
                        font=("Segoe UI", 11, "bold"))

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        self.tab_table = ttk.Frame(notebook)
        self.tab_graph = ttk.Frame(notebook)

        notebook.add(self.tab_table, text="Daily Log")
        notebook.add(self.tab_graph, text="Graph")

        self.create_table()
        self.load_data()
        self.create_graph()

    # ------------------------------------------------------
    def create_table(self):

        columns = ("#","date","sunrise","sunset",
                   "avg_temp","avg_cloud","duration")

        self.tree = ttk.Treeview(
            self.tab_table,
            columns=columns,
            show="headings"
        )

        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120, anchor="center")

        scrollbar = ttk.Scrollbar(
            self.tab_table,
            orient="vertical",
            command=self.tree.yview
        )
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # alternating colors
        self.tree.tag_configure("odd", background="#f2f2f2")
        self.tree.tag_configure("even", background="#ffffff")

    # ------------------------------------------------------
    def load_data(self):

        self.tree.delete(*self.tree.get_children())

        all_rows = []

        for file in sorted(os.listdir(DATA_FOLDER)):

            if not file.endswith(".csv"):
                continue

            filepath = os.path.join(DATA_FOLDER, file)

            try:
                with open(filepath, newline="") as f:
                    reader = csv.DictReader(f)

                    # Only accept NEW FORMAT files
                    if reader.fieldnames != [
                        "date", "sunrise", "sunset",
                        "avg_temp", "avg_cloud", "duration"
                    ]:
                        continue

                    for row in reader:
                        all_rows.append(row)

            except Exception as e:
                print(f"Skipping {file}: {e}")

        # Sort by date (real date sorting, not string)
        try:
            all_rows.sort(
                key=lambda x: datetime.strptime(x["date"], "%Y-%m-%d")
            )
        except:
            pass

        # Insert rows
        for i, row in enumerate(all_rows, 1):
            tag = "even" if i % 2 == 0 else "odd"

            self.tree.insert(
                "",
                "end",
                values=(
                    i,
                    row["date"],
                    row["sunrise"],
                    row["sunset"],
                    row["avg_temp"],
                    row["avg_cloud"],
                    row["duration"]
                ),
                tags=(tag,)
            )
    # ------------------------------------------------------
    def create_graph(self):

        dates = []
        durations = []
        temps = []

        for file in sorted(os.listdir(DATA_FOLDER)):

            if not file.endswith(".csv"):
                continue

            filepath = os.path.join(DATA_FOLDER, file)

            try:
                with open(filepath, newline="") as f:
                    reader = csv.DictReader(f)

                    # Accept ONLY new format files
                    if reader.fieldnames != [
                        "date", "sunrise", "sunset",
                        "avg_temp", "avg_cloud", "duration"
                    ]:
                        continue

                    for row in reader:
                        dates.append(row["date"])
                        durations.append(int(row["duration"]))
                        temps.append(float(row["avg_temp"]))

            except Exception as e:
                print(f"Skipping {file} in graph: {e}")

        if not dates:
            return  # nothing to plot

        fig = plt.Figure(figsize=(8, 5))
        ax = fig.add_subplot(111)

        ax.plot(dates, durations, label="Duration (min)")
        ax.plot(dates, temps, label="Avg Temp (°C)")

        ax.set_title("Duration & Avg Temp Trend")
        ax.set_xlabel("Date")
        ax.set_ylabel("Value")
        ax.legend()

        ax.tick_params(axis="x", rotation=45)

        canvas = FigureCanvasTkAgg(fig, master=self.tab_graph)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

# ==========================================================
# ENTRY
# ==========================================================
if __name__ == "__main__":

    save_daily_summary()

    root = tk.Tk()
    app = BoilerApp(root)
    root.mainloop()