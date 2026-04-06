# 1. Use Python 3.13
FROM python:3.13-slim

# 2. Install Linux system libraries for Tkinter
RUN apt-get update && apt-get install -y \
    python3-tk \
    libtk8.6 \
    && rm -rf /var/lib/apt/lists/*

# 3. Set the working directory
WORKDIR /app

# 4. Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy your project files
COPY . .

# 6. Run the app
CMD ["python", "main.py"]
