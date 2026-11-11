FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 BOILER_DATA_DIR=/data TZ=Asia/Jerusalem
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=60s --timeout=5s CMD python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:8080/healthz', timeout=4)"
CMD ["python", "main.py"]
