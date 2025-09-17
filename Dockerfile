FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata bash \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Ensure scripts are executable
RUN chmod +x /app/run.sh /app/entrypoint.sh

ENV TZ=US/Eastern
ENV PYTHONUNBUFFERED=1

# Use entrypoint.sh instead of run.sh
ENTRYPOINT ["/app/entrypoint.sh"]
