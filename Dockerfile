FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata bash \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Ensure scripts are executable
RUN chmod +x /app/entrypoint.sh /app/run.sh

ENV TZ=US/Eastern
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
CMD []
