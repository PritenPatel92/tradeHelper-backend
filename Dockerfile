FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata bash \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Ensure the script is executable (important!)
RUN chmod +x /app/run.sh
# (Optional but helpful on Windows dev machines: normalize line endings)
# RUN apt-get update && apt-get install -y --no-install-recommends dos2unix && dos2unix /app/run.sh

ENV TZ=US/Eastern
ENV PYTHONUNBUFFERED=1

# Run the supervisor shell; it will exec python app.py
ENTRYPOINT ["/bin/bash", "/app/run.sh"]
CMD []
