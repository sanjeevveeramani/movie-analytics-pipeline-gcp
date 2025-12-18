FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY scripts/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy code
COPY scripts/ /app/scripts/

# Cloud Run uses PORT env var
ENV PYTHONUNBUFFERED=1

# Start Flask via gunicorn
CMD exec gunicorn --bind :$PORT scripts.main:app
