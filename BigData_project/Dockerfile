FROM python:3.11-slim

WORKDIR /app

# # system deps for cassandra driver
# RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ librdkafka-dev \
#     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy the app folder into /app/app so imports "from app.xxx import ..." work
COPY app/ app/

# ensure package is recognized (optional but harmless)
RUN touch app/__init__.py

CMD ["python", "-m", "app.main"]
