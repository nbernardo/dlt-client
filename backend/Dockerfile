FROM python:3.12.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

#COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "src/app.py"]
