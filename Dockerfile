FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    make \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry==1.8.4

WORKDIR /app

# Copiar todo el proyecto
COPY . /app

# Desactivar venv y resolver deps
RUN poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-ansi

CMD ["make", "etl"]
