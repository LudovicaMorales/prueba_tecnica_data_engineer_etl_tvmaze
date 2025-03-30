# Usar imagen de Python 3.10 empaquetada (slim)
FROM python:3.10-slim

# Instalar paquetes del sistema
RUN apt-get update && apt-get install -y \
    make \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instalar Poetry versión 1.8.4
RUN pip install --no-cache-dir poetry==1.8.4

# Crear un directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo de configuración de Poetry y el lock
COPY pyproject.toml poetry.lock /app/

# Desactivar la creación de entornos virtuales dentro del contenedor
RUN poetry config virtualenvs.create false

# Instalar dependencias (en este paso Poetry leerá pyproject.toml y poetry.lock)
RUN poetry install --no-interaction --no-ansi

# Copiar el resto del código del proyecto
COPY . /app

# Comando por defecto para tu contenedor:
CMD ["make", "etl"]
