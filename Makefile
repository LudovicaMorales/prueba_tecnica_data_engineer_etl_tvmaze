# Nombre de la imagen y etiqueta
IMAGE_NAME = prueba_tecnica_data_engineer_etl_tvmaze
TAG = latest

.PHONY: build run shell etl

# Construye la imagen Docker (solo cuando cambies dependencias o el Dockerfile)
build:
	docker build -t $(IMAGE_NAME):$(TAG) .

# Ejecuta el contenedor en modo desarrollo, montando el directorio actual en /app
run:
	docker run --rm -it \
		-v $(PWD):/app \
		--name $(IMAGE_NAME) \
		$(IMAGE_NAME):$(TAG)

# Abre una shell interactiva dentro del contenedor con el código montado
shell:
	docker run --rm -it \
		-v $(PWD):/app \
		--name $(IMAGE_NAME) \
		$(IMAGE_NAME):$(TAG) bash

# Ejecuta el ETL
etl:
	poetry run python src/main_etl.py
