# Nombre de la imagen y etiqueta
IMAGE_NAME = prueba_tecnica_data_engineer_etl_tvmaze
TAG = latest

.PHONY: build run shell

# Construye la imagen Docker
build:
	docker build -t $(IMAGE_NAME):$(TAG) .

# Ejecuta el contenedor
run:
	docker run --rm -it \
		--name $(IMAGE_NAME) \
		$(IMAGE_NAME):$(TAG)

# Para abrir una shell interactiva dentro del contenedor 
shell:
	docker run --rm -it \
		--name $(IMAGE_NAME) \
		$(IMAGE_NAME):$(TAG) bash

etl:
	poetry run python src/main_etl.py
