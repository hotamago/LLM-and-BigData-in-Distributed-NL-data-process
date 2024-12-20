@REM Build the Docker image
docker-compose build

@REM Create and start the container
docker-compose -p "hota-project-3" up -d --no-build