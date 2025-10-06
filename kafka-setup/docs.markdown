docker compose down -v
docker compose up -d

## Verify Kafka listens on 19092 inside the container:
docker exec -it broker netstat -tlnp | grep 19092
nc -zv localhost 19092