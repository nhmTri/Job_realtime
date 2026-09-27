.PHONY: up down build run logs clean

up:             ## bring up Kafka, Cassandra, MySQL, Redis and Grafana
	docker compose --env-file .env up -d

down:           ## stop the stack
	docker compose down

build:          ## compile every module
	mvn -B -DskipTests package

run: build      ## start the pipeline
	java -jar apprunner/target/*.jar

logs:
	docker compose logs -f --tail=100

clean:
	mvn -B clean && docker compose down -v
