.PHONY: up down fault

up:
	docker compose -p strand-dev -f dev/compose.yaml up -d --wait --wait-timeout 300

down:
	docker compose -p strand-dev -f dev/compose.yaml down -v --remove-orphans

fault:
	go test -v -race -timeout 5m ./tests/fault
