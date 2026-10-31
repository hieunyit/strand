.PHONY: up down

up:
	docker compose -p strand-dev -f dev/compose.yaml up -d --wait --wait-timeout 300

down:
	docker compose -p strand-dev -f dev/compose.yaml down -v --remove-orphans
