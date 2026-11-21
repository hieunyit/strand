.PHONY: up down fault image image-tools-test image-tools-smoke

IMAGE ?= strand:local

up:
	docker compose -p strand-dev -f dev/compose.yaml up -d --wait --wait-timeout 300

down:
	docker compose -p strand-dev -f dev/compose.yaml down -v --remove-orphans

fault:
	go test -v -timeout 60m -run "TestG1_E2E_NetworkFaultsSourceAndTarget|TestG1_E2E_Kill9RandomCrashes|TestG1_E2E_TwoInstancesSamePipeline" ./tests/e2e

# Image Web kèm công cụ backup native (Dockerfile); test: công cụ tìm được, quyền đúng, --version chạy.
image:
	docker build -t $(IMAGE) .

image-tools-test:
	scripts/image-tools-test.sh $(IMAGE)

# Như trên + dump/nạp lại với server thật (MySQL 8.4/5.7, MariaDB, PostgreSQL, MongoDB) trong container tạm.
image-tools-smoke:
	LIVE=1 scripts/image-tools-test.sh $(IMAGE)
