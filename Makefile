.PHONY: up down fault

up:
	docker compose -p strand-dev -f dev/compose.yaml up -d --wait --wait-timeout 300

down:
	docker compose -p strand-dev -f dev/compose.yaml down -v --remove-orphans

fault:
	go test -v -timeout 60m -run "TestG1_E2E_NetworkFaultsSourceAndTarget|TestG1_E2E_Kill9RandomCrashes|TestG1_E2E_TwoInstancesSamePipeline" ./tests/e2e
