run_kafka:
	docker compose up
run_server:
	go run main.go
run_listener:
	go run listener/main.go

.PHONY:	run_kafka run_server run_listener
