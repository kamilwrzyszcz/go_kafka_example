run_kafka:
	docker compose up
start_server:
	go run main.go
start_listener:
	go run listener/main.go

.PHONY:	run_kafka start_server start_listener