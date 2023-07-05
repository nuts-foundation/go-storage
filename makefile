.PHONY: run-generators

run-generators: gen-mocks

install-tools:
	go install go.uber.org/mock/mockgen@v0.1.0

gen-mocks:
	mockgen -destination=mock.go -package stoabs -source=store.go
