.PHONY: run-generators

run-generators: gen-mocks

install-tools:
	go install github.com/golang/mock/mockgen@v1.6.0

gen-mocks:
	mockgen -destination=mock.go -package stoabs -source=store.go
