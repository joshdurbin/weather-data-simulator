.PHONY: build clean deps run

build:
	go build -o weather-data-simulator .

clean:
	rm -f weather-data-simulator

deps:
	go get -u ./...
	go mod tidy

run: build
	./weather-data-simulator run