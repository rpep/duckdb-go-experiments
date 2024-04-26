.PHONY: all clean
all:
	go build ./cmd/basic
	go build ./cmd/statistics

clean:
	rm -f basic statistics


