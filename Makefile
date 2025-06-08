.PHONY: test
test:
	go test ./... -count=1 -test.v -test.timeout=120s -p 1
