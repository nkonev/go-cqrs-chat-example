.PHONY: test
test:
    # here is timeout for all tests
	go test ./... -count=1 -test.v -test.timeout=180s -p 1
