.PHONY: int
int:
	INT=1 go test

redis_src/redis-server: get_redis.sh
	./get_redis.sh

# Test of Redis 7 FUNCTION commands
test_function_integration:
	INT=1 go test -v -timeout 30s ./integration -run TestFunction

.PHONY: clean
clean:
	rm -rf \
		redis_src \
		dump.rdb \
		nodes.conf
