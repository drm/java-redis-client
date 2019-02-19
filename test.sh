#!/usr/bin/env bash
set -x
set -e

./build.sh
java -cp ./bin/java-redis-client*.jar nl.melp.redis.RedisTest

