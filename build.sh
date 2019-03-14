#!/usr/bin/env bash

set -x
set -e

mkdir -p bin;

javac $(find src -name "*.java") $(find test -name "*.java") -d bin
(
    cd bin;
    jar -cvfe \
        ./java-redis-client-$(git describe)--$(javac -version 2>&1 | sed 's/[^a-z0-9._]\+/-/g').jar \
        nl.melp.redis.RedisTest \
        $(find . -name "*.class")
);
