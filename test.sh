#!/usr/bin/env bash
set -x
set -e

./build.sh
VERSION=$(java -version 2>&1 | head -1 | egrep -o '"[^"]+"' | tr -d '"')
JAR=$(echo ./bin/java-redis-client-*$VERSION*.jar)

for f in $JAR; do java -jar $f; done;
