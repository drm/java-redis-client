#!/bin/bash

rm -rf ./bin/
mkdir bin;

javac $(find src -name "*.java") $(find test -name "*.java") -d bin
cd bin && jar -cvf ./java-redis-client-$(git describe).jar $(find . -name "*.class")

