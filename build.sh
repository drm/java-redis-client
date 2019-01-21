#!/bin/bash

javac src/Redis.java -d bin
cd bin && jar -cvf ./java-redis-client-$(git describe).jar $(find . -name "*.class")
