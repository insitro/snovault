#!/bin/bash

## Helper to run tests locally using same commands as circle ci config
# See: encoded/.circleci/config.yml
#
# Use Cases:  No argument defaults to not bdd tests
# $ circle-tests.sh bdd
# $ circle-tests.sh npm
# $ circle-tests.sh
##

if [ "$1" == "bdd" ]; then
    bin/test -v -v --timeout=400 -m "bdd" --tb=short --splinter-implicit-wait 10 --splinter-webdriver chrome --splinter-socket-timeout 300 --chrome-options "--headless --disable-gpu --no-sandbox --disable-dev-shm-usage --disable-extensions --whitelisted-ips --window-size=1920,1080"
    exit
fi

if [ "$1" == "npm" ]; then
    npm test
    exit
fi

if [ -z "$1" ]; then
    bin/test -v -v --timeout=400 -m "not bdd"
    exit
fi
