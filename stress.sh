#!/bin/bash

URL="http://127.0.0.1:21121/"

run () {
    echo "====== $1 requests ======"
    ab -n $1 -c 20 $URL
}

run 100
run 1000
run 10000
run 100000
