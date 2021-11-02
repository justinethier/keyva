#!/bin/bash

# Use ApacheBench to make large numbers of requests

ab -n 20000 -c 200 "localhost:8080/seq/ab-test"
