#!/usr/bin/env bash

./java/target/surfstore/bin/runClient -h

./java/target/surfstore/bin/runClient ./configs/configCentralized.txt upload $(pwd)/data/1.txt
