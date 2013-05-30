#!/bin/bash

mvn clean install
/home/fabian/stratosphere-0.2/bin/pact-client.sh run -j ./target/MultiBlocking-0.0-jar-with-packed-dependencies.jar -a 4 file:///home/fabian/lsdd/data/mini_discs.csv file:///home/fabian/lsdd/data/mini_tracks.csv file:///home/fabian/lsdd/out

