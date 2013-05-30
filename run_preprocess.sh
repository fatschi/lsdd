#!/bin/bash

mvn clean install
/home/fabian/stratosphere-0.2/bin/pact-client.sh run -j ./target/MultiBlocking-0.0-jar-with-packed-dependencies.jar -a 1 file:///home/fabian/lsdd/data/freedb_discs_combine.csv file:///home/fabian/lsdd/data/freedb_tracks_combine.csv file:///home/fabian/lsdd/out/preprocess
