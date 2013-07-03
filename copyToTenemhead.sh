#!/bin/bash

mvn -DskipTests clean install
scp target/MultiBlocking-0.0-jar-with-packed-dependencies.jar fabian.tschirschnitz@tenemhead2:/home/fabian.tschirschnitz
