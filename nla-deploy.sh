#!/bin/bash
mvn package -Ppandas
cp -v target/bamboo-*.jar $1/
(cd $1; ln -s bamboo-*.jar bamboo.jar)
