#!/bin/bash
mvn -pl common,ui package -Ppandas
cp -v ui/target/bamboo-*.jar $1/
(cd $1; ln -s bamboo-*.jar bamboo.jar)
