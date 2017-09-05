#!/bin/bash
mvn -pl common,ui package -Ppandas
cp -v ui/target/bamboo-*.jar $1/
(cd $1; rm -f *-sources.jar; ln -s bamboo-*.jar bamboo.jar)
