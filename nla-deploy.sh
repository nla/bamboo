#!/bin/bash
mvn package -Ppandas
cp -v target/bamboo-*.jar $1/
