#!/bin/bash
mvn package -Ppandas
unzip -d $1/ROOT target/*.war
