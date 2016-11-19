#!/bin/bash
mvn compile
mvn exec:java -Dexec.mainClass="com.alibaba.middleware.race.createdata.CreateFileUtil"