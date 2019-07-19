#!/bin/bash
mvn clean package exec:java -Dexec.mainClass="RetrieveFromS3" -Dexec.args="1"
