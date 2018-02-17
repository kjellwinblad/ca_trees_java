#!/bin/bash

#first parameter: Sonatype.org username
#Second parameter: Sonatype.org password

mkdir -p ~/.sbt/1.1.1/


echo credentials += Credentials\(\"Sonatype Nexus Repository Manager\", > ~/.sbt/1.1.1/sonatype.sbt
echo        \"oss.sonatype.org\", >> ~/.sbt/1.1.1/sonatype.sbt
echo        \"$1\", >> ~/.sbt/1.1.1/sonatype.sbt
echo        \"$2\"\) >> ~/.sbt/1.1.1/sonatype.sbt
