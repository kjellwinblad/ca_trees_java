#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cat $DIR/plugins.sbt >> $DIR/../project/plugins.sbt
