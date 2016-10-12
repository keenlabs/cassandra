#!/bin/bash +x

function main() {
    set -x
    local javahome
    javahome=/usr/libexec/java_home

    ## Select JDK 1.7
    export JAVA_HOME=$($javahome -v 1.7)

    ant clean
    ant

    rm cassandra.zip
    zip -9 -j -X cassandra.zip build/apache-cassandra*.jar bin/*-ex
}

main



