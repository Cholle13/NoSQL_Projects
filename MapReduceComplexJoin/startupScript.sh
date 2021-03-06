#!/bin/bash

# Path to hdfs (Set your filepath to hdfs)
hdfsPath=$1

# Path to baseball data (Change to your filepath to baseball data)
dataPath=$2

$hdfsPath dfs -rm -r /users/holle/
javac -cp $(hadoop classpath) -d ./classes/ ComplexJoin.java
jar -cvf ComplexJoin.jar -C ./classes/ .
hadoop jar ComplexJoin.jar ComplexJoin ${dataPath}People.csv ${dataPath}Batting.csv ${dataPath}Teams.csv ${dataPath}TeamsFranchises.csv
$hdfsPath dfs -rm -r /users/holle/tmp/
$hdfsPath dfs -mv /users/holle/final/* /users/holle/
$hdfsPath dfs -rm -r /users/holle/final/
