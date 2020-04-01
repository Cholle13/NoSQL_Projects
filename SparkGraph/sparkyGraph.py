# Author: Chris Holle
# Project: Spark Baseball Maximal Clique
# Date: 3/30/2020
# Description: PySpark program to find all maximal cliques consisting of two or more 
#              teams for 2018 players.

# Imports
from pyspark import SparkContext, SparkConf
from operator import add
from pyspark.sql import SQLContext
import functools as f
from pyspark.sql import functions as sf
import sys
from subprocess import PIPE, Popen
import os

# Confirm in Main
if __name__ == "__main__":
    # Configuration
    sc = SparkContext("local", "Baseball joins")
    sqlc = SQLContext(sc)

    # Grab file path to hadoop data
    if len(sys.argv) < 2:
        sys.exit()
    hdDataFilepath = sys.argv[1]

    # Setup local output file
    localFileOut = "sparkyGraph-out-CH.txt"
    outFile = open(localFileOut, "w")
    # HDFS output file
    hdfsFileOut = "/users/holle/spark/" + localFileOut

    batting = sqlc.read.csv(hdDataFilepath + "/Batting.csv", header=True)
    
    # Filter to select only playerID, teamID, yearID and stint in 2018
    batting = batting.filter(batting.yearID == "2018").select(batting.playerID, batting.teamID, batting.yearID)
    # Get the number of teams each player was on and add column for count
    batting_count = batting.groupBy(batting.playerID).count().filter(sf.col('count') > 1)
    dups = batting_count.select(batting.playerID).rdd.flatMap(lambda x: x).collect()
    # Only show players that played for more than one team in a year
    batting = batting.filter(batting.playerID.isin(dups))
    # Sort in ascending order
    batting = batting.orderBy([batting.playerID, batting.teamID], ascending=[1,1])
    batting = batting.select(batting.playerID, sf.concat(batting.teamID, sf.lit(",")), batting.yearID)
    # Convert to rdd
    batRDD = batting.rdd.map(list)
    # Reduce by key
    batRDD = batRDD.map(lambda x: (x[0], x[1])).reduceByKey(add).collect()
    batting = sc.parallelize(batRDD)
    batting = batting.toDF(["playerID", "teams"])
    split_col = sf.split(batting['teams'], ',')
     
    full_list = []
    for team in batting.rdd.collect():
        split_team = team.teams.split(",")
        split_team = split_team[:-1]
        if (len(split_team) > 2):
            for i in range(len(split_team)-1):
                for j in range(len(split_team)-i-1):
                    if (split_team[i] != split_team[i+j+1]):
                        tmp_lst = [team.playerID, str(split_team[i] + "," + split_team[i+j+1]+",")]
                        full_list.append(tmp_lst)
    extraRDD = sc.parallelize(full_list)
    extraDF = extraRDD.toDF(["playerID", "teams"])
    final_batting = batting.union(extraDF)
    final_batting = final_batting.dropDuplicates()
    final_batting = final_batting.orderBy(["playerID"], ascending=[1])
    final_batting = final_batting.filter(~final_batting.teams.like('%,%,%,'))
    final_batting = final_batting.select(sf.concat(final_batting.playerID, sf.lit(",")), final_batting.teams)
    finalRDD = final_batting.rdd.map(list)
    finalRDD = finalRDD.map(lambda x: (x[1], x[0])).reduceByKey(add).collect()
    final_bat = sc.parallelize(finalRDD)
    final_batting = final_bat.toDF(["teamID", "playerID"])
    final_batting = final_batting.filter(final_batting.playerID.like('%,%,%'))
    finalOutput = final_batting.rdd.map(list)
    finalOutput = finalOutput.map(lambda x: (str('Teams: ' + x[0]), str('Players: ' + x[1]))).collect()
    for team, player in finalOutput:
        outFile.write("{} {}\n".format(team, player))

    # Copy results over to hdfs
    hdfsMove = Popen(["hdfs", "dfs", "-put", localFileOut, hdfsFileOut], stdin=PIPE, bufsize=-1)
    # Run command
    hdfsMove.communicate()
    # Close the local output file
    outFile.close()
    # Delete local output file
    os.remove(localFileOut)
