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
    # Get the number of teams each player(that played on more than one team) was on and add column for count
    batting_count = batting.groupBy(batting.playerID).count().filter(sf.col('count') > 1)
    # Create a list of playerIDs that played on more than 1 team
    dups = batting_count.select(batting.playerID).rdd.flatMap(lambda x: x).collect()
    # Only show players that played for more than one team in a year
    batting = batting.filter(batting.playerID.isin(dups))
    # Sort in ascending order (important for reduce key on teams)
    batting = batting.orderBy([batting.playerID, batting.teamID], ascending=[1,1])
    # Add comma to end of each team
    batting = batting.select(batting.playerID, sf.concat(batting.teamID, sf.lit(",")), batting.yearID)
    # Convert to rdd
    batRDD = batting.rdd.map(list)
    # Reduce by key and add teams together in one column
    batRDD = batRDD.map(lambda x: (x[0], x[1])).reduceByKey(add).collect()
    batting = sc.parallelize(batRDD)
    # Convere back to DataFrame
    batting = batting.toDF(["playerID", "teams"])
    split_col = sf.split(batting['teams'], ',')
    # Break down sets of teams > 2 into sets of 2 with all combinations 
    full_list = []
    for team in batting.rdd.collect():
        split_team = team.teams.split(",")
        split_team = split_team[:-1]
        # Only run breakdown if more than 2 teams present
        if (len(split_team) > 2):
            for i in range(len(split_team)-1):
                for j in range(len(split_team)-i-1):
                    if (split_team[i] != split_team[i+j+1]):
                        tmp_lst = [team.playerID, str(split_team[i] + "," + split_team[i+j+1]+",")]
                        # Add set to list of combinations
                        full_list.append(tmp_lst)
    # Convert list to RDD
    extraRDD = sc.parallelize(full_list)
    # Convert RDD to DataFrame with column names
    extraDF = extraRDD.toDF(["playerID", "teams"])
    # Add the broken down sets to the existing DataFrame
    final_batting = batting.union(extraDF)
    # Remove any duplicate values
    final_batting = final_batting.dropDuplicates()
    # Order data
    final_batting = final_batting.orderBy(["playerID"], ascending=[1])
    # Remove old data with more than 2 teams per row
    final_batting = final_batting.filter(~final_batting.teams.like('%,%,%,'))
    # Add comma to end of each playerID to prepare for reduce
    final_batting = final_batting.select(sf.concat(final_batting.playerID, sf.lit(",")), final_batting.teams)
    finalRDD = final_batting.rdd.map(list)
    # Reduce on teams and add playerIDs together if in the same maximal clique
    finalRDD = finalRDD.map(lambda x: (x[1], x[0])).reduceByKey(add).collect()
    final_bat = sc.parallelize(finalRDD)
    # Convert to DataFrame
    final_batting = final_bat.toDF(["teamID", "playerID"])
    finalOutput = final_batting.rdd.map(list)
    # Prepare output
    finalOutput = finalOutput.map(lambda x: (str('Teams: ' + x[0])[:-1], str('Players: ' + x[1])[:-1])).collect()
    # Write to file
    for team, player in finalOutput:
        outFile.write("{} {}\n".format(team, player))
    # Close the local output file
    outFile.close()
    # Remove existing output file if exists
    hdfsDel = Popen(["hdfs", "dfs", "-rm", hdfsFileOut], stdin=PIPE, bufsize=-1)
    hdfsDel.communicate()
    # Copy results over to hdfs
    hdfsMove = Popen(["hdfs", "dfs", "-put", localFileOut, hdfsFileOut], stdin=PIPE, bufsize=-1)
    # Run command
    hdfsMove.communicate()
    # Delete local output file
    os.remove(localFileOut)
