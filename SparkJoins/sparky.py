# Author: Chris Holle
# Project: Spark Baseball Joins
# Date: 3/4/2020
# Description: PySpark program to find the players that scored more HR than a team in a given year

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
    localFileOut = "sparky-out-CH.txt"
    outFile = open(localFileOut, "w")
    # HDFS output file
    hdfsFileOut = "/users/holle/spark/" + localFileOut
    
    # Pull data from hdfs
    batting = sqlc.read.csv(hdDataFilepath + "/Batting.csv", header=True)
    people = sqlc.read.csv(hdDataFilepath + "/People.csv", header=True)
    teams = sqlc.read.csv(hdDataFilepath + "/Teams.csv", header=True)
    teamsFranch = sqlc.read.csv(hdDataFilepath + "/TeamsFranchises.csv", header=True)

    # Join player's name with their batting info
    joinedPlayer = batting.join(people, batting.playerID == people.playerID).drop(people.playerID)
    # Filter out any data before 1900 and any players with 0 HR
    joinedPlayer = joinedPlayer.filter((joinedPlayer.yearID > '1899') & (joinedPlayer.HR > 0))
    # Combine the nameFirst column with the nameLast column to get player's name
    joinedPlayer = joinedPlayer.withColumn('name', sf.concat(sf.col('nameFirst'), sf.lit(' '), sf.col('nameLast')))
    # Generate a playerKey for the player
    joinedPlayer = joinedPlayer.withColumn('playerKey', sf.concat(sf.col('name'), sf.lit(','), sf.col('yearID')))
    # Select only the relevent information to reduce size
    joinedPlayer = joinedPlayer.select(['playerKey', 'name', 'yearID', 'HR', 'stint'])
    
    # Create RDD from DataFrame
    playerRDD = joinedPlayer.rdd.map(list)
    # Reduce on playerKey as key and HR as value to add all HR for all stints in a given year
    playerRDD = playerRDD.map(lambda x: (x[0], int(x[3]))).reduceByKey(add).collect()
    # Convert back  into RDD from list
    playerRDD = sc.parallelize(playerRDD)
    
    # Convert the player RDD back to DataFrame with the given columns
    joinedPlayer = playerRDD.toDF(['playerKey', 'HR'])
    # Split the player key into a name and yearID column
    split_col = sf.split(joinedPlayer['playerKey'], ',')
    joinedPlayer = joinedPlayer.withColumn('name', split_col.getItem(0))
    joinedPlayer = joinedPlayer.withColumn('yearID', split_col.getItem(1))
    
    # Join the team files to add franchName to team info
    joinedTeams = teams.join(teamsFranch, teams.franchID == teamsFranch.franchID).drop(teamsFranch.franchID)
    # Create a teamKey
    joinedTeams = joinedTeams.withColumn('teamKey', sf.concat(sf.col('franchID'), sf.lit(','), sf.col('yearID')))
    # Remove all data before year 1900
    joinedTeams = joinedTeams.filter(joinedTeams.yearID > '1899')
    # Create a teamKey column for team info
    joinedTeams = joinedTeams.select(sf.col('teamKey'), sf.col('franchName'), sf.col('yearID'), sf.col('HR').alias('teamHR'))
    # Final Join to join player data with team data
    finalJoin = joinedTeams.join(joinedPlayer, joinedTeams.yearID == joinedPlayer.yearID).drop(joinedPlayer.yearID).drop(joinedPlayer.playerKey).drop(joinedTeams.teamKey)
    # Filter out players that don't have more HRs than a team
    finalJoin = finalJoin.filter(finalJoin.teamHR.cast('float') < finalJoin.HR.cast('float'))
    # Convert DataFrame into RDD for  
    finalOutput = finalJoin.rdd.map(list)
    # Setup the rdd by mapping to particular attributes then convert to list
    finalOutput = finalOutput.map(lambda x: (x[0], x[4], x[1])).collect()
    # Output results
    for team, player, year in finalOutput:
        outFile.write("{} {} {}\n".format(team, player, year))

    # Create /users/holle directory if not already created
    hdfsMkdir = Popen(['hdfs', 'dfs', '-mkdir', '-p', '/users/holle'], stdin=PIPE, bufsize=-1)
    # Run the command
    hdfsMkdir.communicate()
    # Create /users/holle/spark directory if not already created
    hdfsMkdir = Popen(['hdfs', 'dfs', '-mkdir', '-p', '/users/holle/spark'], stdin=PIPE, bufsize=-1)
    # Run the command
    hdfsMkdir.communicate()
    # Copy results over to hdfs
    hdfsMove = Popen(["hdfs", "dfs", "-put", localFileOut, hdfsFileOut], stdin=PIPE, bufsize=-1)
    # Run command
    hdfsMove.communicate()
    # Close the local output file
    outFile.close()
    # Delete local output file
    os.remove(localFileOut)

