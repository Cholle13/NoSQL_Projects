# Author: Chris Holle
# Project: Spark Baseball Maximal Clique GraphFrames
# Date: 4/14/2020
# Description: PySpark program to find all maximal cliques consisting of two or more 
#              teams for 2018 players.

# Imports
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as sf
from subprocess import PIPE, Popen
from graphframes import *
from operator import add
import functools as f
import sys
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
    localFileOut = "sparkyGraphFrames-out-CH.txt"
    outFile = open(localFileOut, "w")
    # HDFS output file
    hdfsFileOut = "/users/holle/spark/" + localFileOut

    # Read batting file
    batting = sqlc.read.csv(hdDataFilepath + "/Batting.csv", header=True, inferSchema=True)
    # Only use data from year 2018
    batting = batting.filter(batting.yearID == "2018").select(batting.playerID, batting.teamID, batting.stint, batting.yearID)
    # Create a table for pySQL
    batting.createOrReplaceTempView("Batting")
    
    # Create the edges for the Graph
    batting_edges = sqlc.sql('SELECT teamID as src, playerID as dst, stint FROM Batting')
    # Grab the vertices for the Graph
    batting_vertices = sqlc.sql('(SELECT distinct playerID as id FROM Batting) UNION (SELECT distinct teamID FROM Batting)')
    # Create Graph 
    bat_graph = GraphFrame(batting_vertices, batting_edges)

    # Create motif to find subgraph where two teams are related to a player
    motifs = bat_graph.find("(a)-[]->(player); (b)-[]->(player)")
    motifs = motifs.filter(motifs.a.id != motifs.b.id)
    motifs = motifs.filter(motifs.a.id < motifs.b.id)
    # combine teams and sort them
    motifs = motifs.select(sf.concat(motifs.player.id, sf.lit(",")), sf.concat(motifs.a["id"], sf.lit(","), motifs.b["id"]))
    # Rename columns
    motifs = motifs.withColumnRenamed("concat(player[id], ,)", "player")
    motifs = motifs.withColumnRenamed("concat(a[id], ,, b[id])", "teams")
    # Remove any duplicates that slip through
    motifs = motifs.drop_duplicates()
    # combine players
    finalRDD = motifs.rdd.map(list)
    # Reduce on teams and add playerIDs together if in the same maximal clique
    finalRDD = finalRDD.map(lambda x: (x[1], x[0])).reduceByKey(add).collect()
    final_bat = sc.parallelize(finalRDD)
    # Convert to DataFrame
    final_batting = final_bat.toDF(["teamID", "playerID"])
    finalOutput = final_batting.rdd.map(list)
    # Prepare output
    finalOutput = finalOutput.map(lambda x: (str('Teams: ' + x[0]), str('Players: ' + x[1])[:-1])).collect()
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

