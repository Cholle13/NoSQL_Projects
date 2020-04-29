# author: Chris Holle
# file: bigHitterMongo.py
# project: Assignment 6
# description: Find all players that hit more homeruns than a 
#              team in a given year

import pymongo
from pymongo import MongoClient
import os
import sys
from subprocess import PIPE, Popen

# Configure the MongoDB client with default settings
client = MongoClient()
# Use the baseball database
db = client.baseball

# Setup local output file
localFileOut = "mongoDBBigHitter-out-CH.txt"
outFile = open(localFileOut, "w")
# HDFS output file
hdfsFileOut = "/users/mongo/holle/" + localFileOut

# Function to join player stats with their respective names
def get_names(year, hr):
    resultSet = db.batting.aggregate([
        {
            '$match': { 'yearID': year}
        },
        {
            '$group': {
                '_id': '$playerID',
                'hits': {'$sum': '$HR'}
            }
        },
        {
            '$match': {  
                'hits': { '$gt':hr }
            }
        },
        {
            '$lookup':
                {
                    'from': 'people',
                    'localField': '_id',
                    'foreignField': 'playerID',
                    'as': 'name'
                }
        },
        {
            '$sort': {'hits':-1}
        }
    ])
    return resultSet

# Function to join teams and teamsfranchises to connect franchise name wtih teams
def get_team_names(year):
    resultSet = db.teams.aggregate([
        {
            '$match': { 'yearID':year }
        },
        {
            '$lookup':
                {
                    'from': 'teamsfranchises',
                    'localField': 'franchID',
                    'foreignField': 'franchID',
                    'as': 'teamArr'
                }
        },
#        {
#            '$sort': {'_id':-1}
#        }
    ])    
    return resultSet

# Find all franchise names for the years 1900-1950
for year in range(1900,1950):
    results_teams = get_team_names(year)
    # Find all players that hit more than a given team for the given year
    for team in results_teams:
        # team['HR'] contains the number of Homeruns for the team for the given year
        results_players = get_names(year, team['HR'])
        # Print out all players that hit more than the given team for the given year
        for player in results_players:
            outFile.write("{} {} {} {}\n".format(
                team['teamArr'][0]['franchName'], 
                player['name'][0]['nameFirst'], 
                player['name'][0]['nameLast'], 
                year))
    print("Year:", year, "completed!")

# Close the file
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

