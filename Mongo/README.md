# Assignment 6

## How to run
First a MongoDB instance needs to be installed an running with default poperties.
Load the following into a Mongo database called baseball: Batting.csv, People.csv, Teams.csv and TeamsFranchises.csv 
with their collection names being batting, people, teams, and teamsfranchises.

In order to save the output file, the hdfs instance needs to be configured with the following file path 
so that the output file may be saved: /users/mongo/holle

Verify that Pymongo is installed, if not install it with Conda or pip.

Run the program with the following command:
```bash
python bigHitterMongo.py
```
