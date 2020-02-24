## How to Run code
First navigate to project directory and then ensure startupScript.sh has execute permissions:
```bash
chmod +x startupScript.sh
```
Then run the startupScript.sh by passing in the local path to hdfs and the path to the baseball data within hdfs:
```bash
./startupScript.sh ../../bin/hdfs ./baseballdatabank-2019.2/core/
```
This will delete the directory /users/holle in hdfs then compile the java code, create a jar and run the code on hadoop. After running it will delete the tmp directories that are created in /users/holle and then move the final data into the /users/holle directory from /users/holle/final. It will then delete the directory /users/holle/final.

The output directory will contain 2 files:
- _SUCCESS: notifies of successful run
- Holle-out-r-00000: Output file containing <Franchise Name Player Name Year>


## Layout  
Uses 4 input files:  
- People.csv
- Batting.csv
- Teams.csv
- TeamsFranchises.csv

#### There are 3 joins performed:  
- Reduce-side join to add a player's name to his stats  
    Classes:
    - PlayerMapper (Uses People.csv)
    - PlayerDataMapper (Uses Batting.csv)
    - Player Reducer
- Fragment and Replicate Join to add a team's franchise name to the team stats  
    Classes:
    - TeamDataMapper
- Reduce-side join to join the team data with the player data and reduce to the required output  
    Classes:  
    - FinalPlayerMapper (Uses output of PlayerReducer)
    - FinalTeamMapper (Uses output of Fragment and Replicate join)
    - PlayerTeamReducer (Produces final output)

#### There are 3 jobs executed:
- Player Job
    - Runs the reduce-side join on player info.
- Team Job
    - Runs the fragment and replicate join on team info.
- Final Job
    - Collects info from previous two jobs and produces correct output
    
## Fragment and Replicate Explanation
A fragment and replicate join was used on Teams.csv and TeamsFranchises.csv. This type of join works well, because TeamsFranchises.csv is a relatively small dataset that can easily fit into memory and be replicated to the data stored in Teams.csv. The franchise name from TeamsFranchises.csv was the only needed value, making it fast and easy to perform.

## Reduce-Side Join Explanation
The two reduce-side joins were used for simplicity and the fact that the data sets were relatively large in comparison to TeamsFranchises.csv which used the fragment and replicate join.

## Removing Created temp Files
An attempt was made to remove files from within the Java code. However, the following code snippit did not work as expected.
```Java
FileSystem.delete(Path f, boolean recursive)
```
Contrary to popular belief, the FileSystem delete function did not delete any files or directories. Several hours were spent in frustration on it. Many attempts were made to properly set the FileSystem configurations, but none were successful. As a result, the extra files are removed in the startupScript.sh after the java program is run on hadoop.
