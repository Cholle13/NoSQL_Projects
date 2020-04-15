# Assignment 5

## Notes
The GraphFrames solution of finding the maximal cliques is notably faster than the DataFrames implementation. This is due to the reduction in operations
and speed in which the Graph algorithms are able to find relations.

The motif used was setup to find players that had more than one relation with a team.

## How to compile

Install GraphFrames
```bash
pip install graphframes
```

Navigate to GraphFrames directory and replace \<File Path to HDFS Baseball Data\> in the following command with the correct path. Then run the following command:
```bash
spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 sparkyGraphFrames.py <File Path to HDFS Baseball Data>
```
Example:
```bash
spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 sparkyGraphFrames.py hdfs://localhost:9000/user/hadoop/baseballdatabank-2019.2/core/ 
```

The output file should appear in your hdfs /users/holle/spark directory as sparkyGraphFrames-out-CH.txt.
