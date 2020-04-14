# Assignment 4

## How to compile
Navigate to SparkJoins directory and replace \<File Path to HDFS Baseball Data\> with the correct path. Then run the following command:
```bash
spark-submit sparky.py <File Path to HDFS Baseball Data>
```
Example:
```bash
spark-submit sparky.py hdfs://localhost:9000/user/hadoop/baseballdatabank-2019.2/core/ 
```

The output file should appear in your hdfs /users/holle/spark directory as sparkyGraph-out-CH.txt.
