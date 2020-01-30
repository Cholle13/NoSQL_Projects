## How to Compile code
First generate a classes directory:
```bash
mkdir classes
```
Then compile RunsCreated.java saving the classes in the class directory:
```bash
javac -cp $(hadoop classpath) -d ./classes/ RunsCreated.java 
```
Next generate the jar:
```bash
jar -cvf RunsCreated.jar -C ./classes/ .
```
If an output directory already exists in your target hadoop output directory, remove it:
```bash
$PATH_TO_HDFS/hdfs dfs -rm -r output
```

Next run the program and specify the input file (Batting.csv) and output directory (output) locations:
```bash
hadoop jar RunsCreated.jar RunsCreated baseballdatabank-2019.2/core/Batting.csv output
```

The output directory will contain 2 files:
- _SUCCESS: notifies of successful run
- RunsCreated_out-r-00000: Output file containing playerID's and their respective runs created.
