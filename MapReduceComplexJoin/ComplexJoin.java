/*
 * Author: Chris Holle
 * Course: NoSQL
 * Assignment: 2
 * Date: 02/23/20
 *
 * Description: MapReduce program that finds which players hit more homeruns
 * than a team in a given year.
 */
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;

public class ComplexJoin {
  // Player Mapper to grab player name
  public static class PlayerMapper
       extends Mapper<Object, Text, Text, Text>{

    // Number of features present in data
    final static int NUM_BASEBALL_ATTRIBUTES = 24;
    // Mapper function
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] attribute_string = line.split(",");
      // Ensure that the header is not being read
      if (!attribute_string[0].equals("playerID")) {
      	// Setup attr int array to fix some data errors
      	String[] attr = new String[NUM_BASEBALL_ATTRIBUTES];
      	for (int i = 5; i < attr.length; i++) {
		if (i >= attribute_string.length) {
                     attr[i] = "0";
                } else if (attribute_string[i].equals("")) {
		     attr[i] = "0";
	      	} else {
		     attr[i] = attribute_string[i];
	      	} 
      	}
	// Player id will be used as key
      	String player_id = attribute_string[0];
	// Player full name used as value
	StringBuilder dataStringBuilder = new StringBuilder();
	// Add tag and data to value
	dataStringBuilder.append("Person-").append(attribute_string[13] + " ").append(attribute_string[14]);
	context.write(new Text(player_id), new Text(dataStringBuilder.toString()));
	dataStringBuilder = null;
      }
    }
  }
  // Player Mapper to grab player HR for a given year
  public static class PlayerDataMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] attribute_str = line.split(",");
		if (!attribute_str[0].equals("")) {
			if(!attribute_str[0].equals("playerID")) {
				String player_id = attribute_str[0];
				StringBuilder dataStringBuilder = new StringBuilder();
				dataStringBuilder.append("Player-");
				// Add yearID
		        	dataStringBuilder.append(attribute_str[1].toString() + ",");
				// Add stint
				dataStringBuilder.append(attribute_str[2].toString() + ",");
				// Add teamID
				dataStringBuilder.append(attribute_str[3].toString() + ",");
				// Add HR
				dataStringBuilder.append(attribute_str[11].toString());
				String dataString = dataStringBuilder.toString();
				dataStringBuilder = null;
				if (!attribute_str.equals("")) {
					if (Integer.parseInt(attribute_str[1]) > 1899) {
						context.write(new Text(player_id), new Text(dataString));
					}	
				}
			}
		}

	}
  }

  // Team Mapper
  // Fragment and Replicate through Map-side Join
  public static class TeamDataMapper extends Mapper<Object, Text, Text, Text> {
	// Stores HashMap of Franchise ID to Franchise Name 
	private static HashMap<String, String> TeamFranchMap = new HashMap<String, String>();
	// Used to read from DCache
	private BufferedReader reader;
	private String franchName = "";
	private Text txtOutputKey = new Text("");
	private Text txtOutputValue = new Text("");

	enum FLAG { RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, ERROR }

	// Setup the Local Cache to find the correct File.
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path[] cacheLocal = context.getLocalCacheFiles(); 
		for (Path path : cacheLocal) {
			if (path.getName().toString().trim().equalsIgnoreCase("TeamsFranchises.csv")) {
				context.getCounter(FLAG.FILE_EXISTS).increment(1);
				loadFranchHashMap(path, context);
			}
		}
	}

	// Setup the Fanchise name HashMap to store map of Franchise id to Franchise name
	private void loadFranchHashMap(Path filePath, Context context) {
		String line = "";
		try {
			reader = new BufferedReader(new FileReader(filePath.toString()));

			// Read lines and load into HashMap
			while ((line = reader.readLine()) != null) {
				String franchAttr[] = line.split(",");
				TeamFranchMap.put(franchAttr[0].trim(), franchAttr[1].trim());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	// Map function that joins the team data
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		context.getCounter(FLAG.RECORD_COUNT).increment(1);
		if (line.length() > 0) {
			String[] attribute_str = line.split(",");
			if(!attribute_str[0].equals("yearID")) {
				// Set the Franch id
				String franch_id = attribute_str[3];
				// Grab the franchise Name from the Franchise HashMap
				franchName = TeamFranchMap.get(franch_id);
				StringBuilder dataStringBuilder = new StringBuilder();
				dataStringBuilder.append(attribute_str[19]);
				if (Integer.parseInt(attribute_str[0]) > 1899 && (!franchName.equals("") || !franchName.equals(null))) {
					// write the franchise name and year as key and the HR as the value
					context.write(new Text(franchName + "," + attribute_str[0] + ","), new Text(dataStringBuilder.toString()));
				}
				dataStringBuilder = null;
			}
		}
		franchName = "";
	}
  }	

  // Player mapper for prepping for Final reducer phase
  public static class FinalPlayerMapper extends Mapper<Object, Text, Text, Text> {
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] attribute_str = line.split(",");
		  // Setup key=year values=(playerName,HR)
		  context.write(new Text(attribute_str[1]), new Text("Player-" + attribute_str[2].trim() + "," + attribute_str[4]));
	  }
  }


  // Team Mapper for prepping for Final reducer phase
  public static class FinalTeamMapper extends Mapper<Object, Text, Text, Text> {
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] line_split = line.toString().split(",");
		  // Setup key=year values=(Team-franchName,HR)
		  context.write(new Text(line_split[1]), new Text("Team-" + line_split[0] + "," + line_split[2]));
	  }
  }

  // Player Reducer Output to be used in another job
  public static class PlayerReducer
       extends Reducer<Text,Text,Text,Text> {
    // Reducer function
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	    String value;
	    String[] splitValues;
	    String tag;
	    String playerName = null;
	    String data = null, personDetails = null, playerDetails = null;
	    ArrayList<String> dataVals = new ArrayList<String>();
	    HashMap<String, Integer> stintHr = new HashMap<String, Integer>();

	    // Collects player name and values
	    for (Text txtVal : values) {
		    value = txtVal.toString();
		    splitValues = value.split("-");
		    tag = splitValues[0];
		    // If Player, add values to ArrayList
		    if (tag.equalsIgnoreCase("Player")) {
			    // Add playerID,year as key and HR as value to HashMap
			    String[] playerAttr = splitValues[1].split(",");
			    if (playerAttr.length > 3) {
			    	String stintHrKey = key.toString() + "," + playerAttr[0];
			    	int hr = Integer.parseInt(playerAttr[3]);
			    	if (stintHr.containsKey(stintHrKey)) {
				    stintHr.put(stintHrKey, (stintHr.get(stintHrKey) + hr));
			    	} else {
				    stintHr.put(stintHrKey, hr);
				    dataVals.add(splitValues[1]);
			    	}
			    }
		      // If Person, add player name to variable
		    } else if (tag.equalsIgnoreCase("Person")) {
			    playerName = splitValues[1];
		    }
	    }

	    // Loops through all collected values for player and writes to context
	    for (String playerValue : dataVals) {
	    		String[] splitAttr;
	    		String hr = null, yearID = null;
	    		splitAttr = playerValue.split(",");
			// StringBuilders to prepare key and value for context write
	    		StringBuilder keyBuilder = new StringBuilder();
			StringBuilder valBuilder = new StringBuilder();
	    		keyBuilder.append(key.toString());
	    		// Check if HR is greater than 0 and add year to key
	    		if (splitAttr.length > 3) {
				yearID = splitAttr[0];
				hr = String.valueOf(stintHr.get(key.toString() + "," + yearID));
				if (hr.equals("")) {
					hr = "0";
				}
				// Add year to key
				keyBuilder.append("," + yearID + ",");
				// Setup value to contain player's name, year and the total hr for a year regardless of stint
				valBuilder.append(playerName + ",").append(yearID + ",").append(hr);
	    		}

	    		if (hr != null) {
				if (Integer.parseInt(hr) > 0) {
	    	   			context.write(new Text(keyBuilder.toString()), new Text(valBuilder.toString())); 
				}
	    		}
	    }
    }
  }

  // Final reducer for joining Players and Teams
  public static class PlayerTeamReducer extends Reducer<Text, Text, Text, Text> {
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  // Setup hashMap of player to hr
		  // Setup hashMap of team to hr
		  // each value in this reducer will have the same year as the key
		  // Loop through Player hashMap 
		  // 	Loop through Team hashMap(smaller)
		  // 		if Player HR > Team HR
		  // 			franchName playerName year
		  HashMap<String, String> TeamMap = new HashMap<String, String>();
		  HashMap<String, String> PlayerMap = new HashMap<String, String>();

		  String value;
		  String[] splitValues;
		  String[] splitAttributes;
		  String tag;
		  String year = key.toString();
		  // Load data into HashMaps
		  for (Text txtVal : values) {
			  value = txtVal.toString();
			  // Split values by tag delimeter
			  splitValues = value.split("-");
			  // Save tag for checking
			  tag = splitValues[0];
			  // Split values after tag
			  splitAttributes = splitValues[1].split(",");
			  // If team tag save data to Team HashMap
			  if(tag.equalsIgnoreCase("Team") && splitAttributes.length > 1) {
				  TeamMap.put(splitAttributes[0].trim(), splitAttributes[1].trim());
			    // If Player tag, save data to Player HashMap
			  } else if (tag.equalsIgnoreCase("Player")) {
				  PlayerMap.put(splitAttributes[0].trim(), splitAttributes[1].trim());
			  }
		  }
		  // Data is now loaded into HashMaps
		  
		  // Loop through Player HashMap
		  for (String player : PlayerMap.keySet()) {
			  int playerHR = Integer.parseInt(PlayerMap.get(player));
			  // Loop through Team HashMap
			  for (String team : TeamMap.keySet()) {
				  int teamHR = Integer.parseInt(TeamMap.get(team));
				  // If player HR is greater than team HR write to context
				  if (playerHR > teamHR) {
					  System.out.println("Team: " + team + " Player: " + player);
					  context.write(new Text(team + " " + player + " " + year), new Text(""));
				  }
			  }
		  }
	  }
  }

  // Get Job Info
  public static Job getPlayerJob(Configuration conf, String personInputPath, String playerInputPath, String outputPath) throws IOException {
	  // Player job configuration
	  Job job = Job.getInstance(conf, "complex join");
	  job.setJarByClass(ComplexJoin.class);
	  job.setMapperClass(PlayerMapper.class);
	  job.setReducerClass(PlayerReducer.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  MultipleInputs.addInputPath(job, new Path(personInputPath), TextInputFormat.class, PlayerMapper.class);
	  MultipleInputs.addInputPath(job, new Path(playerInputPath), TextInputFormat.class, PlayerDataMapper.class);
	  FileOutputFormat.setOutputPath(job, new Path(outputPath));
	  job.getConfiguration().set("mapreduce.output.basename", "Player_out");
	  return job;
  }
  // Get Team job info
  public static Job getTeamJob(Configuration conf, String inputPath, String cachePath, String outputPath) throws IOException {
	  // Team Job Configuration
	  Job job = Job.getInstance(conf, "complex join");
	  job.setJarByClass(ComplexJoin.class);
	  job.setMapperClass(TeamDataMapper.class);
	  job.setNumReduceTasks(0);
	  job.addCacheFile(new Path(cachePath).toUri());
	  FileInputFormat.setInputPaths(job, new Path(inputPath));
	  FileOutputFormat.setOutputPath(job, new Path(outputPath));
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  job.getConfiguration().set("mapreduce.output.basename", "Team_out");
	  return job;
  }

  // Get Final job info
  public static Job getFinalJob(Configuration conf, String playerInputPath, String teamInputPath, String outputPath) throws IOException {
	  Job job = Job.getInstance(conf, "complex join");
	  job.setJarByClass(ComplexJoin.class);
	  job.setReducerClass(PlayerTeamReducer.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  MultipleInputs.addInputPath(job, new Path(playerInputPath), TextInputFormat.class, FinalPlayerMapper.class);
	  MultipleInputs.addInputPath(job, new Path(teamInputPath), TextInputFormat.class, FinalTeamMapper.class);
	  FileOutputFormat.setOutputPath(job, new Path(outputPath));
	  job.getConfiguration().set("mapreduce.output.basename", "Holle-out");
	  return job;
  }
  // Main Driver code
  public static void main(String[] args) throws Exception {
    // Configuration stuff
    Configuration conf = new Configuration();
    // Handle given paths from command line arguments
    if (args.length < 4) {
	    throw new IOException("Command Line Usage: <People.csv> <Batting.csv> <Teams.csv> <TeamsFranchises.csv>"); 
    } else {
    	String playerInfoInput = args[0];
    	String playerStatsInput = args[1]; 
    	String playerOutput = "/users/holle/tmp/player/";
    	String teamStatsInput = args[2];
    	String teamFranchInput = args[3];
    	String teamOutput = "/users/holle/tmp/team/";
    	// Set output file path
    	final String finalOutput = "/users/holle/final/";
    	Job playerJob = getPlayerJob(conf, playerInfoInput, playerStatsInput, playerOutput);
    	Job teamJob = getTeamJob(conf, teamStatsInput, teamFranchInput, teamOutput);
   	Job finalJob = getFinalJob(conf, playerOutput, teamOutput, finalOutput);

    	playerJob.waitForCompletion(true);
    	teamJob.waitForCompletion(true);

    	System.exit(finalJob.waitForCompletion(true) ? 0 : 1);
    }
  }
}
