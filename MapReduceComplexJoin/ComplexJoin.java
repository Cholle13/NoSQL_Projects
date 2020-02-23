/*
 * Author: Chris Holle
 * Course: NoSQL
 * Assignment: 2
 * Date: 02/20/20
 *
 * Description: MapReduce program that calculates Runs Created
 * by each player in Batting.csv over their career.
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

public class ComplexJoin {
  // Mapper
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
      	// Setup attr int array
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
	dataStringBuilder.append("Person-").append(attribute_string[13] + " ").append(attribute_string[14]);
	context.write(new Text(player_id), new Text(dataStringBuilder.toString()));
	dataStringBuilder = null;
      }
    }
  }
  public static class PlayerDataMapper extends Mapper<Object, Text, Text, Text> {
	final static int NUM_PLAYER_ATTRIBUTES = 22;
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
					// write the franchise name and year as key and the year, homerun and franchise name as the value
					context.write(new Text(franchName + "," + attribute_str[0]), new Text(dataStringBuilder.toString()));
				}
				dataStringBuilder = null;
			}
		}
		franchName = "";
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
	    // Collects user name and values
	    for (Text txtVal : values) {
		    value = txtVal.toString();
		    splitValues = value.split("-");
		    tag = splitValues[0];
		    if (tag.equalsIgnoreCase("Player")) {
			    dataVals.add(splitValues[1]);
		    } else if (tag.equalsIgnoreCase("Person")) {
			    playerName = splitValues[1];
		    }
	    }

	    // Loops through all collected values for player and writes to context
	    for (String playerValue : dataVals) {
	    		String[] splitAttr;
	    		String hr = null, yearID = null;
	    		splitAttr = playerValue.split(",");
	    		StringBuilder keyBuilder = new StringBuilder();
	    		keyBuilder.append(key.toString());
	    		// Check if HR is greater than 0
	    		if (splitAttr.length > 3) {
				yearID = splitAttr[0];
				hr = splitAttr[3];
				if (hr.equals("")) {
					hr = "0";
				}
				// Remove this to remove year from key
				keyBuilder.append("," + yearID);
	    		}

	    		if (hr != null) {
				if (Integer.parseInt(hr) > 0) {
	    	   			context.write(new Text(keyBuilder.toString()), new Text(new String(playerName + "," + playerValue))); 
				}
	    		}
	    }
    }
  }

  // Team Reducer to be used in another job
  public static class TeamReducer extends Reducer<Text, Text, Text, Text> {
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		  String value;
		  String[] splitValues;
		  String tag;
		  String data = null, teamDetails = null, franchDetails = null;
		  for (Text txtVal : values) {
			  value = txtVal.toString();
			  splitValues = value.split("-");
			  tag = splitValues[0];
			  if(tag.equalsIgnoreCase("Team")) {
				  teamDetails = splitValues[1];
			  } else if (tag.equalsIgnoreCase("Franch")) {
				  franchDetails = splitValues[1];
			  }
		  }

		  if (teamDetails != null && franchDetails != null) {
			  data = new String(teamDetails + "," + franchDetails);
		  } else if (teamDetails == null && franchDetails != null) {
			  data = new String(franchDetails);
		  
	  	  } else if (teamDetails != null && franchDetails == null) {
		  	data = new String(teamDetails);
		  } else {
			  data = new String("");
		  }
		  context.write(key, new Text(data));
	  }
  }

  // Get Job Info
  public static Job getPlayerJob(Configuration conf, String personInputPath, String playerInputPath, String outputPath) throws IOException {
	  Job job = Job.getInstance(conf, "complex join");
	  job.setJarByClass(ComplexJoin.class);
	  job.setMapperClass(PlayerMapper.class);
	  //job.setCombinerClass(PlayerReducer.class);
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


  public static void main(String[] args) throws Exception {
    // Configuration stuff
    Configuration conf = new Configuration();
    String playerInfoInput = args[0];
    String playerStatsInput = args[1]; 
    String playerOutput = args[2];
    String teamStatsInput = args[3];
    String teamFranchInput = args[4];
    String teamOutput = args[5];
    String finalOutput = "/users/holle/";
    Job playerJob = getPlayerJob(conf, playerInfoInput, playerStatsInput, playerOutput);
    Job teamJob = getTeamJob(conf, teamStatsInput, teamFranchInput, teamOutput);

    playerJob.waitForCompletion(true);

    System.exit(teamJob.waitForCompletion(true) ? 0 : 1);
  }
}
/*
 * Setup:
 * - Join People and Batting to get Player name associated with stats
 *   	- Creates playerInfo
 * - Fragment and replicate TeamFranchises and Teams to get franchName associated with team stats
 *   	- Creates teamInfo
 * - Join playerInfo and teamInfo on year
 *   	- Where player HR > team HR
 *
 *  TODO: Setup player job configuration
 *  TODO: Setup team job configuration
 *  TODO: Chain player and team job output into Reduce side join
 */
