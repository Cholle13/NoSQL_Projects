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
import java.util.StringTokenizer;
import java.util.ArrayList;

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
			if (Integer.parseInt(attribute_str[1]) > 1899) {
				context.write(new Text(player_id), new Text(dataString));
			}
		}

	}
  }
  public static class TeamDataMapper extends Mapper<Object, Text, Text, Text> {
	final static int NUM_TEAM_ATTRIBUTES = 20;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] attribute_str = line.split(",");
		
		if(!attribute_str[0].equals("yearID")) {
			String franch_id = attribute_str[3];
			StringBuilder dataStringBuilder = new StringBuilder();
			dataStringBuilder.append("Team-");
			dataStringBuilder.append(attribute_str[0] + ",").append(attribute_str[19]);
			if (Integer.parseInt(attribute_str[0]) > 1899) {
				context.write(new Text(franch_id), new Text(dataStringBuilder.toString()));
			}
			dataStringBuilder = null;
		}
	}
  }	

  public static class FranchDataMapper extends Mapper<Object, Text, Text, Text> {
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] attribute_str = line.split(",");

		  if(!attribute_str[0].equals("franchID")) {
			  String franch_id = attribute_str[0];
			  StringBuilder dataStringBuilder = new StringBuilder();
			  dataStringBuilder.append("Franch-");
			  dataStringBuilder.append(attribute_str[1]);
			  context.write(new Text(franch_id), new Text(dataStringBuilder.toString()));
			  dataStringBuilder = null;
		  }
	  }
  }


  // Reducer Output to be used in another job
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

  public static void main(String[] args) throws Exception {
    // Configuration stuff
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "complex join");
    job.setJarByClass(ComplexJoin.class);
    // Player job
    job.setMapperClass(PlayerMapper.class);
    //job.setCombinerClass(PlayerReducer.class);
    job.setReducerClass(PlayerReducer.class);
    // Team Job
    //job.setMapperClass(TeamDataMapper.class);
    //job.setCombinerClass(TeamReducer.class);                                                                                                                                                                                          
    //job.setReducerClass(TeamReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // Player Job
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PlayerMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PlayerDataMapper.class);
    // Team job
    //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TeamDataMapper.class);
    //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FranchDataMapper.class);
    job.getConfiguration().set("mapreduce.output.basename", "Complex_out");
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
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
