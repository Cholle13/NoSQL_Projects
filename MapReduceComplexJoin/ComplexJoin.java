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
			int[] attr = new int[NUM_PLAYER_ATTRIBUTES];
			for(int i = 5; i < attr.length; i++) {
				if (i >= attribute_str.length) {
					attr[i] = 0;
				} else if (attribute_str[i].equals("")) {
					attr[i] = 0;
				} else {
					attr[i] = Integer.parseInt(attribute_str[i]);
				}
			}
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
			context.write(new Text(player_id), new Text(dataString));
		}

	}
  }
  // Reducer
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    // Reducer function
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	    String value;
	    String[] splitValues;
	    String tag;
	    String data = null, personDetails = null, playerDetails = null;
	    for (Text txtVal : values) {
		    value = txtVal.toString();
		    splitValues = value.split("-");
		    tag = splitValues[0];
		    if (tag.equalsIgnoreCase("Player")) {
		    // TODO: finish
		    	playerDetails = splitValues[1];
		    } else if (tag.equalsIgnoreCase("Person")) {
			personDetails = splitValues[1];
		    }
	    }
	    if (personDetails != null && playerDetails != null) {
		data = new String(personDetails + "," + playerDetails);
	    } else if (personDetails == null && playerDetails != null) {
		data = new String(playerDetails);
	    } else if (personDetails != null && playerDetails == null) {
	    	data = new String(personDetails);
	    } else {
		data = new String("");
	    }
	    String[] splitAttr;
	    String hr = null, yearID = null;
	    splitAttr = data.split(",");
	    StringBuilder keyBuilder = new StringBuilder();
	    keyBuilder.append(key.toString());
	    // Check if HR is greater than 0
	    if (splitAttr.length == 5) {
		yearID = splitAttr[1];
		hr = splitAttr[4];
		keyBuilder.append("," + yearID);
	    }

	    if (hr != null) {
		if (Integer.parseInt(hr) > 0) {
	    	    context.write(new Text(keyBuilder.toString()), new Text(data)); 
		}
	    }
    }
  }

  public static void main(String[] args) throws Exception {
    // Configuration stuff
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "complex join");
    job.setJarByClass(ComplexJoin.class);
    job.setMapperClass(PlayerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PlayerMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PlayerDataMapper.class);
    job.getConfiguration().set("mapreduce.output.basename", "Complex_out");
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
