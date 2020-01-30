/*
 * Author: Chris Holle
 * Course: NoSQL
 * Assignment: 1
 * Date: 01/30/20
 *
 * Description: MapReduce program that calculates Runs Created
 * by each player in Batting.csv over their career.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunsCreated {
  // Mapper
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    // Number of features present in data
    final static int NUM_BASEBALL_ATTRIBUTES = 22;
    // Mapper function
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Player attributes with their corresponding index
      // playerID, yearID[1],stint[2],teamID[3],lgID[4],G[5],AB[6],R[7],
      // H[8],2B[9],3B[10],HR[11],RBI[12],SB[13],CS[14],BB[15],SO[16],IBB[17],HBP[18],SH[19],SF[20],GIDP[21]
      String line = value.toString();
      String[] attribute_string = line.split(",");
      // Ensure that the header is not being read
      if (!attribute_string[0].equals("playerID")) {
      	// Setup attr int array
      	int[] attr = new int[NUM_BASEBALL_ATTRIBUTES];
      	for (int i = 5; i < attr.length; i++) {
		if (i >= attribute_string.length) {
                     attr[i] = 0;
                } else if (attribute_string[i].equals("")) {
		     attr[i] = 0;
	      	} else {
		     attr[i] = Integer.parseInt(attribute_string[i]);
	      	} 
      	}
	// Player id will be used as key
      	String player_id = attribute_string[0];
     	//double oneBase = h - (2b + 3b + hr);
      	double oneBase = attr[8] - (attr[9] + attr[10] + attr[11]);
      	//double tb = ( oneBase + (2.0*2b) + (3.0*3b) + (4.0*4b) );
      	double tb = ( oneBase + (2.0*attr[9]) + (3.0*attr[10]) + (4.0*attr[11]) );
      	//double obp_numerator = h + bb + ibb + hbp;
      	double obp_numerator = attr[8] + attr[15] + attr[17] + attr[18];
      	//double obp_denominator = ab + bb + ibb + hbp + sf + sh;
      	double obp_denominator = attr[6] + attr[15] + attr[17] + attr[18] + attr[20] + attr[19];
	// rc will be used as the value in the key, value pair
	double rc;
	if (obp_numerator == 0 || obp_denominator == 0) {
		rc = 0.0;	
	} else {
		double obp = obp_numerator / obp_denominator;
      	        rc = tb * obp;
	}
	context.write(new Text(player_id), new DoubleWritable(rc));
      }
    }
  }
  // Reducer
  public static class IntSumReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
    // Reducer function
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      // Add up each player's career rc
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      // Save into a DoubleWritable
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    // Configuration stuff
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "runs created");
    job.setJarByClass(RunsCreated.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.getConfiguration().set("mapreduce.output.basename", "RunsCreated_out");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
