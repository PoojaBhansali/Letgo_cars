// cc Mfr Wise Price Analysis 
// vv LetGoCarsDriver

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LetGoCarsDriver {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: LetGoCars <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(LetGoCarsDriver.class);
    job.setJobName("LetGo Cars"); // Set a name of the Job

	// Set input and output directories using command line arguments, 
	//args[0] = name of input directory on HDFS, and 
	//args[1] =  name of output directory to be created to store the output file.

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
	// Specify names of Mapper and Reducer Class
    job.setMapperClass(LetGoCarsMapper.class);
    job.setReducerClass(LetGoCarsReducer.class);
	
	// Specify data type of output key and value
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ LetGoCarsDriver 