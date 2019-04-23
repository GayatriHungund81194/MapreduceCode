import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountProgram {

	  public static class TokenizerMapper
	       extends Mapper<LongWritable, Text,Text,Text>{

		
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	   
	    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
	      String line = value.toString();
	      String[] tok = value.toString().split(",");
	      Text reduKey = new Text(tok[0]);
	      String reduVal = tok[3]+","+"0";
	      context.write(new Text(reduKey), new Text(reduVal));
	      
	      
	     
	    }
	  }

	  public static class IntSumReducer
	       extends Reducer<Text,Text,Text,IntWritable> {
	    private IntWritable result = new IntWritable();
	    int count=0;
	    public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	
//	    	for (String val:values) {
//	    		String[] v = values.toString().split(",");
//		    	count = Integer.parseInt(v[1]);
//	    		if (key.toString().equals("200")) {
//	    		context.write(new Text(key), new IntWritable(count+1));
//	    		}
//	    		else {
//	    			context.write(new Text(key), new IntWritable(count+1));
//	    		}
//	    	}
	      
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(WordCountProgram.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}
