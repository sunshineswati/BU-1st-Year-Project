package com.accenture.hadoop.wordcount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver {
	public static void main(String[] args) throws Exception {
		// Setting the input file path and output path.
		// These below 3 lines must be commented/removed while building the
		// MR job jar to deploy the it in cluster
		// args= new String[3];
		// args[0]=" C:\\ApacheHadoop\\InputFiles\\WordCount_Sample.txt ";
		// args[1]="C:\\ApacheHadoop\\wordcount_out";
		// note: The input path and output will be given as command line
		// arguments while submitting to the cluster.
		// creating the configuration
		Configuration conf = new Configuration();
		// creating the job instance
		Job job = Job.getInstance(conf, "wordcount");
		// set the Mapper, Reducer, Driver details to job
		job.setJarByClass(WordCountDriver.class);
		// The entry point and the configuration class which drives the job
		job.setMapperClass(WordCountMapper.class);
		// Mapper logic must be implemented in mapper class
		job.setReducerClass(WordCountReducer.class);
		// Reducer logic must be implemented in reducer class
		// set the map & reduce output key,value types
		// The key out and value out of mapper must be same in mapper class
		// implementation.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// The key out and value out of reducer must be same in reducer
		// class implementation.
		// It is considered as the actual key out and value out.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// set the file input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// job submission
		boolean jobStatus = job.waitForCompletion(true);
		if (jobStatus == false) {
			System.exit(1);
		}
	}
}