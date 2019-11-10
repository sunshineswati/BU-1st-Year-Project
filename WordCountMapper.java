package com.accenture.hadoop.wordcount;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	protected void map(Object key, Text value, Context  output) throws IOException, InterruptedException {
		// split the input string by 'space' delimiter
		/*StringTokenizer words = new StringTokenizer(value.toString(), " ");
		while (words.hasMoreTokens()) {
			String word = words.nextToken();
			context.write(new Text(word), new IntWritable(1));
		}*/
		StringTokenizer words = new StringTokenizer(value.toString()," ");
		while(words.hasMoreTokens()){
			String word = words.nextToken();
			output.write(new Text(word), new IntWritable(1));
			
			
		}
	}
}
