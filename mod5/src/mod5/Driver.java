package mod5;
import java.text.SimpleDateFormat;
import java.util.ArrayList;


import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;



import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class Driver {

	public static void main(String[] args) throws Exception { 
	Configuration conf = new Configuration();
	int numMapTasks = Integer.parseInt(args[0]); 
	
	int numRecordsPerTask = Integer.parseInt(args[1]); 

	Path wordList = new Path(args[2]); 

	Path outputDir = new Path(args[3]); 

	Job job = new Job(conf, "Driver");
	job.setJarByClass(Driver.class);
	
	job.setNumReduceTasks(0);
	job.setInputFormatClass(RandomStackOverflowInputFormat.class);

	RandomStackOverflowInputFormat.setNumMapTasks(job,numMapTasks);
	
	
	
	
	
	RandomStackOverflowInputFormat.setNumRecordPerTask(job, numRecordsPerTask);

	RandomStackOverflowInputFormat.setRandomWordList(job, wordList); 

	//RandomStackOverflowInputFormat.setRandomWordList(job, outputDir);
	TextOutputFormat.setOutputPath(job,outputDir); 

	job.setOutputKeyClass(Text.class);

	job.setOutputValueClass(NullWritable.class);
	System.exit(job.waitForCompletion(true)?0:2); 

}
}