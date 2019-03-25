package mod51;



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






public class RedisOutputMapper extends
Mapper<Object , Text,Text,Text>
{
	private Text outkey = new Text(); 

	private Text outvalue = new Text(); 

	public void map(0bject key, Text value, Context context) 
	throws IOException, InterruptedException 
	{ 
		Map<String, String> parsed = MRDPUtils.transforXmlToMap(value
		value.toString()); 
		String userId = parsed.get("Id");
		String reputation = parsed.get("Reputation"); 

// Set our output key and values 

		outkey.set(userId);
		outvalue.set(reputation); 

		context.write(outkey, outvalue); 

	}
}
