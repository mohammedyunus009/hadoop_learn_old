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





public static class RedisHashOutputFormat extends OutputFormat<Text, Text> 
{ 
	public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
	public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key"; 

	public static void setRedisHosts(Job job, String hosts)
	{
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts); 
	
	}
	
	public static void setRedisHashKey(Job job, String hashKey)
	{
		job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
	}

	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException,InterruptedException
	{
		return new RedisHashRecordWriter(job.getConfiguration().get(
			REDIS_HASH_KEY_CONF), job.getConfiguration().get(
			REDIS_HOSTS_CONF));
	}
	
	public void checkOutputSpecs(JobContext job) throws IOException
	{
		String hosts=job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty())
		{
			throw new IOException(REDIS_HOSTS_CONF + " is not act in configuration");
		}
		String hashKey = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if(hashKey == null ||hashKey.isEmpty())
		{
			throw new IOException(REDIS_HASH_KEY_CONF + "is not set in cinfiguration");
		}
	}
//////////////////////////////////////////////////////
	////////////////////////////////////////////
	////////////
	


public OutputCommitter getOutputCommitter(TaskAttemptContext context)
throws IOException, InterruptedException 
{ 
	return (new NullOutputFormat<Text, Text()).getOutputCommitter(context); 

} 

public static class RedisHashRecordWriter extends RecordWriter<Text, Text> 
{ 
	public static class RedisHashOutputFormat extends OutputFormat<Text, Text> 
	{ 

		public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts"; 
		public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

		public static void setRedisHosts(Job job, String hosts)
		{
			job.getConfiguration().set(REDIS_HOSTS_CONF, hosts); 
		} 


		public static void setRedisHashKey(Job job, String hashKey)
		{
			job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
		} 

		public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) 
				throws IOException, InterruptedException
		{
			return new RedisHashRecordWriter(job.getConfiguration().get( REDIS_HASH_KEY_CONF), job.getConfiguration().get( REDIS_HOSTS_CONF)); 
		}

		public void checkOutputSpecs(JobContext job) throws IOException
		{
	
			String hosts = job.getConfiguration().get(REDIS_HOST_CONF);

			if (hosts == null || hosts.isEmpty())
			{ 
				throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
			}


			String hashKey = job.getConfiguration().get(
					REDIS_HASH_KEY CONF);


	///////////////////////////////////////////


	// REDIS_HASH_KEY CONF); 

			if (hashKey == null || hashKey.isEmpty())
			{ 
				throw new IOException(REDIS_HASH_KEY_CONF +
						"is not set in cinfiguration");
			}
}


		public OutputCommitter getOutputCommitter(TaskAttemptContext context)
				throws IOException, InterruptedException 
		{ 
			return (new NullOutputFormat<Text,
					Text>()).getOutputCommitter(context);

		} 



public static class RedisHashRecordWriter
extends RecordWriter<Text, Text>
{
	public static class RedisHashRecordWriter extends
	RecordWriter<Text,Text>
	{

	private HashMap<Integer,Jedis> jedisMap= new HashMap<Integer, Jedis>();

	private String hashKey = null; 

	public RedisHashRecordWriter(String hashKey, String hosts)
	{ 
		this.hashKey = hashKey; 
	// Create a connection to Redis for each host 
	// Map an integer 0-(numRedisInstances 1) to the instanma 
		int i = O; 
		for (String host : hosts.split(",")) 
		{
			Jedis jedis = new Jedis(host); 
			jedis.connect();
			jedisMap.put(i, jedis); 
			++i;
		} 
	}


	public void write(Text key, value)
				throws IOException, InterruptedException
	{

//////////////////////////////
// written to 
			Jedis j = jedisMap.get(Math.abs(
					key.hashCode()) % jedisMap.size()); 
// Write the key/value pair 
			j.hset(hashKey, key.toString(),
					value.toString());

	} 

	public void close(TaskAttemptContext context)
				throws IOException,InterruptedException 
	{ 
// For each jedis instance, disconnect it 
		for (Jedis jedis ; jedisMap.values()) 
		{ 

			jedis.disconnect();

		}
	}
}