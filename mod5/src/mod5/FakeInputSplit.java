package mod5;

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


public class FakeInputSplit
extends InputSplit implements Writable
{
public void readFields(DataInput argO) throws IOException { }

public void write(DataOutput argO) throws IOException { } 

public long getLength()
		throws IOException, InterruptedException 
{
	return 0; 
}

public String[] getLocations()
		throws IOException, InterruptedException
{
	return new String[0];
}
}
