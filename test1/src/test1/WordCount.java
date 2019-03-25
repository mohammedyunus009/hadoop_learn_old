package test1;

import java.io.IOException;

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


import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.lang.String;

//import com.opencsv.CSVReader;


public class WordCount {
public static void main(String [] args) throws Exception
{
	Configuration c=new Configuration();
	String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
	Path input=new Path(files[1]);
	Path output=new Path(files[2]);
	Job j=new Job(c,"Brawl");
	j.setJarByClass(WordCount.class);
	j.setMapperClass(MapForWordCount.class);
	j.setReducerClass(ReduceForWordCount.class);
	j.setOutputKeyClass(Text.class);
	j.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(j, input);
	FileOutputFormat.setOutputPath(j, output);
	System.exit(j.waitForCompletion(true)?0:1);
}




public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
{

	
	String[][] beating = new String[250][250];
	String[][] beaten = new String[250][250];
	int i=0;
	
	

String line = value.toString();
String[] words=line.split("\t");
String[] tup =words[1].split("\n");


String[] beat =new String[250];
IntWritable one = new IntWritable(1);

for(String w:tup)
{ 

	beat = w.split(",");
	for(String q:beat)
	{
		con.write(new Text(q), one);
	}
	
}




String[] tup1 = words[2].split("\n");
String[] beated =new String[250];
IntWritable neg = new IntWritable(-1);

for(String w:tup1)
{ 
	
	beated = w.split(",");
	for(String q:beated)
	{
		con.write(new Text(q), neg);
	}
	
}

}

		//for(String j:words){tup[i++]= j.split(","); }
//for(int w=1;w<words.length;w++){ System.out.println(words[w]);}
//System.exit(1);
		//for (String[] j:words){ beating[i] = j[1].split(",");    beaten[i++] = j[2].split(","); }
/*for(String word: words )
{
  Text outputKey = new Text(word.toUpperCase().trim());
  IntWritable outputValue = new IntWritable(1);
  con.write(outputKey, outputValue);
}*/

}







public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
{
public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
{
int sum = 0,i=0,j,max=0,min=0,imin=0,imax=0;
   for(IntWritable value : values)
   {
	   i++;
	   j=value.get();
	   if (max<j){max=j; imax=i;}
	   if (min>j){min=j; imin=i;}
	   sum += j;
   }
   con.write(word, new IntWritable(sum));
   //con.write(new Text("max"), new IntWritable(imax));
   //con.write(new Text("imin"), new IntWritable(imin));
}
}

}