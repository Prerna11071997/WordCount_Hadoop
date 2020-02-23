package com.prernasingh.wc;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//the base TokenizerMapper class<MapInputKey , MapInputValue, MapOutputKey, MapOutputValue>
//Object is the base case of java.. it can hold anything.. alternative LongWritable
public class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable>
{
	
	private final static IntWritable one = new IntWritable(1);
	//the data needs to be travel over network hence it needs to be optimized hence it is compulsory
	private Text word = new Text();

	//Map method that we overwrite 
	//key = byteOffset = 0
	//value = complete line = DataFlair is the training provider
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		//Input key we ignore
		//value is t he important
		//context is the application context
		StringTokenizer itr = new StringTokenizer(value.toString());
		//StringTokenizer is going to tokenize the line
		/*
		 * DataFlair
		 * is
		 * the
		 * training
		 * Provider
		 */
		while (itr.hasMoreTokens())
		{
			word.set(itr.nextToken());
			context.write(word, one);
			/*
			 * DataFlair, 1
			 * is, 1
			 * the, 1
			 */
		}
	}
}