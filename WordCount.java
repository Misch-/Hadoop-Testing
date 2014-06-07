

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
//Mapper takes longwritable line index as key with value text, has to be long or buffer overflows.
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private IntWritable productID = new IntWritable();
		private IntWritable productPrice = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//split into lines, each line has two tokens, read and parse them, store them, write them as intermediary pairs
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				productID.set(Integer.parseInt(tokenizer.nextToken()));
				productPrice.set(Integer.parseInt(tokenizer.nextToken()));
				context.write(productID, productPrice);
			}
		}
	} 

	public static class Reduce extends Reducer<IntWritable, IntWritable, Text, Text> {
		//take the intermediary pairs of product IDs and prices, do opertations on them, then print.
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException {
			int count = 0;
			int minimum = 0;
			int maximum = 0;
			double average = 0;
			for (IntWritable productPrice : values) {
				//min and max are set to first value given, Double.MAX_VALUE was giving me runtime problems
				if (count == 0) {minimum = productPrice.get(); maximum = productPrice.get();}
				if (productPrice.get() > maximum) {maximum = productPrice.get();}
				if (productPrice.get() < minimum) {minimum = productPrice.get();}
				count++;
				//increase count, add to the sum(average is used as sum variable)
				average += productPrice.get();
			}
			average = average / count;
			//print product id
			Text output1 = new Text("Product " + key + ": ");
			//print associated statistics/values
			Text output2 = new Text("Count: " + count + ", " + "Average: " + average + ", " + "Min: " + minimum + ", " + "Max: " + maximum);
			context.write(output1, output2);
		}
	}

	public static void main(String[] args) throws Exception {
		//gets start time to be subtracted later
		long startTime = System.nanoTime();  
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, "wordcount");

		//the example input setup wasn't working in the hadoop lab because it requested from a folder I 
		//didn't have permission for so I just changed the path to be explicit
		String inputFile = "/home/mische/wordcount/input/ProductPriceData.txt"; 
		String outputFile = "/home/mische/wordcount/output/";

		job.setJarByClass(WordCount.class);

		//mapper has different ouput than reducer so I had to add .setMapOutput values
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//Uses the number of nodes passed to main in args
		Integer nodes = Integer.parseInt(args[0]);
		//sets maxsplitsize to be the total lines divided by the number of nodes
		long dataLength = fs.getContentSummary(new Path(inputFile)).getLength(); 
		FileInputFormat.setMaxInputSplitSize(job, (long) (dataLength / nodes));
		//half as many reducers as mappers, note this means this code doesn't work on
		//one node because 1 mapper has 0 reducers and it stops once mapped
		//this must be edited if 1 node is desired
		job.setNumReduceTasks(nodes/2); 

		//add paths
		FileInputFormat.addInputPath(job, new Path(inputFile)); 
		Path outPath = new Path(outputFile);
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, new Path(outputFile));

		//do the map reduce
		job.waitForCompletion(true);

		long totalTime = System.nanoTime() - startTime;
		//System.nanoTime() is much more accurate than System.currentTimeMillis()
		//https://stackoverflow.com/questions/351565/system-currenttimemillis-vs-system-nanotime
		System.out.println("Executed in " + ((System.nanoTime() - startTime)/(double)1000000000) + " seconds");
	}

}


