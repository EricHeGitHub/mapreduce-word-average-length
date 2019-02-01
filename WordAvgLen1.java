//This is MapReduce with combiner designed by Jiawei He
//Student ID: z5086661
package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

public class WordAvgLen1 {
	// create a data type containing two integers as private elements
	public static class IntPair implements Writable{
		private int length;
		private int number;
		// the default construction
		public IntPair(){
		}
		// using set function to initialize the two integers
		public IntPair(int length, int number){
			set(length, number);
		}
		// set the value of length and number integers
		public void set(int length, int number){
			this.length = length;
			this.number = number;
		}
		// return the value of length
		public int getLength(){
			return this.length;
		}
		// return the value of number
		public int getNumber(){
			return this.number;
		}
		@Override
		// initialize numbers with input streams
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.length = in.readInt();
			this.number = in.readInt();
		}
		@Override
		//output the values of two integers to the output stream
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(this.length);
			out.writeInt(this.number);
		}
	}
	
	public  static class TokenizerMapper extends Mapper<Object, Text, Text, IntPair> {
		// input <object, text>; output<Text, IntPair>
		// create some privately used values as keys and values.
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private IntPair ipair = new IntPair();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(),
					" *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			while (itr.hasMoreTokens()) {
				String w = itr.nextToken().toLowerCase();
				// get the first letter of the word
				String c = String.valueOf(w.charAt(0));
				// only consider words starting with letters
				if (c.charAt(0)<='z' && c.charAt(0)>='a'){
				// use the letter to set the key
				word.set(c);
				// use the length of word and one to form a pair as values
				ipair.set(w.length(), 1);
				context.write(word, ipair);
				}
			}
		}
	}
	/*
	 * Create a combiner which accumulates the total length 
	 * and the total number of words starting with the same
	 * letter as the output pair of (length, number) of 
	 * the corresponding Mapper.
	 */
	public static class AvgCombiner extends Reducer<Text, IntPair, Text, IntPair>{
		private static IntPair ipair = new IntPair();
		public void combiner(Text key,Iterable<IntPair> values, Context context) throws IOException, InterruptedException{
			int totalLength = 0;
			int totalNumber = 0;
			for(IntPair val : values){
				// add up the total length and total number of words of one letter
				// in a local mapper
				totalLength += val.getLength();
				totalNumber += val.getNumber();
			}
			ipair.set(totalLength, totalNumber);
			// The output should be the same output Mapper
			context.write(key, ipair);
		}
	}
	public static class AvgReducer extends Reducer<Text, IntPair, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		
		public void reduce(Text key, Iterable<IntPair> values, Context context)
				throws IOException, InterruptedException {
			float totalLength = 0;
			float totalNumber = 0;
			//Traverse all keys sent to the mapper by the partitioner
			for (IntPair val : values) {
				// add up the total length of words and the total 
				// number of words in the whole file
				totalLength +=  val.getLength();
				totalNumber +=  val.getNumber();
			}
			// calculate the average length of these words
			result.set(totalLength* 1.0/totalNumber * 1.0);
			// output the results to the file
			context.write(key, result);
		}
	}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word average length");
    job.setJarByClass(WordAvgLen1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(AvgCombiner.class);
    job.setReducerClass(AvgReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntPair.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}