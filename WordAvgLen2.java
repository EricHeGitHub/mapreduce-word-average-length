//This is MapReduce with in-map combining designed by Jiawei He
//Student ID: z5086661
package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.EnumCounters.Map;
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


public class WordAvgLen2 {
	// create a data type containing two integers as private elements
	public static class IntPair implements Writable {
		private int length;
		private int number;

		// the default construction
		public IntPair() {
		}

		// using set function to initialize the two integers
		public IntPair(int length, int number) {
			set(length, number);
		}

		// set the value of length and number integers
		public void set(int length, int number) {
			this.length = length;
			this.number = number;
		}

		// return the value of length
		public int getLength() {
			return this.length;
		}

		// return the value of number
		public int getNumber() {
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
		// output the values of two integers to the output stream
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(this.length);
			out.writeInt(this.number);
		}
	}

	public static class InMapper extends Mapper<Object, Text, Text, IntPair> {
		// input <object, text>; output<Text, IntPair>
		HashMap<String, IntPair> partialResults = new HashMap<String, IntPair>();
		private final static IntWritable one = new IntWritable(1);
		private IntPair ipair = new IntPair();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			int length;
			int number;
			while (itr.hasMoreTokens()) {
				String w = itr.nextToken().toLowerCase();
				// get the first letter of the word
				String c = String.valueOf(w.charAt(0));
				// only consider words starting with letters
				if (c.charAt(0) <= 'z' && c.charAt(0) >= 'a') {
					// if HashMap has the key, then update the length and the numebr
					if (partialResults.containsKey(c)) {
						length = partialResults.get(c).getLength();
						number = partialResults.get(c).getNumber();
						length += w.length();
						number += 1;
						partialResults.put(c, new IntPair(length, number));
					} 
					// If HashMap has no such key, add a new key of (length, 1)
					else {
						length = w.length();
						number = 1;
						partialResults.put(c, new IntPair(length, number));
					}
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Set<String> keys = partialResults.keySet();
			Text word = new Text();
			// for each key in the current map, add up all length and number.
			for (String k : keys) {	
				word.set(k);
				context.write(word, partialResults.get(k));
			}
		}
	}

	public static class AvgReducer extends Reducer<Text, IntPair, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<IntPair> values, Context context)
				throws IOException, InterruptedException {
			float totalLength = 0;
			float totalNumber = 0;
			// Traverse all keys sent to the mapper by the partitioner
			for (IntPair val : values) {
				// add up the total length of words and the total
				// number of words in the whole file
				totalLength += val.getLength();
				totalNumber += val.getNumber();
			}
			// calculate the average length of these words
			result.set((totalLength * 1.0) / totalNumber * 1.0);
			// output the results to the file
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word average length");
		job.setJarByClass(WordAvgLen2.class);
		job.setMapperClass(InMapper.class);
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