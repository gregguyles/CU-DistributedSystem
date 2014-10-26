// Greg Guyles
// Distributed Systems
// 10-29-2013

/*********************************************************************
* This function searches the input directory extracting the temperature
* values. It then counts the number of values for each temperature.
**********************************************************************/

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Count {
	
	public static class MapCount
		extends Mapper <LongWritable, Text, DoubleWritable, IntWritable>{
		
		public void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			IntWritable uno = new IntWritable(1);
			String temp = line.substring(26, 30);
			DoubleWritable airTemp;
			if (temp.matches("\\d\\d\\.\\d")){
				airTemp = new DoubleWritable(Double.parseDouble(temp));	
				context.write(airTemp, uno);
			}
		}
	}
	
	public static class ReduceCount
		extends Reducer <DoubleWritable, IntWritable, IntWritable, DoubleWritable> {
		
		public void reduce(DoubleWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			int count = 0;
			for(IntWritable value : values) {
				count = count + value.get();
			}
			context.write(new IntWritable(count), key);
		}
	}
	
	public static class IntComparator extends WritableComparator {

	    public IntComparator() {
	        super(IntWritable.class);
	    }

	    public int compare(byte[] b1, int s1, int l1,
	            byte[] b2, int s2, int l2) {

	        Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
	        Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

	        return v1.compareTo(v2) * (-1);
	    }
	}
	
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(Count.class);
		job.setJobName("Count Temp Values");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapCount.class);
		job.setReducerClass(ReduceCount.class);
		
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(IntComparator.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1);	
	}
}