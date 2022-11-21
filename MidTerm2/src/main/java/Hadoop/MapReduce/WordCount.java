package Hadoop.MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

		private Text itemset = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] transactions = value.toString().split("\\t");
			
			List<String> itemsets = getItemSet(transactions);
			
			
			for(String set: itemsets) {
				itemset.set(set);
				
				
				context.write(itemset,);
					
			}
		}
		
		public List<String> getItemSet(String[] items){
			
			List<String> itemset = new ArrayList<String>();
			int n = items.length;
			int [] masks = new int[n];
			
			for(int i = 0; i < n ; i++)
				masks[i] = (1 << i);
			
			for(int i = 0; i < ( 1<<n); i++) {
				List<String> newList = new ArrayList<String>(n);
				
				for(int j = 0; j < n; j++) {
					
					if((masks[j] & i) != 0) {
						newList.add(items[j]);
					}
					if(j == n-1 && newList.size() > 0 && newList.size() < 5)
						itemset.add(newList.toString());
				}
			}
			return itemset;
		}
	}
	
	
	public static class pPartitioner extends Partitioner<Text, IntWritable>{
		
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			
			int keySize = key.toString().length();
			
			if(keySize == 1)
				return 0;
			else if(keySize == 2)
				return 1;
			else
				return 2;
		}	
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		
		job.setPartitionerClass(pPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
