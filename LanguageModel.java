import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			Configuration conf = new Configuration();
			conf.getInt("threashold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			if (count < threashold) return;
			
			//this is --> cool = 20
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {

				sb.append(words[i]).append(" ");
			}

			//what is the outputkey?
			String outputkey = sb.toString();
			//what is the outputvalue?
			String outputvalue = words[words.length - 1];
			
			//write key-value to reducer?
			if (outputkey != null && outputkey.length() >= 1)
				context.write(new Text(outputkey), new Text(outputvalue + " = " + count));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?
			//girl = 40
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for (Text value : values) {
				String[] strings = value.toString().trim().split("=");

				String word = strings[0].trim();
				int count = Integer.valueOf(strings[1].trim());

				if (tm.containsKey(count)) {
					tm.get(count).add(word);
				} else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count, list);
				}
			}

			Iterator<Integer> iterator = tm.keySet().iterator();
			for (int i = 0; i < n && iterator.hasNext(); i++) {
				int keyvalue = iterator.next();
				List<String> words = tm.get(keyvalue);
				for (String word : words) {
					context.write(new DBOutputWritable(key.toString(), word, keyvalue), NullWritable.get());
					i++;
				}
			}

		}
	}
}
