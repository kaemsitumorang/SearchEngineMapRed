import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MutualFriend {
	public static class MyMapper extends Mapper<Object, Text, Text, Text> { 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.readLine()
			Text wordKey = new Text();
			Text wordOutput = new Text();
			while (line != null) { 
				String[] value = line.split(" ");
				for(int i = 2; i < value.length; i++){
					int compare = value[i].compareTo(value[0]);
					if(compare > 0){
						wordKey.set(value[0] + "," +value[i]);
						context.write(wordKey, new Text(value[i]);
					}
					else{
						wordKey.set(value[i] + "," +value[0]);
						context.write(wordKey, new Text(value[i]);
					}
				}
			}
		}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	      Set<Text> uniques = new HashSet<Text>();
	      for (Text value : values) {
	        if (uniques.add(value)) {
	          context.write(key, value);
	        }
	      }
	    }
	  }

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TreeSet<String> treeSet = new TreeSet<String>();
			Text result = new Text();
			for (Text val : values) {
				treeSet.add(val.toString());
			}
			String str = "";
			Iterator<String> itr = treeSet.iterator();
			while(itr.hasNext()){
	  			str = str + itr.next() + ",";
			}
			str = str.substring(0, str.length()-1)
			if(str.equals(key.toString())){
				result.set(str);
				context.write(null, result);
			}
		}	
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "mutual friend");
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}