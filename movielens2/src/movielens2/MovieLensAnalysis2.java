package movielens2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieLensAnalysis2 {
	public static class RatingMapper extends Mapper<Object,Text,Text,Text>{
		
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[1]), new Text("rating\t"+parts[2]));
		}
	
	}
	
	public static class MovieNameMapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			context.write(new Text(parts[0]), new Text("movies\t"+parts[1]));
		}
	}
	
	public static class FinalReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			String name = "";
			float maxVal = Float.MIN_VALUE;
			for (Text val : values){
				String[] parts = val.toString().split("\t");
				if(parts[0].equals("movies")){
					name = parts[1];
				}
				else if(parts[0].equals("rating")){
					
					if(Float.parseFloat(parts[1]) > maxVal){
						maxVal = Float.parseFloat(parts[1]);
					}
				}
			}
			String str = String.format("%f", maxVal);
			context.write(new Text(name), new Text(str));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"highest rating of a moviename");
		
		job.setJarByClass(MovieLensAnalysis2.class);
		job.setReducerClass(FinalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieNameMapper.class);
		Path outputPath = new Path(args[2]);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	
}