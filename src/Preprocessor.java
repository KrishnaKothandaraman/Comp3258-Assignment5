import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Preprocessor{
	
	public static class URLMapper extends Mapper<Object, Text, Text, NullWritable> {

	  private Text modifiedLine = new Text();

	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    String[] columns = value.toString().split("\\s+");
	    if (!columns[0].equals("queryTime")) {
	      String url = columns[columns.length - 1];
	      String domainName = extractDomainName(url);
	      columns[columns.length - 1] = domainName;
	      String modified = String.join("\t", columns);
	      modifiedLine.set(modified);
	      context.write(modifiedLine, NullWritable.get());
	    }
	  }

	  private String extractDomainName(String url) {
	    int firstSlashIndex = url.indexOf('/');
	    if (firstSlashIndex > 0) {
	    return url.substring(0, firstSlashIndex);
	    }
	    return url;
	  }
	}
		public static class URLReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		  public void reduce(Text key, Iterable<NullWritable> values, Context context)
		    throws IOException, InterruptedException {
		    for (NullWritable value : values) {
		      context.write(key, NullWritable.get());
		    }
		  }
		}
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(Preprocessor.class);
		    job.setMapperClass(URLMapper.class);
		    job.setReducerClass(URLReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(NullWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}

