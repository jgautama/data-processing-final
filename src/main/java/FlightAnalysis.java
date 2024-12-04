import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class FlightAnalysis {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: FlightAnalysis <input path> <output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Flight Analysis");

    job.setJarByClass(FlightAnalysis.class);
    job.setMapperClass(FlightMapper.class);
    job.setReducerClass(FlightReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class FlightMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOutput = new Text();
    private Text one = new Text("1");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      // Skip header line
      if (line.startsWith("YEAR")) {
        return;
      }

      String[] fields = line.split(",");
      if (fields.length > 7) {
        String year = fields[0];
        String month = fields[1];
        String origin = fields[5];
        String destination = fields[6];
        String yearMonthKey = year + "-" + month + "/" + origin + "-" + destination;
        keyOutput.set(yearMonthKey);
        context.write(keyOutput, one);
      }
    }
  }

  public static class FlightReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (Text val : values) {
        sum += Integer.parseInt(val.toString());
      }
      result.set(String.valueOf(sum));
      context.write(key, result);
    }
  }

}
