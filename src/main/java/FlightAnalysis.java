import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FlightAnalysis {

  // Mapper class that assigns each data point to the nearest centroid
  public static class FlightMapper extends Mapper<Object, Text, IntWritable, Text> {
    private List<Double> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      for (int i = 0; i < 3; i++) {
        centroids.add(Double.parseDouble(conf.get("centroid." + i))); // Load centroids from configuration
      }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] columns = value.toString().split(",");
      try {
        double depDelay = Double.parseDouble(columns[9]); // DEP_DELAY is column 9
        int clusterId = getClosestCentroid(depDelay, centroids); // Get the closest centroid
        context.write(new IntWritable(clusterId), value); // Emit the data point with assigned cluster ID
      } catch (NumberFormatException e) {
        // If there's an error parsing DEP_DELAY, ignore the record
      }
    }

    private int getClosestCentroid(double depDelay, List<Double> centroids) {
      double minDistance = Double.MAX_VALUE;
      int clusterId = -1;
      for (int i = 0; i < centroids.size(); i++) {
        double distance = Math.abs(depDelay - centroids.get(i));
        if (distance < minDistance) {
          minDistance = distance;
          clusterId = i;
        }
      }
      return clusterId;
    }
  }

  public static class FlightReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      // Key is the cluster ID assigned by the mapper
      // Values are all the lines belonging to that cluster

      for (Text val : values) {
        String[] columns = val.toString().split(","); // Split the input line by commas
        try {
          // Extract OP_UNIQUE_CARRIER from index 4 and DEP_DELAY from index 9
          String carrier = columns[4];  // OP_UNIQUE_CARRIER is at index 4
          double depDelay = Double.parseDouble(columns[9]);  // DEP_DELAY is at index 9

          // Create a new key in the format OP_UNIQUE_CARRIER-DEP_DELAY
          String carrierDepDelayKey = carrier + "-" + depDelay;

          // Write the output: key is OP_UNIQUE_CARRIER-DEP_DELAY, value is the cluster ID
          context.write(new Text(carrierDepDelayKey), key);  // key is the cluster ID (IntWritable)

        } catch (Exception e) {
          // In case of any parsing errors, skip this record
          // (e.g., if DEP_DELAY is missing or improperly formatted)
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // Initialize random centroids between 0 and 100
    Random rand = new Random();
    for (int i = 0; i < 3; i++) {
      conf.set("centroid." + i, String.valueOf(rand.nextDouble() * 100));
    }

    Job job = Job.getInstance(conf, "Flight Analysis - Assigning Clusters");
    job.setJarByClass(FlightAnalysis.class);

    job.setMapperClass(FlightMapper.class);
    job.setReducerClass(FlightReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}