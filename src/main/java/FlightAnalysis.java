import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
    private List<double[]> centroids = new ArrayList<>(); // Store centroids as pairs of (DEP_DELAY, AIR_TIME)

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      for (int i = 0; i < 3; i++) {
        // Load centroids from configuration as pairs (DEP_DELAY, AIR_TIME)
        double depDelay = Double.parseDouble(conf.get("centroid." + i + ".depDelay"));
        double airTime = Double.parseDouble(conf.get("centroid." + i + ".airTime"));
        centroids.add(new double[]{depDelay, airTime});
      }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] columns = value.toString().split(",");
      try {
        double depDelay = Double.parseDouble(columns[9]); // DEP_DELAY is column 9
        double airTime = Double.parseDouble(columns[11]); // AIR_TIME is column 11

        int clusterId = getClosestCentroid(depDelay, airTime, centroids); // Get the closest centroid
        context.write(new IntWritable(clusterId), value); // Emit the data point with assigned cluster ID
      } catch (NumberFormatException e) {
        // If there's an error parsing DEP_DELAY or AIR_TIME, ignore the record
      }
    }

    private int getClosestCentroid(double depDelay, double airTime, List<double[]> centroids) {
      double minDistance = Double.MAX_VALUE;
      int clusterId = -1;
      for (int i = 0; i < centroids.size(); i++) {
        double[] centroid = centroids.get(i);
        // Euclidean distance calculation between the point and centroid
        double distance = Math.sqrt(Math.pow(depDelay - centroid[0], 2) + Math.pow(airTime - centroid[1], 2));
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
          // Extract OP_UNIQUE_CARRIER from index 4, DEP_DELAY from index 9, and AIR_TIME from index 11
          String carrier = columns[4];  // OP_UNIQUE_CARRIER is at index 4
          double depDelay = Double.parseDouble(columns[9]);  // DEP_DELAY is at index 9
          double airTime = Double.parseDouble(columns[11]);  // AIR_TIME is at index 11

          // Create a new key in the format OP_UNIQUE_CARRIER-DEP_DELAY-AIR_TIME
          String carrierDepDelayAirTimeKey = carrier + "-" + depDelay + "-" + airTime;

          // Write the output: key is OP_UNIQUE_CARRIER-DEP_DELAY-AIR_TIME, value is the cluster ID
          context.write(new Text(carrierDepDelayAirTimeKey), key);  // key is the cluster ID (IntWritable)

        } catch (Exception e) {
          // In case of any parsing errors, skip this record
          // (e.g., if DEP_DELAY or AIR_TIME is missing or improperly formatted)
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // Initialize random centroids between 0 and 100 for both DEP_DELAY and AIR_TIME
    Random rand = new Random();
    for (int i = 0; i < 3; i++) {
      conf.set("centroid." + i + ".depDelay", String.valueOf(rand.nextDouble() * 100));
      conf.set("centroid." + i + ".airTime", String.valueOf(rand.nextDouble() * 100));
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
