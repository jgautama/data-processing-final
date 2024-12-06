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

    // Set up the centroids from the configuration (passed in from the reducer)
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      for (int i = 0; i < 3; i++) {
        centroids.add(Double.parseDouble(conf.get("centroid." + i))); // Load centroids from configuration
      }
    }

    // Main map function to assign each flight data to a cluster
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] columns = value.toString().split(",");
      try {
        double depDelay = Double.parseDouble(columns[9]); // DEP_DELAY is column 9 (index 9)
        int clusterId = getClosestCentroid(depDelay, centroids); // Get the closest centroid
        context.write(new IntWritable(clusterId), value); // Emit the data point with assigned cluster ID
      } catch (NumberFormatException e) {
        // If there's an error parsing DEP_DELAY, ignore the record (skip missing or malformed values)
      }
    }

    // Calculate the closest centroid to the current data point
    private int getClosestCentroid(double depDelay, List<Double> centroids) {
      double minDistance = Double.MAX_VALUE;
      int clusterId = -1;
      for (int i = 0; i < centroids.size(); i++) {
        double distance = Math.abs(depDelay - centroids.get(i)); // Calculate Euclidean distance
        if (distance < minDistance) {
          minDistance = distance; // Update closest centroid
          clusterId = i; // Assign cluster ID based on closest centroid
        }
      }
      return clusterId;
    }
  }

  // Reducer class that computes new centroids and outputs data points with cluster IDs
  public static class FlightReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    private List<Double> newCentroids = new ArrayList<>();
    private List<Double> oldCentroids = new ArrayList<>();

    // Setup method to initialize old centroids for comparison
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      for (int i = 0; i < 3; i++) {
        oldCentroids.add(Double.parseDouble(conf.get("centroid." + i))); // Load centroids from configuration
      }
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<Double> clusterData = new ArrayList<>();
      double sum = 0;
      int count = 0;
      String carrier = "";

      // Iterate over all values (data points) for the current key (cluster ID)
      for (Text val : values) {
        String[] columns = val.toString().split(",");
        carrier = columns[4]; // OP_UNIQUE_CARRIER at index 4
        double depDelay = Double.parseDouble(columns[9]); // DEP_DELAY at index 9

        // Add the DEP_DELAY to the sum
        sum += depDelay;
        count++;

        // Add the carrier and depDelay to the cluster data for final output
        // String carrierDepDelayKey = carrier + "-" + depDelay;  // OP_UNIQUE_CARRIER-DEP_DELAY format
        clusterData.add(depDelay);
      }

      // Calculate the new centroid for the cluster
      double newCentroid = sum / count;

      // Determine the cluster ID based on the closest centroid (0, 1, or 2)
      int clusterId = 0;
      double minDistance = Double.MAX_VALUE;
      for (int i = 0; i < newCentroids.size(); i++) {
        double centroid = newCentroids.get(i);
        double distance = Math.abs(newCentroid - centroid); // Default Euclidean distance
        if (distance < minDistance) {
          minDistance = distance;
          clusterId = i;
        }
      }

      // Write the final output: key is OP_UNIQUE_CARRIER-DEP_DELAY, value is the cluster ID (as IntWritable)
      // Using the first data point's carrier-DEP_DELAY key from the clusterData
      if (!clusterData.isEmpty()) {
        String carrierDepDelayKey = carrier + "-" + clusterData.get(0); // OP_UNIQUE_CARRIER-DEP_DELAY format
        context.write(new Text(carrierDepDelayKey), new IntWritable(clusterId)); // Emit carrier and depDelay as key, clusterId as value
      }

      // Update the centroids with the new centroid for the next iteration
      newCentroids.add(newCentroid);
    }
  }

  // Main driver class to set up and run the MapReduce job
  public static void main(String[] args) throws Exception {
    // Configuration object to hold job settings and centroid values
    Configuration conf = new Configuration();

    // Initialize random centroids between 0 and 100
    Random rand = new Random();
    for (int i = 0; i < 3; i++) {
      conf.set("centroid." + i, String.valueOf(rand.nextDouble() * 100)); // Set initial random centroids
    }

    // Create a new Hadoop job and set basic configurations
    Job job = Job.getInstance(conf, "Flight Analysis - KMeans Clustering");
    job.setJarByClass(FlightAnalysis.class);

    // Set the Mapper and Reducer classes
    job.setMapperClass(FlightMapper.class);
    job.setReducerClass(FlightReducer.class);

    // Set output key and value types for the map phase
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    // Set output key and value types for the reduce phase
    job.setOutputKeyClass(Text.class);  // Key is OP_UNIQUE_CARRIER-DEP_DELAY (Text)
    job.setOutputValueClass(IntWritable.class); // Value is the cluster ID (IntWritable)

    // Set input and output paths for the job (provided as command-line arguments)
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Run the job and wait for completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
