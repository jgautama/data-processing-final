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
  public static class FlightReducer extends Reducer<IntWritable, Text, Text, Text> {
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

    // Reduce function to calculate the new centroids for each cluster
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      double sum = 0.0;
      int count = 0;

      // Sum up all the DEP_DELAY values for the current cluster
      for (Text value : values) {
        String[] columns = value.toString().split(",");
        double depDelay = Double.parseDouble(columns[9]); // DEP_DELAY is column 9
        sum += depDelay;
        count++;
      }

      // Calculate the new centroid (average of the data points in the cluster)
      double newCentroid = count == 0 ? 0.0 : sum / count;
      newCentroids.add(newCentroid); // Store the new centroid

      // Output the data point along with the cluster ID
      for (Text value : values) {
        context.write(new Text(value.toString()), new Text(String.valueOf(key.get())));
      }
    }

    // Cleanup method to check if the centroids have stabilized
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      boolean centroidsChanged = false;

      // Compare new centroids with old centroids to check for stabilization
      for (int i = 0; i < oldCentroids.size(); i++) {
        if (!oldCentroids.get(i).equals(newCentroids.get(i))) {
          centroidsChanged = true; // If any centroid has changed, mark as changed
          break;
        }
      }

      // If centroids have changed, update them in the configuration for the next iteration
      if (centroidsChanged) {
        for (int i = 0; i < newCentroids.size(); i++) {
          conf.set("centroid." + i, String.valueOf(newCentroids.get(i))); // Save updated centroids
        }
      }

      // Output the final centroids as the last line in the output
      for (int i = 0; i < newCentroids.size(); i++) {
        context.write(new Text("Final Centroid " + i), new Text(String.valueOf(newCentroids.get(i))));
      }
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
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Set input and output paths for the job (provided as command-line arguments)
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Run the job and wait for completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}