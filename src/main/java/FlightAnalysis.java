import static java.lang.Double.parseDouble;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class FlightAnalysis {

  // FlightDataPoint Class to represent each data point
  public static class FlightDataPoint implements Writable {
    private double depDelay, arrDelay, distance, taxiOut;
    private double carrierDelay, weatherDelay, nasDelay, securityDelay, lateAircraftDelay;
    private String airline;
    private String origin;
    private String destination;

    public FlightDataPoint() {}

    public FlightDataPoint(double depDelay, double arrDelay, double distance, double taxiOut,
        double carrierDelay, double weatherDelay, double nasDelay,
        double securityDelay, double lateAircraftDelay,
        String airline, String origin, String destination) {
      this.depDelay = depDelay;
      this.arrDelay = arrDelay;
      this.distance = distance;
      this.taxiOut = taxiOut;
      this.carrierDelay = carrierDelay;
      this.weatherDelay = weatherDelay;
      this.nasDelay = nasDelay;
      this.securityDelay = securityDelay;
      this.lateAircraftDelay = lateAircraftDelay;
      this.airline = airline;
      this.origin = origin;
      this.destination = destination;
    }

    // Serialization methods for Hadoop
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeDouble(depDelay);
      out.writeDouble(arrDelay);
      out.writeDouble(distance);
      out.writeDouble(taxiOut);
      out.writeDouble(carrierDelay);
      out.writeDouble(weatherDelay);
      out.writeDouble(nasDelay);
      out.writeDouble(securityDelay);
      out.writeDouble(lateAircraftDelay);
      out.writeUTF(airline);
      out.writeUTF(origin);
      out.writeUTF(destination);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      depDelay = in.readDouble();
      arrDelay = in.readDouble();
      distance = in.readDouble();
      taxiOut = in.readDouble();
      carrierDelay = in.readDouble();
      weatherDelay = in.readDouble();
      nasDelay = in.readDouble();
      securityDelay = in.readDouble();
      lateAircraftDelay = in.readDouble();
      airline = in.readUTF();
      origin = in.readUTF();
      destination = in.readUTF();
    }

    // Getters for fields
    public double getDepDelay() { return depDelay; }
    public double getArrDelay() { return arrDelay; }
    public String getAirline() { return airline; }
    public String getOrigin() { return origin; }
    public String getDestination() { return destination; }
    public double getCarrierDelay() { return carrierDelay; }
    public double getWeatherDelay() { return weatherDelay; }
    public double getNasDelay() { return nasDelay; }
    public double getSecurityDelay() { return securityDelay; }
    public double getLateAircraftDelay() { return lateAircraftDelay; }
  }

  public static class Centroid {
    private int id;

    public Centroid(int id) {
      this.id = id;
    }

    public int getId() { return id; }

    public double calculateEuclideanDistance(FlightDataPoint point) {
      //dummy distance for now.

      return 1;
    }

  }

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
      if (fields.length > 28) {
//        String year = fields[0];
//        String month = fields[1];
        double depDelay = parseDouble(fields[9]);
        double arrDelay = parseDouble(fields[17]);
        double distance = parseDouble(fields[27]);
        double taxiOut = parseDouble(fields[13]);
        double carrierDelay = parseDouble(fields[28]);
        double weatherDelay = parseDouble(fields[29]);
        double nasDelay = parseDouble(fields[30]);
        double securityDelay = parseDouble(fields[31]);
        double lateAircraftDelay = parseDouble(fields[32]);
        String airline = fields[4];
        String origin = fields[5];
        String destination = fields[6];

        FlightDataPoint point = new FlightDataPoint(depDelay, arrDelay, distance, taxiOut, carrierDelay,
            weatherDelay, nasDelay, securityDelay, lateAircraftDelay,
            airline, origin, destination);

        String key_context = point.getAirline() + "-" + point.getOrigin() + "-" + point.getDestination();

        keyOutput.set(key_context);
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
