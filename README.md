# data-processing-final
Final Project Data Processing

# Setup Maven

Prerequisite:
- IntelliJ

```shell
git clone git@github.com:jgautama/data-processing-final.git
cd final-project
mvn install
```

# Running in AWS EMR

To run in AWS EMR, you need to package the program to a `.jar` file. This project use maven to manage
packaging the dependencies.
```shell
mvn clean package
```
the output file name is located in the newly generated `target/` folder with the following name
`<artifact_id>-<version>.jar`

Next, upload the file to your S3 bucket. you can use AWS CLI or sign in to your AWS console and upload
via the GUI.
```shell
aws s3 cp target/<artifact_id>-<version>.jar s3://<bucket_name>/<artifact_id>-<version>.jar
```

TODO: add instructions below to setup EMR Steps

# Project Information
Analysis 1 - Flight Route Clustering (K-Means):
- Analysis Task: Cluster flight routes based on attributes such as distance, delay
patterns, and traffic volume to identify high-risk routes prone to delays.
- Main Task: Use distributed K-means clustering with MapReduce to efficiently partition
flight routes into clusters. This will help categorize routes that show similar delay
characteristics. - we are currently using 3 clusters
- Helper Task: Experiment with different values of K to determine the optimal number of
clusters. Perform feature selection to ensure the most relevant attributes are used for
clustering. - the optimal seems to be 3-4 clusters
- Date and Time Information: `YEAR, MONTH, DAY_OF_MONTH, FL_DATE,
CRS_DEP_TIME, DEP_TIME, CRS_ARR_TIME, ARR_TIME`
- Airline and Route Information: `OP_UNIQUE_CARRIER, ORIGIN_AIRPORT_ID,
DEST_AIRPORT_ID`
- Delay Information: DEP_DELAY, `ARR_DELAY, DEP_DELAY_NEW, ARR_DELAY_NEW,
DEP_DELAY_GRsOUP, ARR_DELAY_GROUP`, along with various specific delay causes
such as `CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, and
LATE_AIRCRAFT_DELAY`
- Operational Metrics: `TAXI_OUT, TAXI_IN, AIR_TIME, FLIGHTS, DISTANCE,
FIRST_DEP_TIME, TOTAL_ADD_GTIME`

## Dataset
[Bureau of Transportation Statistics](https://www.transtats.bts.gov/ot_delay/ot_delaycause1.asp)

## File Input Headers
The detailed list of available headers can be found at the [BTS website](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ),

The current downloaded file has the following columns:

|   |                   |    |                   |    |                     |
|---|-------------------|----|-------------------|----|---------------------|
| 0 | YEAR              | 12 | DEP_TIME_BLK      | 24 | CRS_ELAPSED_TIME    |
| 1 | MONTH             | 13 | TAXI_OUT          | 25 | AIR_TIME            |
| 2 | DAY_OF_MONTH      | 14 | TAXI_IN           | 26 | FLIGHTS             |
| 3 | FL_DATE           | 15 | CRS_ARR_TIME      | 27 | DISTANCE            |
| 4 | OP_UNIQUE_CARRIER | 16 | ARR_TIME          | 28 | CARRIER_DELAY       |
| 5 | ORIGIN_AIRPORT_ID | 17 | ARR_DELAY         | 29 | WEATHER_DELAY       |
| 6 | DEST_AIRPORT_ID   | 18 | ARR_DELAY_NEW     | 30 | NAS_DELAY           |
| 7 | CRS_DEP_TIME      | 19 | ARR_DELAY_GROUP   | 31 | SECURITY_DELAY      |
| 8 | DEP_TIME          | 20 | ARR_TIME_BLK      | 32 | LATE_AIRCRAFT_DELAY |
| 9 | DEP_DELAY         | 21 | CANCELLED         | 33 | FIRST_DEP_TIME      |
| 10 | DEP_DELAY_NEW     | 22 | CANCELLATION_CODE | 34 | TOTAL_ADD_GTIME     |
| 11 | DEP_DELAY_GROUP   | 23 | DIVERTED          |    |                     |

As for the `ORIGIN_AIRPORT_ID` and `DEST_AIRPORT_ID`, we need a lookup table that maps to the airport name.
This can be found at [resources](src/main/resources/) folder. 

## Sample Output

### version 0.2.3
[refer to output for version 0.2.3](./output/depDelay-and-airTime-v0.2.3)
```
// version 0.2.3
//Output format: <OP_UNIQUE_CARRIER>-<DEP_DELAY>-<AIR_TIME>   <clusterID>
//note: negative value for DEP_DELAY value means the airline arrives early / ahead of schedule.
// TODO | negative AIR_TIME value is currently not know why it is negative.
YX--10.0--1.0	0
YX--8.0--1.0	0
YX-25.0-1.0	0
YX--2.0--1.0	0
YX--10.0--1.0	0
YX--9.0--1.0	0
YX-8.0-0.0	0
...
F9-99.0-6.0	1
B6-82.0-5.0	1
WN-136.0-9.0	1
AA-185.0-12.0	1
OO-107.0-7.0	1
NK-76.0-5.0	1
AA-135.0-9.0	1
NK-171.0-11.0	1
...
```
### version 0.2.2
[refer to output for version 0.2.2](./output/depDelay-v0.2.2)
```text
// version 0.2.2
//Output format: <OP_UNIQUE_CARRIER>-<DEP_DELAY>   <clusterID>
//note: negative value for DEP_DELAY value means the airline arrives early / ahead of schedule.
YX,-10.00,0
YX,-8.00,0
YX,25.00,0
YX,-2.00,0
YX,-10.00,0
YX,-9.00,0
YX,8.00,0
YX,-4.00,0
YX,42.00,0
YX,-8.00,0
...
NK,80.00,1
YX,66.00,1
NK,60.00,1
AA,61.00,1
WN,81.00,1
NK,76.00,1
NK,63.00,1
AS,70.00,1
OO,65.00,1
WN,67.00,1
WN,66.00,1
WN,77.00,1
YX,70.00,1
...
WN,91.00,2
B6,206.00,2
OO,129.00,2
AA,176.00,2
AA,136.00,2
B6,139.00,2
B6,92.00,2
AA,96.00,2
AA,489.00,2
UA,195.00,2
AA,156.00,2
OO,95.00,2
AA,133.00,2
NK,93.00,2
OH,343.00,2
...
```

