import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.*;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;


public class BFSShortestPathSpark {

    public static void main(String args[]) {

        String inputFile = args[0];
        SparkConf conf = new SparkConf().setAppName("BFS-based Shortest Path Search");
        JavaSparkContext jsc = new JavaSparkContext(conf);
            JavaRDD<String> lines = jsc.textFile(inputFile);

        final String srcVertex = args[1];
        String destVertex = args[2];

        // now start a timer
        long startTime = System.currentTimeMillis();

        // Constructing Vertex and DataObject pairs.
        JavaPairRDD<String, Data> network = lines.mapToPair(line -> {
            String[] info = line.split("=");
            String[] neighborInfo = info[1].split(";");
            String vertex = info[0];

            List<Tuple2<String,Integer>> neighbors = new ArrayList<>();

            for(String neighbor : neighborInfo) {
                neighbors.add(new Tuple2<>(neighbor.split(",")[0],
                        Integer.parseInt(neighbor.split(",")[1])));
            }

            Data data = new Data();
            data.neighbors = neighbors;

            // Only activating the source vertex and marking the distance as zero.
            if(vertex.equals(srcVertex)) {
                data.status = "ACTIVE";
                data.distance = 0;
            }

            return new Tuple2<>(vertex, data);
        });

        // Using filter transformation to get active vertices in the network.
        JavaPairRDD<String, Data> activeVertices = network.filter(vertex -> vertex._2.status.equals("ACTIVE"));

        while(activeVertices.count() > 0) {

            JavaPairRDD<String, Data> propagatedNetwork = network.flatMapToPair(vertex -> {
                // If a vertex is “ACTIVE”, create Tuple2( neighbor, new Data( … ) ) for
                // each neighbor where Data should include a new distance to this neighbor.
                // Add each Tuple2 to a list. Don’t forget this vertex itself back to the
                // list. Return all the list items.

                List<Tuple2<String, Data>> propagatedVertices = new ArrayList<>();

                if(vertex._2.status.equals("ACTIVE")) {

                    for(Tuple2<String, Integer> neighbor : vertex._2.neighbors) {
                           Data newData = new Data(null, neighbor._2 + vertex._2.distance, Integer.MAX_VALUE, "INACTIVE");
                           propagatedVertices.add(new Tuple2<>(neighbor._1, newData));
                    }

                }

                propagatedVertices.add(vertex);
                return propagatedVertices.iterator();
            });

            network = propagatedNetwork.reduceByKey((k1, k2) -> {
                // For each key, (i.e., each vertex), find the shortest distance and
                // update this vertex’ Data attribute.

                if(k1.distance > k2.distance) {

                    // In case the neighbors are empty for the shorter distance DataNode, we are
                    // replacing with original DataNode neighbors.
                    if(!k1.neighbors.isEmpty()) {
                        k2.neighbors = k1.neighbors;
                    }

                    return k2;
                } else {

                    // In case the neighbors are empty for the shorter distance DataNode, we are
                    // replacing with original DataNode neighbors.
                    if(!k2.neighbors.isEmpty()) {
                        k1.neighbors = k2.neighbors;
                    }

                    return k1;
                }
            });


            network = network.mapValues(value -> {
                // If a vertex’ new distance is shorter than prev, activate this vertex
                // status and replace prev with the new distance.

                // For all other vertices we will mark vertex as Inactive.

                if(value.distance < value.prev) {
                    value.prev = value.distance;
                    value.status = "ACTIVE";
                } else  {
                    value.status = "INACTIVE";
                }

                return value;
            });

            // Using filter transformation to get active vertices in the network.
           activeVertices = network.filter(vertex -> vertex._2.status.equals("ACTIVE"));
        }

        // stopping the timer.
        long stopTime = System.currentTimeMillis();
        System.err.println("Execution time in milli secs " + (stopTime - startTime));

        // Filtering the destination vertex and printing the result.
        network.collect().forEach(vertex -> {
            if(vertex._1.equals(destVertex)) {
                System.err.println("From " + srcVertex + " to " + destVertex + " takes distance = " + vertex._2.distance);
            }
        });

        // Stopping the spark context.
        jsc.stop();
    }
}

class Data implements Serializable {
    List<Tuple2<String,Integer>> neighbors; // <neighbor0, weight0>, ...
    String status; // "INACTIVE" or "ACTIVE"
    Integer distance; // the distance so far from source to this vertex
    Integer prev; // the distance calculated in the previous iteration

    public Data() {
        neighbors = new ArrayList<>();
        status = "INACTIVE";
        distance = Integer.MAX_VALUE;
        prev = Integer.MAX_VALUE;
    }

    public Data(List<Tuple2<String,Integer>> neighbors,
                Integer dist, Integer prev, String status ){
        if ( neighbors != null ) {
            this.neighbors = new ArrayList<Tuple2<String,Integer>>( neighbors );
        } else {
            this.neighbors = new ArrayList<Tuple2<String,Integer>>( );
        }
        this.distance = dist;
        this.prev = prev;
        this.status = status;
    }
}
