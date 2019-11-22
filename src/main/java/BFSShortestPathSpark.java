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

            if(vertex.equals(srcVertex)) {
                data.status = "ACTIVE";
                data.distance = 0;
            }

            return new Tuple2<>(vertex, data);
        });

        System.out.println("....... Size ......... " + network.count());

        JavaPairRDD<String, Data> activeVertices = network.filter(vertex -> vertex._2.status == "ACTIVE");

        Map<String, Data> originalNetwork = network.collectAsMap();

//        originalNetwork.forEach((key, value) -> {
//            for(Tuple2<String, Integer> neighbor : value.neighbors) {
//                System.out.println("Vertex " + key + " Neighbor " + neighbor._1 + " distance " + neighbor._2);
//            }
//        });

        System.out.println("....... Size ......... " + activeVertices.count());


        while(activeVertices.count() > 0) {

            System.out.println("Entered the loop ....");

            JavaPairRDD<String, Data> propagatedNetwork = network.flatMapToPair(vertex -> {
                // If a vertex is “ACTIVE”, create Tuple2( neighbor, new Data( … ) ) for
                // each neighbor where Data should include a new distance to this neighbor.
                // Add each Tuple2 to a list. Don’t forget this vertex itself back to the
                // list. Return all the list items.

                List<Tuple2<String, Data>> propagatedVertices = new ArrayList<>();
                System.out.println(" ...Active vertex outside " + vertex._1);

//                propagatedVertices.add(vertex);
//                return propagatedVertices.iterator();
//
                if(vertex._2.status.equals("ACTIVE")) {

                   // System.out.println(" ...Active vertex  " + vertex._1);

                    for(Tuple2<String, Integer> neighbor : vertex._2.neighbors) {

//                        if(neighbor._1.equals(srcVertex)) {
//                            continue;
//                        }

                       Data data = originalNetwork.get(neighbor._1);

                       if(data != null) {
                           Data newData = new Data(data.neighbors, neighbor._2 + vertex._2.distance, data.prev, data.status);
                           propagatedVertices.add(new Tuple2<>(neighbor._1, newData));
                       }

                        //Data newData = new Data(null, neighbor._2 + vertex._2.distance, Integer.MAX_VALUE, "INACTIVE");
                        //propagatedVertices.add(new Tuple2<>(neighbor._1, newData));
                    }

                }

                propagatedVertices.add(new Tuple2<>(vertex._1, vertex._2));
                return propagatedVertices.iterator();
            });


            //System.out.println("size...&&&& " + propagatedNetwork.count());

           // propagatedNetwork.collect().forEach(vertex -> System.out.println("key  *** " + vertex._1 + " " + vertex._2.distance));


            network = propagatedNetwork.reduceByKey((k1, k2) -> {
                // For each key, (i.e., each vertex), find the shortest distance and
                // update this vertex’ Data attribute.
                Data data = new Data();

//                if(k1.neighbors != null && k1.neighbors.size() > 0) {
//                    data.neighbors = k1.neighbors;
//                } else {
//                    data.neighbors = k2.neighbors;
//                }

                if(k1.distance > k2.distance) {
                   return k2;
                } else {
                   return k1;
                }
            });


            network = network.mapValues(value -> {
                // If a vertex’ new distance is shorter than prev, activate this vertex
                // status and replace prev with the new distance.

                if(value.distance < value.prev) {
                    value.prev = value.distance;
                    value.status = "ACTIVE";
                } else  {
                    value.status = "INACTIVE";
                }

                return value;
            });

            //System.out.println(" Size ... *****  " + network.count());

            activeVertices = network.filter(vertex -> vertex._2.status == "ACTIVE");
            //break;

        }
//
//        System.out.println(" Size ... propogated " + propagatedNetwork.count());

        //propagatedNetwork.collect().forEach(System.out::println);



      //  jsc.stop();

       network.collect().forEach(vertex -> System.out.println(vertex._1 + " " + vertex._2.distance));
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
