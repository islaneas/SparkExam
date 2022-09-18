package exercices;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Exercice2_2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Exe2 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rddLines=sc.textFile("hdfs://localhost:9000/villes.txt");
        JavaRDD<String> rddRows=rddLines.flatMap(s ->
                Arrays.asList(s.split("\n")).iterator());

        // string annee  13/12/2022
        String annee="2022";
        JavaRDD<String> rdddates=rddRows.filter(s ->s.contains(annee));
        List<String> elems = rddRows.collect();
        for (String t : elems) {
            System.out.println(t.toString());
        }
        JavaPairRDD<String, Double> rddPairs = rdddates
                .mapToPair(s -> new Tuple2<>((s.split(" "))[1],
                        Double.valueOf(s.split(" ")[3]) ));
        JavaPairRDD<String,Double> ventecount=rddPairs.reduceByKey((a, b) -> a+b);
        // calcul en local
        List<Tuple2<String, Double>>   el = ventecount.collect();
        for (Tuple2<String, Double> t : el) {
            System.out.println(t.toString());
        }
        // calcul sur le cluster
        ventecount.saveAsTextFile("hdfs://localhost:9000/venteAnnee.txt");

    }
}
