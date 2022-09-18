package exercices;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

public class exercice1 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Exerc1").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.parallelize(
                Arrays.asList("asmae islane","jomana imane sara",
                        "amina rajae aya","nassima joudia najib",
                        "jad hamza joud","rania aya",
                        "sara aya asmae nassima"));

        JavaRDD<String> rdd2=rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        /*for (String t: rdd2.collect()) {
            System.out.println(t);
        }*/


        JavaRDD<String> rdd3 = rdd2.filter(s -> s.startsWith("jo", 0));
        /*for (String t: rdd3.collect()) {
            System.out.println(t);
        }*/

        JavaRDD<String> rdd4 = rdd2.filter(s -> s.length() == 4);
        /*for (String t: rdd4.collect()) {
            System.out.println(t);
        }*/

        JavaRDD<String> rdd5 = rdd2.filter(s -> s.endsWith("a"));
        /*for (String t: rdd5.collect()) {
            System.out.println(t);
        }*/

        JavaRDD<String> rdd6 = rdd3.union(rdd4);
        System.out.println(rdd6.count());

        JavaPairRDD<String, Integer> rdd71 = rdd5.mapToPair(s -> new Tuple2<>(s, 1));

        List<Tuple2<String, Integer>> elems = rdd71.collect();
      /*  for (Tuple2<String, Integer> t : elems) {
            System.out.println(t.toString());
        }*/

        JavaPairRDD<String, Integer> rdd81 = rdd6.mapToPair(s -> new Tuple2<>(s, 1));
        /*elems = rdd81.collect();
        for (Tuple2<String, Integer> t : elems) {
            System.out.println(t.toString());
        }*/
        JavaPairRDD<String, Integer> rdd7=rdd71.reduceByKey((a, b) -> a+b);
       /* elems=rdd7.collect();
        for (Tuple2<String,Integer> t:elems) {
            System.out.println(t.toString());
        }*/
        JavaPairRDD<String, Integer> rdd8=rdd81.reduceByKey((a, b) -> a+b);
       /* elems=rdd8.collect();
        for (Tuple2<String,Integer> t:elems) {
            System.out.println(t.toString());
        }*/
        JavaPairRDD<String, Integer> rdd9 = rdd7.union(rdd8);
        elems=rdd9.collect();
        for (Tuple2<String,Integer> t:elems) {
            System.out.println(t.toString());
        }
        JavaPairRDD<String, Integer> rdd10 = rdd9.sortByKey();
        elems=rdd10.collect();
        for (Tuple2<String,Integer> t:elems) {
            System.out.println(t.toString());
    }
    }
}
