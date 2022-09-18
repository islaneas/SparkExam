package exercices;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Exercice3 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Exe2 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rddLines=sc.textFile("hdfs://localhost:9000/1763.csv");
        JavaRDD<String> rddRows=rddLines.flatMap(s ->
                Arrays.asList(s.split("\n")).iterator());

        //TMIN(rddRows);
        //TMAX(rddRows);
        //MAXTMAX(rddRows);
        TOPMAX(rddRows);

    }

    public static void TMIN(JavaRDD<String> rddRows){
        JavaRDD<String> rddTMIN=rddRows.filter(s ->s.contains("TMIN"));
        JavaRDD< String> rddTminTemp = rddTMIN.map(s -> s.split(",")[3]);
        JavaRDD< Double> rdddouble= rddTminTemp.map(s ->Double.parseDouble(s));
        Double SumTemp= rdddouble.reduce((x,y) ->x+y);
        SumTemp=SumTemp/rdddouble.count();

        System.out.println(SumTemp.toString());
    }

    static void  TMAX( JavaRDD<String> rddRows){
        JavaRDD<String> rddTMIN=rddRows.filter(s ->s.contains("TMAX"));
        JavaRDD< String> rddTminTemp = rddTMIN.map(s -> s.split(",")[3]);
        JavaRDD< Double> rdddouble= rddTminTemp.map(s ->Double.parseDouble(s));
        Double SumTemp= rdddouble.reduce((x,y) ->x+y);
        SumTemp=SumTemp/rdddouble.count();

        System.out.println(SumTemp.toString());
    }
    static void  MAXTMAX( JavaRDD<String> rddRows){
        JavaRDD<String> rddTMIN=rddRows.filter(s ->s.contains("TMAX"));
        JavaRDD< String> rddTminTemp = rddTMIN.map(s -> s.split(",")[3]);
        JavaRDD< Double> rdddouble= rddTminTemp.map(s ->Double.parseDouble(s));
        Double MaxTemp= rdddouble.reduce((x,y) ->Math.max(x,y));
        System.out.println(MaxTemp.toString());
    }

    static void  MINTMIN( JavaRDD<String> rddRows){
        JavaRDD<String> rddTMIN=rddRows.filter(s ->s.contains("TMIN"));
        JavaRDD< String> rddTminTemp = rddTMIN.map(s -> s.split(",")[3]);
        JavaRDD< Double> rdddouble= rddTminTemp.map(s ->Double.parseDouble(s));
        Double MinTemp= rdddouble.reduce((x,y) ->Math.min(x,y));
        System.out.println(MinTemp.toString());
    }

    static void  TOPMAX( JavaRDD<String> rddRows){
        JavaRDD<String> rddTMIN=rddRows.filter(s ->s.contains("TMAX"));
        JavaPairRDD<Double,String > rddPairs = rddTMIN
                .mapToPair(s -> new Tuple2<>(
                        Double.valueOf(s.split(",")[3]),(s.split(","))[0] ));

        List<Tuple2<Double,String >> el = rddPairs.sortByKey(true).take(5);

        for (Tuple2<Double,String > t : el) {
            System.out.println(t.toString());
        }

    }
}
