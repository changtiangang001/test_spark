package cn._51doit.spark.day01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class LambdaWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("LambdaWordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //通过JavaSparkContext创建JavaRDD
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.stream(line.split(" ")).iterator());
        //将单词和一组合
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> Tuple2.apply(w, 1));
        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((i, j) -> i + j);
        //排序
        JavaPairRDD<String, Integer> sorted = reduced.mapToPair(tp -> tp.swap())
                .sortByKey(false)
                .mapToPair(tp -> tp.swap());

        //保存到HDFS
        sorted.saveAsTextFile(args[1]);

        //释放资源
        jsc.stop();
    }
}
