package com.fbytes.java.examples.veeva;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SparkVeeva {
    private static Logger log = Logger.getLogger(SparkVeeva.class.toString());

    public static void main(String[] args) throws Exception {
//        String log4jConfigFile = "src/main/resources/log4j.xml";
//        ConfigurationSource source = new ConfigurationSource(new FileInputStream(log4jConfigFile));
//        Configurator.initialize(null, source);
//        SparkConf conf = new SparkConf().setAppName("spark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        System.setProperty("hadoop.home.dir", "c:/hadoop");
//        org.apache.log4j.Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//        JavaRDD<String> t = sc.textFile("streams/src/main/resources/test_keys.csv");

        SparkSession spark = SparkSession.builder().appName("KafkaTopCourse Application").
                config("spark.master", "local").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        javaSparkContext.sc().setLogLevel("WARN");
        org.apache.log4j.Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaRDD<String> t = javaSparkContext.textFile("streams/src/main/resources/test_keys.csv");

        JavaPairRDD<List<String>, List<String>> in = t.mapToPair(line -> {
            String[] parts = line.split(",");
            return new Tuple2<>(Arrays.asList(parts[0].split("-")), Arrays.asList(parts[1].split(" ")));
        });

        JavaPairRDD<List<String>, List<String>> out = groupWords(in);
        logRDD(out, "Output:");

        JavaPairRDD<List<String>, List<String>> out1 = removeUnmatched(out);
        logRDD(out1, "Output1:");

        JavaPairRDD<List<String>, List<String>> out2 = removeUnmatched(groupWords(out1));
        logRDD(out2, "Output2:");

        spark.close();

    }

    static public JavaPairRDD<List<String>, List<String>> removeUnmatched(JavaPairRDD<List<String>, List<String>> in){
        JavaPairRDD<List<String>, List<String>> out1 = in.flatMapToPair(tuple -> {
                    List<Tuple2<String, Tuple2<List<String>, List<String>> >> merge = new LinkedList<>();
                    tuple._1().forEach(id -> merge.add(new Tuple2(id, new Tuple2<>(tuple._1(), tuple._2()))));
                    return  merge.iterator();
                }).reduceByKey((i1, i2) -> {
                    if (i1._1().size()>i2._1().size()){
                        return i1;
                    }
                    else{
                        return i2;
                    }
                }).mapToPair(tuple -> new Tuple2<>(tuple._2()._1(), tuple._2()._2()))
                .distinct();
        return out1;
    }


    static public JavaPairRDD<List<String>, List<String>> groupWords(JavaPairRDD<List<String>, List<String>> in) {
        //logRDD(in, "Input:");
        JavaPairRDD<String, Tuple2<Set<String>, Set<String>>> rdd = in.flatMapToPair(tuple -> {
            List<Tuple2<String, Tuple2<Set<String>, Set<String>>>> lst = new LinkedList<>();
            Set<String> ids = new HashSet<>();
            ids.addAll(tuple._1());
            Set<String> words = new HashSet<>();
            words.addAll(tuple._2());
            tuple._2().forEach(word -> lst.add(new Tuple2<String, Tuple2<Set<String>, Set<String>>>(word, new Tuple2<>(ids, words))));
            return lst.iterator();
        });
        //logRDD(rdd, "rdd");      //    <word, <[ids][words]>>

        JavaPairRDD<String, Tuple2<Set<String>, Set<String>>> rdd2 = rdd.reduceByKey((i1, i2) -> {
            Set<String> wordsList = new HashSet<>();
            wordsList.addAll(i1._2());
            wordsList.addAll(i2._2());
            Set<String> ids = new HashSet<>();
            ids.addAll(i1._1());
            ids.addAll(i2._1());
            return new Tuple2<Set<String>, Set<String>>(ids, wordsList);
        });
        //logRDD(rdd2, "rdd2");     // <word, <[ids][words]>>

        JavaPairRDD<List<String>, List<String>> rdd3 = rdd2.mapToPair(tuple -> new Tuple2<>(tuple._2()._1().stream().toList(),
                tuple._2()._2().stream().toList()));
        //logRDD(rdd3, "Output:");

        return rdd3;
    }

    public static <K,V> void logRDD(JavaPairRDD<K,V> rdd, String header){
        System.out.println(header);
        rdd.foreach(item -> {
            System.out.println(item._1() + "=" + item._2());
        });
        System.out.println("\n\n");
    }
}