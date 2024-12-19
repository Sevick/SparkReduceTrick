package com.fbytes.java.examples.veeva;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;

public class SparkTrick1 {

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

        JavaRDD<String> t = javaSparkContext.textFile("streams/src/main/resources/test_keys.csv");

        JavaPairRDD<Set<Integer>, Set<String>> in = t.mapToPair(line -> {
            String[] parts = line.split(",");
            Set<Integer> idSet = new HashSet<>();
            idSet.add(Integer.parseInt(parts[0]));
            return new Tuple2<>(idSet, new HashSet<>(Arrays.asList(parts[1].split(" "))));
        });

        JavaPairRDD<Set<Integer>, Set<String>> out = groupWords(in, true);
        logRDD(out, "Output:");

        spark.close();

    }


    static public JavaPairRDD<Set<Integer>, Set<String>> groupWords(JavaPairRDD<Set<Integer>, Set<String>> in, boolean log) {
        //logRDD(in, "Input:");
        JavaPairRDD<String, Set<Integer>> rdd = in.flatMapToPair(tuple -> {
            List<Tuple2<String, Set<Integer>>> lst = new ArrayList<>(tuple._2().size());
            Set<String> words = new HashSet<>(tuple._2());
            tuple._2().forEach(word -> lst.add(new Tuple2<String, Set<Integer>>(word, tuple._1())));
            return lst.iterator();
        });
        if (log) logRDD(rdd, "rdd:");       // <string, [ids]>

        JavaPairRDD<String, Set<Integer>> rdd2 = rdd.reduceByKey((i1, i2) -> {
            Set<Integer> ids = new HashSet<>(i1);
            ids.addAll(i2);
            return ids;
        });
        if (log) logRDD(rdd2, "rdd2");       // <word, [ids, ids2]>>

        JavaPairRDD<Integer, Tuple2<Set<Integer>, Set<String>>> rdd3 = rdd2.flatMapToPair(tuple -> {
            List<Tuple2<Integer, Tuple2<Set<Integer>, Set<String>>>> lst = new ArrayList<>(tuple._2().size());
            tuple._2().forEach(id -> {
                Set<String> words = new HashSet();
                words.add(tuple._1());
                lst.add(new Tuple2<>(id, new Tuple2<>(tuple._2(), words)));
            });
            return lst.iterator();
        });
        if (log) logRDD(rdd3, "rdd3:");      // <id, <[ids], [word]>>

        JavaPairRDD<Set<Integer>, Set<String>> rdd5 = groupIds(rdd3);
        if (log) logRDD(rdd5, "rdd5:");     // <[ids],[words]>

        JavaPairRDD<Integer, Tuple2<Set<Integer>, Set<String>>> rdd6 =
                rdd5.flatMapToPair(tuple -> tuple._1().stream().map(id -> new Tuple2<>(id, new Tuple2<Set<Integer>, Set<String>>(tuple._1(), tuple._2()))).iterator());

        JavaPairRDD<Set<Integer>, Set<String>> rdd7 = groupIds(rdd6);
        if (log) logRDD(rdd7, "rdd7:");

        return rdd7;
    }


    static public JavaPairRDD<Set<Integer>, Set<String>> groupIds(JavaPairRDD<Integer, Tuple2<Set<Integer>, Set<String>>> in) {
        JavaPairRDD<Integer, Tuple2<Set<Integer>, Set<String>>> rdd1 = in.reduceByKey((i1, i2) -> {
            Set<Integer> mergeIds = new HashSet<>(i1._1());
            mergeIds.addAll(i2._1());
            Set<String> mergeWords = new HashSet<>(i1._2());
            mergeWords.addAll(i2._2());
            return new Tuple2<>(mergeIds,mergeWords);
        });
        //if (log) logRDD(rdd1, "groupIds-rdd1:");     // <id, <[ids],[words]>

        JavaPairRDD<Set<Integer>, Set<String>> rdd2 = rdd1
                .mapToPair(tuple -> new Tuple2<>(tuple._2()._1(), tuple._2()._2()))     // <[ids],[words]>
                .reduceByKey((i1, i2) -> {
                    Set<String> newSet = new HashSet(i1);
                    newSet.addAll(i2);
                    return newSet;
                });
        //if (log) logRDD(rdd2, "groupIds-rdd2:");     // <[ids],[words]>
        return rdd2;
    }

/*    static public JavaPairRDD<List<String>, List<String>> removeUnmatched(JavaPairRDD<List<String>, List<String>> in) {
        JavaPairRDD<List<String>, List<String>> out = in.flatMapToPair(tuple -> {
                    List<Tuple2<String, Tuple2<List<String>, List<String>>>> merge = new ArrayList<>();
                    tuple._1().forEach(id -> merge.add(new Tuple2(id, new Tuple2<>(tuple._1(), tuple._2()))));
                    return merge.iterator();
                }).reduceByKey((i1, i2) ->  (i1._1().size() > i2._1().size()) ? i1 : i2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2()._1(), tuple._2()._2()))
                .distinct();
        return out;
    }*/

    public static <K, V> void logRDD(JavaPairRDD<K, V> rdd, String header) {
        System.out.println("**" + header + ":**  ");
        rdd.foreach(item -> {
            System.out.println(item._1() + "=" + item._2() + "  ");
        });
        System.out.println("\n\n");
    }

    @Test
    public void memUsage() {
        Set<String> hashSet = new HashSet<>();
        Set<String> treeSet = new TreeSet<>();
        for (int i = 0; i < 1000; i++) {
            String element = "Element" + i;
            hashSet.add(element);
            treeSet.add(element);
        } // Memory usage example (for illustrative purposes)
        Runtime runtime = Runtime.getRuntime(); runtime.gc(); // Encourage garbage collection
        long hashSetMemory = runtime.totalMemory() - runtime.freeMemory();
        hashSet.clear();
        runtime.gc();
        long treeSetMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("HashSet memory usage: " + hashSetMemory + " bytes"); System.out.println("TreeSet memory usage: " + treeSetMemory + " bytes");
    }

    ;
}