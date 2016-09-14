package com.soneca.poc.spark.scores.file;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Simple word count based on file
 * Created by soneca on 14/09/16.
 */
public class WordCount {

    private static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
        public Iterator<String> call(String s) throws Exception {
            return Arrays.asList(s.split(" ")).iterator();
        }
    };

    private static final PairFunction<String, String, Integer> WORDS_MAPPER = new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<String, Integer>(s, 1);
        }
    };

    private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer integer, Integer integer2) throws Exception {
            return integer+integer2;
        }
    };

    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("Please provide the file");
            System.exit(0);
        }

        JavaSparkContext context =
                new JavaSparkContext(
                        new SparkConf().setAppName("com.soneca.poc.spark.scores.file.WordCount").setMaster("local")
                );

        JavaRDD<String> file = context.textFile(args[0]);
        JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
        JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
        JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);

        counter.saveAsTextFile(args[1]);


    }
}
