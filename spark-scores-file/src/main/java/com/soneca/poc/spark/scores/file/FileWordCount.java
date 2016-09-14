package com.soneca.poc.spark.scores.file;

import com.soneca.poc.spark.scores.core.WordCount;
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
public class FileWordCount extends WordCount {

    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("Please provide the file");
            System.exit(0);
        }

        JavaSparkContext context =
                new JavaSparkContext(
                        new SparkConf().setAppName("com.soneca.poc.spark.scores.file.FileWordCount").setMaster("local")
                );

        JavaRDD<String> file = context.textFile(args[0]);
        JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
        JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
        JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);

        counter.saveAsTextFile(args[1]);


    }
}
