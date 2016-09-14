package com.soneca.poc.spark.scores.core;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by soneca on 14/09/16.
 */
public class WordCount {

    protected static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
        public Iterator<String> call(String s) throws Exception {
            return Arrays.asList(s.split(" ")).iterator();
        }
    };

    protected static final PairFunction<String, String, Integer> WORDS_MAPPER = new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<String, Integer>(s, 1);
        }
    };

    protected static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer integer, Integer integer2) throws Exception {
            return integer+integer2;
        }
    };
}
