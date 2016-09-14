### Simple Spark tests ###
Only a spark test with some scores

DEPENDENCIES:

 - Apache Spark 2.0.0
 
 RUN 
  - spark-score-file
  ```
    ./{PATH_TO_SPARK}/bin/spark-submit --class com.soneca.poc.spark.scores.file.FileWordCount --master local[2] {PATH_TO_PROJECT}/spark-scores/spark-scores-file/target/wordcount.jar {PATH_TO_PROJECT}/spark-scores/spark-scores-file/src/test/resources/big.txt {PATH_TO_OUTPUT_FOLDER}
  ```
  
  
### Author ###

Soneca