# conan_thesis_program#

##Environment##
* [Hadoop](https://hadoop.apache.org/)(Use YARN and HDFS)
* [Spark](http://spark.apache.org/)
* These are Spark programs which are written by Scala. So we must build Spark environment, and use HDFS to our storage. Then YARN is a selected component.
 
##Compile method##
* Install the Scala compiler [SBT](http://www.scala-sbt.org/download.html)
* Go to the subdirectory which contains file "simple.sbt" and do "sbt package"
* Source codes are named trendmicro.scala

##Execution method##
* Reference [Spark website](http://spark.apache.org/docs/latest/submitting-applications.html)
* Here is my command example. 
  
  spark-submit --class Hamburger  --master yarn-cluster --executor-memory 3g  --num-executors 7 --executor-cores 2  /home2/conan/Final_Analyze/target/scala-2.10/trendmicro_2.10-1.0.jar itri
* According to my code, the last parameter is which enviroment I am running, you can modify it.

##Execution order(I divide my program to 4 parts)

1. Creat_index_table
2. Recommender_ALS
3. Clustering_Kmeans
4. Final_Analyze
