package com.juandavid.sparkexample

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object connect {
  
  
   def main(args: Array[String]) {
     
    System.setProperty("SPARK_YARN_MODE", "true") 
     
    val sparkConfiguration = new SparkConf();
    sparkConfiguration.setMaster("yarn-client");
    sparkConfiguration.setAppName("test-spark-job");
    //sparkConfiguration.setJars(Array("/ejecutar.jar"));
    sparkConfiguration.set("spark.yarn.jar","hdfs:///user/cloudera/spark-assembly-1.6.0-hadoop2.4.0.jar")
    sparkConfiguration.set("spark.hadoop.fs.defaultFS", "hdfs://quickstart.cloudera");
    sparkConfiguration.set("spark.hadoop.dfs.nameservices", "quickstart.cloudera:8020");
    sparkConfiguration.set("spark.hadoop.yarn.resourcemanager.hostname", "quickstart.cloudera");
    sparkConfiguration.set("spark.hadoop.yarn.resourcemanager.address", "quickstart.cloudera:8032");
 

  
  
    val sc: SparkContext = new SparkContext(sparkConfiguration)
   
  
    
     val values = sc.parallelize(List(1,2,3,4,5))
     values.collect()
  
 
   sc.stop()
  }
  
}