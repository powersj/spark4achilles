/**
  * @author Joshua Powers <powersj@gatech.edu>.
  */

package edu.gatech.cse8803.main

import edu.gatech.cse8803.query.{Group300, Group400}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("CSE8803FinalProject")
      .setMaster("yarn-cluster")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    new Group300().benchmark(sqlContext)
    new Group400().benchmark(sqlContext)

    sc.stop()
  }


}
