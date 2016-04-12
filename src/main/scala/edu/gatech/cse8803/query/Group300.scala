/**
  * @author Joshua Powers <powersj@gatech.edu>
  */

package edu.gatech.cse8803.query

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.Group300Data
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class Group300() {

  /**
    * Primary method to load the data and then run queries.
    *
    * @param sqlContext
    * @return
    */
  def benchmark(sqlContext: SQLContext) = {
    runQueries(loadData(sqlContext))
  }

  /**
    * Pull data out of CSV files using SQL like syntax.
    *
    * @param sqlContext
    * @return
    */
  def loadData(sqlContext: SQLContext): (RDD[Group300Data]) = {
    List("data/care_site.csv", "data/provider.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _))

    sqlContext.sql(
      """
        |SELECT specialty_concept_id, provider_id, provider.care_site_id
        |FROM provider
        |JOIN care_site on (provider.care_site_id = care_site.care_site_id)
      """.stripMargin)
      .map(r => Group300Data(r(0).toString.toInt, r(1).toString, r(2).toString.toInt))
  }

  /**
    * Used to cache and then collect the data.
    *
    * @return List of the query times
    */
  def runQueries(data: RDD[Group300Data]): List[Long] = {
    println("[Group 300] Collecting data to force caching...")

    val start: Long = System.currentTimeMillis
    data.cache()
    data.collect()
    val end: Long = System.currentTimeMillis

    println("\n[Group 300] Caching + collect completed in " + (end - start) + " ms")
    List(query_300(data), query_301(data), query_302(data))
  }

  /**
    * SELECT COUNT(DISTINCT provider_id)
    * FROM provider;
    *
    * @param data
    * @return
    */
  def query_300(data: RDD[Group300Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.map(p => p.provider_id).distinct.count
    val end: Long = System.currentTimeMillis

    println("[Group 300] Query 300 completed in " + (end - start) + " ms")
    end - start
  }

  /**
    * SELECT specialty_concept_id,
    * COUNT(DISTINCT provider_id)
    * FROM provider
    * GROUP BY specialty_CONCEPT_ID;
    *
    * @param data
    * @return
    */
  def query_301(data: RDD[Group300Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.groupBy(p => (p.specialty_concept_id, p.provider_id))
      .map(p => p._1).distinct.count
    val end: Long = System.currentTimeMillis

    println("[Group 300] Query 301 completed in " + (end - start) + " ms")
    end - start
  }

  /**
    * SELECT COUNT(provider_id)
    * FROM provider
    * LEFT JOIN care_site ON provider.care_site_id = care_site.care_site_id
    * WHERE provider.care_site_id IS NOT NULL
    * AND care_site.care_site_id IS NULL;
    *
    * @param data
    * @return
    */
  def query_302(data: RDD[Group300Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.map(p => p.provider_id).count()
    val end: Long = System.currentTimeMillis

    println("[Group 300] Query 302 completed in " + (end - start) + " ms")
    end - start
  }

}
