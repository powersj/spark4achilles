/**
  * @author Joshua Powers <powersj@gatech.edu>
  */

package edu.gatech.cse8803.query

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.Group400Data
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class Group400() {

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
  def loadData(sqlContext: SQLContext): (RDD[Group400Data]) = {
    CSVUtils.loadCSVAsTable(sqlContext, "data/condition_occurrence.csv")

    sqlContext.sql(
      """
        |SELECT
        |condition_occurrence.person_id,
        |condition_occurrence.provider_id,
        |condition_occurrence.condition_concept_id,
        |condition_occurrence.condition_type_concept_id,
        |condition_occurrence.visit_occurrence_id,
        |condition_occurrence.condition_start_date,
        |condition_occurrence.condition_end_date
        |FROM condition_occurrence
      """.stripMargin)
      .map(
        r => Group400Data(
          r(0).toString.toInt,
          r(1).toString, r(2).toString, r(3).toString,
          r(4).toString, r(5).toString, r(6).toString
        )
      )
  }

  /**
    * Used to cache and then collect the data.
    *
    * @return List of the query times
    */
  def runQueries(data: RDD[Group400Data]): List[Long] = {
    println("[Group 400] Collecting data to force caching...")

    val start: Long = System.currentTimeMillis
    data.cache()
    data.collect()
    val end: Long = System.currentTimeMillis

    println("\n[Group 400] Caching + collect completed in " + (end - start) + " ms")
    List(query_400(data), query_401(data), query_405(data),
      query_409(data), query_411(data), query_412(data),
      query_413(data))
  }

  /**
    * SELECT condition_concept_id, COUNT(DISTINCT person_id)
    * FROM condition_occurrence
    * GROUP BY condition_concept_id;
    *
    * @param data
    * @return
    */
  def query_400(data: RDD[Group400Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.groupBy(d => d.condition_concept_id).map(d => (d._1, d._2)).distinct().count()
    val end: Long = System.currentTimeMillis

    println("[Group 400] Query 400 completed in " + (end - start) + " ms")
    end - start
  }

  /**
    * SELECT condition_concept_id, COUNT(person_id)
    * FROM condition_occurrence
    * GROUP BY condition_concept_id;
    *
    * @param data
    * @return
    */
  def query_401(data: RDD[Group400Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.groupBy(d => d.condition_concept_id).map(d => (d._1, d._2)).count()
    val end: Long = System.currentTimeMillis

    println("[Group 400] Query 401 completed in " + (end - start) + " ms")
    end - start
  }

  /**
    * SELECT condition_concept_id, condition_type_concept_id, COUNT(person_id)
    * FROM condition_occurrence
    * GROUP BY condition_concept_id, condition_type_concept_id;
    *
    * @param data
    * @return
    */
  def query_405(data: RDD[Group400Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.groupBy(d => (d.condition_concept_id, d.condition_type_concept_id)).map(d => (d._1._1, d._1._2, d._2)).count
    val end: Long = System.currentTimeMillis

    println("[Group 400] Query 405 completed in " + (end - start) + " ms")
    end - start
  }

  /**
    * SELECT COUNT(person_id)
    * FROM condition_occurrence
    * WHERE person.person_id IS NULL;
    *
    * @param data
    * @return
    */
  def query_409(data: RDD[Group400Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.filter(d => d.person_id != 0).map(d => d.person_id).count
    val end: Long = System.currentTimeMillis

    println("[Group 400] Query 409 completed in " + (end - start) + " ms")
    end - start
  }

  /**
    * SELECT COUNT(person_id)
    * FROM condition_occurrence
    * WHERE condition_occurrence.condition_end_date < condition_occurrence.condition_start_date;
    *
    * @param data
    * @return
    */
  def query_411(data: RDD[Group400Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.filter(d => d.condition_end_date < d.condition_start_date).map(d => d.person_id).count
    val end: Long = System.currentTimeMillis
    println("[Group 400] Query 411 completed in " + (end - start) + " ms")
    end - start
  }

  /**
    * SELECT COUNT(person_id)
    * FROM condition_occurrence
    * WHERE condition_occurrence.provider_id IS NOT NULL;
    *
    * @param data
    * @return
    */
  def query_412(data: RDD[Group400Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.filter(d => d.provider_id != null).map(d => d.person_id).count
    val end: Long = System.currentTimeMillis

    println("[Group 400] Query 412 completed in " + (end - start) + " ms")
    end - start
  }

  /**
    * SELECT COUNT(person_id)
    * FROM condition_occurrence
    * WHERE condition_occurrence.visit_occurrence_id IS NOT NULL;
    *
    * @param data
    * @return
    */
  def query_413(data: RDD[Group400Data]): Long = {
    val start: Long = System.currentTimeMillis
    data.filter(d => d.visit_occurrence_id != null).map(d => d.person_id).count
    val end: Long = System.currentTimeMillis

    println("[Group 400] Query 413 completed in " + (end - start) + " ms")
    end - start
  }
}
