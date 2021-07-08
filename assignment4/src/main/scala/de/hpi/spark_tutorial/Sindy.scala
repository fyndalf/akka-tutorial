package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, explode}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    // read all required csv files into data frames
    val datasets = inputs.map(filename => {
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(filename)
    })


    // create cells from input tuples
    val cells = datasets.map(dataset => {
      val columnNames = dataset.columns
      dataset.flatMap(row => {
        row.toSeq.zipWithIndex.map(c => (c._1.toString, columnNames(c._2)))
      }).toDF()
    })

    val groupedCells = cells.reduce((cell1, cell2) => cell1.join(cell2, cell2.columns, joinType = "fullouter"))

    val groupAndAggregated = groupedCells.groupBy("_1").agg(collect_set("_2").as("attributeSet"))

    val attributeSets = groupAndAggregated.select("attributeSet")

    val inclusionLists = attributeSets.select(explode(col("attributeSet")).as("key"), col("attributeSet")).as[(String, Seq[String])]
      .map(row => (row._1, row._2.filter(a => !a.equals(row._1)))).filter(row => row._2.nonEmpty).sort(col("_1"))

    val inds = inclusionLists.groupByKey(t => t._1).mapGroups((g, h) => (g, h.map(_._2).reduce(_.intersect(_)))).sort("_1")

    inds.collect().foreach(row => println(row._1 + " < " + row._2.sorted.reduce(_ + ", " + _)))

  }
}
