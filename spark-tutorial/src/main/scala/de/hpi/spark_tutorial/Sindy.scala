package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    //Read all Datasets
    val datasets = inputs.map(filename => {
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(filename)
    })

    val columns = datasets.flatMap(data => data.columns.map(data.select(_)))

    val cells = columns.map(col => col
      .map(row => (row.get(0).toString, row.schema.fieldNames.head)))
      .reduce(_.union(_))
      .toDF("val", "header")

    import org.apache.spark.sql.functions._
    val attributeset = cells
      .groupBy($"val").agg(collect_set($"header").as("attr_set"))
      .select("attr_set")
      .distinct()
      .as[Seq[String]]

    val inclusionlist = attributeset
      .select(explode($"attr_set").as("key"), col("attr_set"))
      .as[(String, Seq[String])]
      .map(row => (row._1, row._2.toSet - row._1))
      .toDF("attr", "set")

    val aggregate = inclusionlist
      .groupBy($"attr").agg(collect_set($"set").as("aggr"))
      .as[(String,Seq[Seq[String]])]
      .map(row => (row._1,row._2.reduce(_.intersect(_))))
      .filter(row=> row._2.nonEmpty)
      .toDF("dep", "ref")
      .sort("dep")
      .as[(String,Seq[String])]

    aggregate
      .collect()
      .foreach(row => println(row._1 + " < " + row._2.reduce(_ + ", " + _)) )

  }
}