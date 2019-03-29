// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

// COMMAND ----------

// First initialize SparkSession object by default it will available in shells as spark
val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("Spark CSV Reader")
        .getOrCreate;

// COMMAND ----------

// do it in programmatic way
val dfTags = spark.read.format("csv").option("header", "true").load("/FileStore/tables/question_tags_10K.csv")


// COMMAND ----------

//To visually inspect some of the data points from our dataframe, we call the method show(10) which will print only 10 line items to the console.
dfTags.show(10)

// COMMAND ----------

// To show the dataframe schema which was inferred by Spark, you can call the method printSchema() on the dataframe dfTags. 
dfTags.printSchema()

// COMMAND ----------


