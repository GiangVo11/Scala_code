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

// We will make use of the open-sourced StackOverflow dataset. We've cut down each dataset to just 10K line.
val dfTags = spark.read.format("csv").option("header", "true").load("/FileStore/tables/question_tags_10K.csv")


// COMMAND ----------

//To visually inspect some of the data points from our dataframe, we call the method show(10) which will print only 10 line items to the console.
dfTags.show(10)

// COMMAND ----------

// To show the dataframe schema which was inferred by Spark, you can call the method printSchema() on the dataframe dfTags. 
dfTags.printSchema()

// COMMAND ----------

//DataFrame Query: select columns from a dataframe. To select specific columns from a dataframe, we can use the select() method and pass in the columns which we want to select
 dfTags.select("id", "tag").show(10)

// COMMAND ----------

// DataFrame Query: filter by column value of a dataframe
// To find all rows matching a specific column value, we can use the filter() method of a dataframe. For example, let's find all rows where the tag column has a value of php.
dfTags.filter("tag == 'php'").show(10)

// COMMAND ----------

// DataFrame Query: count rows of a dataframe
// To count the number of rows in a dataframe, we can use the count() method. As an example, let's count the number of php tags in our dataframe dfTags.We've chained the filter() and count() methods.
println(s"Number of php tags=${dfTags.filter("tag=='php'").count()}")
//We've prefixed the s at the beginning of our println statement.Prepending s to any string literal allows the usage of variables directly in the string. With the s interpolator, any name that is in scope can be used within a string.The s interpolator knows to insert the value of the name variable at this location in the string
//We also used the dollar sign $ to refer to our variable. Any arbitrary expression can be embedded in ${}.


// COMMAND ----------

 val coun1=dfTags.filter("tag=='php'").count()
println(s"Number of php tags=$coun")

// COMMAND ----------

//DataFrame Query: SQL like query
//We can query a dataframe column and find an exact value match using the filter() method. In addition to finding the exact value, we can also query a dataframe column's value using a familiar SQL 'like' clause
dfTags.filter("tag like 's%'").show(10)


 

// COMMAND ----------

DataFrame Query: Multiple filter chaining

// COMMAND ----------

 dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)
