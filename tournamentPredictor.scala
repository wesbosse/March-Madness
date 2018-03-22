// Databricks notebook source
import org.apache.spark.sql.functions.{regexp_extract, regexp_replace}
import org.apache.spark.sql.types.IntegerType
import java.sql.{Connection,DriverManager}

import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}


// COMMAND ----------

var df = sqlContext
  .read
  .format("csv")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("/FileStore/tables/Auto.csv")

// COMMAND ----------

df.dtypes

// COMMAND ----------

df.orderBy($"horsepower".desc).show()

// COMMAND ----------

df = df.filter("horsepower != '?'")

// COMMAND ----------

df.orderBy($"horsepower".desc).show()

// COMMAND ----------

df.dtypes

// COMMAND ----------

df = df.withColumn("horsepower", df("horsepower").cast(IntegerType))

// COMMAND ----------

df.show()

// COMMAND ----------

df = df.withColumn("make", regexp_extract($"name", "^\\w+", 0))

// COMMAND ----------

df.show()

// COMMAND ----------

df = df.withColumn("make", regexp_replace($"make", "(chevroelt|chevy)", "chevrolet"))
df = df.withColumn("make", regexp_replace($"make", "capri", "mercury"))
df = df.withColumn("make", regexp_replace($"make", "hi", "ihc"))
df = df.withColumn("make", regexp_replace($"make", "ma.da", "mazda"))
df = df.withColumn("make", regexp_replace($"make", "toyouta", "toyota"))
df = df.withColumn("make", regexp_replace($"make", "(vokswagen|vw)", "volkswagen"))
df.groupBy("make").count().orderBy($"make".asc).show(300)

// COMMAND ----------

var makeIndexer = new StringIndexer()
  .setInputCol("make")
  .setOutputCol("makeIndexed")
  .setHandleInvalid("keep")

// COMMAND ----------

var makeEncoder = new OneHotEncoder()
  .setInputCol(makeIndexer.getOutputCol)
  .setOutputCol("makeEncoded")

// COMMAND ----------

df.dtypes

// COMMAND ----------

df.dtypes

// COMMAND ----------

var assembler = new VectorAssembler()
  .setInputCols(Array("mpg", "cylinders", "displacement", "horsepower", "weight", "year", "origin", makeEncoder.getOutputCol))
  .setOutputCol("features")

// COMMAND ----------

var rf = new RandomForestRegressor()
  .setLabelCol("acceleration")

// COMMAND ----------

var evaluator = new RegressionEvaluator()
  .setMetricName("r2")
  .setLabelCol("acceleration")

// COMMAND ----------

var pipeline = new Pipeline()
  .setStages(Array(makeIndexer, makeEncoder, assembler, rf))

// COMMAND ----------

var paramGrid = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(20, 30, 40))
  .addGrid(rf.maxDepth, Array(2, 3, 4, 5))
  .build()

// COMMAND ----------

var cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEstimatorParamMaps(paramGrid)
  .setEvaluator(evaluator)

// COMMAND ----------

var Array(train, test) = df.randomSplit(Array(.8, .2), 42)

// COMMAND ----------

test.show()

// COMMAND ----------

pipeline.fit(train)

// COMMAND ----------

var cvModel = cv.fit(train)

// COMMAND ----------

var predictions = cvModel.transform(test)

// COMMAND ----------

evaluator.evaluate(predictions)

// COMMAND ----------

train.show()

// COMMAND ----------

test.show()

// COMMAND ----------

predictions.show()

// COMMAND ----------

var maxScore = cvModel.avgMetrics.max
var maxScoreIndex = cvModel.avgMetrics.indexOf(maxScore)

// COMMAND ----------

cvModel.getEstimatorParamMaps(maxScoreIndex)

// COMMAND ----------

var df = sqlContext
  .read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/Credit.csv")

// COMMAND ----------

df.show()

// COMMAND ----------

var genderIndexer = new StringIndexer()
  .setInputCol("Gender")
  .setOutputCol("GenderIndexed")

// COMMAND ----------

var studentIndexer = new StringIndexer()
  .setInputCol("Student")
  .setOutputCol("StudentIndexed")

// COMMAND ----------

var marriedIndexer = new StringIndexer()
  .setInputCol("Married")
  .setOutputCol("MarriedIndexed")

// COMMAND ----------

var ethnicityIndexer = new StringIndexer()
  .setInputCol("Ethnicity")
  .setOutputCol("EthnicityIndexed")

var ethnicityEncoder = new OneHotEncoder()
  .setInputCol(ethnicityIndexer.getOutputCol)
  .setOutputCol("EthnicityEncoded")

// COMMAND ----------

var rf = new RandomForestRegressor()
  .setLabelCol("Balance")

// COMMAND ----------

df.dtypes

// COMMAND ----------

var assembler = new VectorAssembler()
  .setInputCols(Array("Income", "Limit", "Rating", "Cards", "Age", "Education", genderIndexer.getOutputCol, studentIndexer.getOutputCol, marriedIndexer.getOutputCol, ethnicityEncoder.getOutputCol))
  .setOutputCol("features")

// COMMAND ----------

var creditEvaluator = new RegressionEvaluator()
  .setMetricName("r2")
  .setLabelCol("Balance")

// COMMAND ----------

var pipeline = new Pipeline()
  .setStages(Array(studentIndexer, marriedIndexer, ethnicityIndexer, ethnicityEncoder, genderIndexer, assembler, rf))

// COMMAND ----------

var paramGrid = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(40, 50, 1000))
  .build()

// COMMAND ----------

var cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEstimatorParamMaps(paramGrid)
  .setEvaluator(creditEvaluator)

// COMMAND ----------

var Array(train, test) = df.randomSplit(Array(.8, .2), 42)

// COMMAND ----------

var cvModel = cv.fit(train)

// COMMAND ----------

var predictions = cvModel.transform(test)

// COMMAND ----------

creditEvaluator.evaluate(predictions)

// COMMAND ----------

var maxScore = cvModel.avgMetrics.max
var maxIndex = cvModel.avgMetrics.indexOf(maxScore)

// COMMAND ----------

thiscvModel.getEstimatorParamMaps(maxIndex)

val url = "jdbc:mysql://localhost:8889/mysql"
val driver = "com.mysql.jdbc.Driver"
val username = ""
val password = ""
var connection:Connection = _
try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    val rs = statement.executeQuery(
        "CREATE TABLE predictions (game_id int, team varchar(32), prob float)"
    )
} catch {
    case e: Exception => e.printStackTrace
}
connection.close




