package dip24.ex4

import java.io.File
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{FileSystems, Files, Path, Paths}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.{Try, Success, Failure}

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.nspl.{Color, InLegend, line, par, point, xyplot}
import org.nspl.awtrenderer.{defaultAWTFont, renderToFile, shapeRenderer, textRenderer}


object Ex4Main extends App {
    // In Databricks, the Spark session is created automatically, and you should not create it yourself.
	val spark = SparkSession.builder()
                            .appName("ex4")
                            .config("spark.driver.host", "localhost")
                            .master("local")
                            .getOrCreate()

    // suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.ERROR.toString())

    // reduce the number of shuffle partitions from the default 200 to have more efficient local execution
    spark.conf.set("spark.sql.shuffle.partitions", "5")


    // COMP.CS.320 Data-Intensive Programming, Exercise 4
    //
    // This exercise is in two parts.
    //
    // - Tasks 1-4 contain task related to using the Spark machine learning (ML) library.
    // - Tasks 5-8 contain tasks for aggregated data frames with streaming data.
    //     - Task 5 is a reference task with static data, and the other tasks deal with streaming data frames.
    //
    // This is the Scala version intended for local development.
    //
    // Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    // There is example output following most of the tasks.
    //
    // Don't forget to submit your solutions to Moodle.

    // Some resources that can help with the tasks in this exercise:
    //
    // - The tutorial notebook from our course (can be found from the ex1 folder of this repository)
    // - Chapters 8 and 10 in [Learning Spark, 2nd Edition](https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
    //     - There are additional code examples in the related [GitHub repository](https://github.com/databricks/LearningSparkV2).
    //     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    //       `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
    // - Apache Spark documentation on all available functions that can be used on DataFrames:
    //     https://spark.apache.org/docs/3.5.0/sql-ref-functions.html
    // - The full Spark Scala functions API listing for the functions package might have some additional functions listed that have not been updated in the documentation:
    //     https://spark.apache.org/docs/3.5.0/api/scala/org/apache/spark/sql/functions$.html



    // the initial data for the linear regression tasks
    val hugeSequenceOfXYData: Seq[Row] = Seq(
        Row(9.44, 14.41), Row(0.89, 1.77), Row(8.65, 12.47), Row(10.43, 15.43), Row(7.39, 11.03), Row(10.06, 15.18), Row(2.07, 3.19), Row(1.24, 1.45),
        Row(3.84, 5.45), Row(10.78, 16.51), Row(10.23, 16.11), Row(9.32, 13.96), Row(7.98, 12.32), Row(0.99, 1.02), Row(6.85, 9.62), Row(8.59, 13.39),
        Row(7.35, 10.44), Row(9.85, 15.26), Row(4.59, 7.26), Row(2.43, 3.35), Row(1.58, 2.71), Row(1.59, 2.2), Row(2.1, 2.95), Row(0.62, 0.47),
        Row(5.65, 9.02), Row(5.9, 9.58), Row(8.5, 12.39), Row(8.74, 13.73), Row(1.93, 3.37), Row(10.22, 15.03), Row(10.25, 15.63), Row(1.97, 2.96),
        Row(8.03, 12.03), Row(2.05, 3.23), Row(0.69, 0.9), Row(7.58, 11.01), Row(9.99, 14.83), Row(10.53, 15.92), Row(6.12, 9.48), Row(1.34, 2.83),
        Row(3.87, 5.27), Row(4.98, 7.21), Row(4.72, 6.48), Row(8.15, 12.19), Row(2.37, 3.45), Row(10.19, 15.16), Row(10.28, 15.39), Row(8.6, 12.76),
        Row(7.46, 11.11), Row(0.25, 0.41), Row(6.41, 9.55), Row(10.49, 15.61), Row(5.18, 7.92), Row(3.74, 6.18), Row(6.27, 9.25), Row(7.51, 11.11),
        Row(4.07, 6.63), Row(5.17, 6.95), Row(9.61, 14.85), Row(4.17, 6.31), Row(4.12, 6.31), Row(9.22, 13.96), Row(5.54, 8.2), Row(0.58, 0.46),
        Row(10.13, 14.68), Row(0.53, 1.25), Row(6.87, 10.0), Row(7.17, 10.35), Row(0.09, -0.55), Row(10.8, 16.6), Row(10.31, 15.96), Row(4.74, 6.53),
        Row(1.6, 2.31), Row(5.45, 7.84), Row(0.65, 1.02), Row(2.89, 3.93), Row(6.28, 9.21), Row(8.59, 13.05), Row(6.6, 10.51), Row(8.42, 12.91)
    )
    val dataRDD: RDD[Row] = spark.sparkContext.parallelize(hugeSequenceOfXYData)


    printTaskLine(1)
    // Task 1 - Linear regression - Training and test data
    //
    // Background
    //
    // In statistics, Simple linear regression is a linear regression model with a single explanatory variable.
    // That is, it concerns two-dimensional sample points with one independent variable and one dependent variable
    // (conventionally, the x and y coordinates in a Cartesian coordinate system) and finds a linear function (a non-vertical straight line)
    // that, as accurately as possible, predicts the dependent variable values as a function of the independent variable.
    // The adjective simple refers to the fact that the outcome variable is related to a single predictor.
    //     Wikipedia: https://en.wikipedia.org/wiki/Simple_linear_regression
    //
    // You are given an RDD of Rows, `dataRDD`, where the first element are the `x` and the second the `y` values.
    // We are aiming at finding simple linear regression model for the dataset using Spark ML library.
    // I.e. find a function `f` so that `y = f(x)` (for the 2-dimensional case `f(x)=ax+b`).
    //
    // Task instructions
    //
    // Transform the given `dataRDD` to a DataFrame `dataDF`, with two columns `X` (of type Double) and `label` (of type Double).
    // (`label` used here because that is the default dependent variable name in Spark ML library)
    //
    // Then split the rows in the data frame into training and testing data frames.

    val dataDF: DataFrame = ???

    // Split the data into training and testing datasets (roughly 80% for training, 20% for testing)
    val trainingDF: DataFrame = ???
    val testDF: DataFrame = ???

    println(s"Training set size: ${trainingDF.count()}")
    println(s"Test set size: ${testDF.count()}")
    println("Training set (showing only the first 6 points):")
    trainingDF.show(6)


    // Example output for task 1 (the data splitting done by using seed value of 1 for data frames random splitting method):
    //
    // Training set size: 64
    // Test set size: 16
    // Training set (showing only the first 6 points):
    // +----+-----+
    // |   X|label|
    // +----+-----+
    // |0.09|-0.55|
    // |0.25| 0.41|
    // |0.53| 1.25|
    // |0.58| 0.46|
    // |0.65| 1.02|
    // |0.69|  0.9|
    // +----+-----+
    // only showing top 6 rows
    //
    // Your output does not have to match this exactly, not even with the sizes of the training and test sets.



    printTaskLine(2)
    // Task 2 - Linear regression - Training the model
    //
    // To be able to use the ML algorithms in Spark, the input data must be given as a vector in one column.
    // To make it easy to transform the input data into this vector format, Spark offers VectorAssembler objects.
    //
    // - Create a `VectorAssembler` for mapping the input column `X` to `features` column.
    //   And apply it to training data frame, `trainingDF,` in order to create an assembled training data frame.
    // - Then create a `LinearRegression` object. And use it with the assembled training data frame to train a linear regression model.

    val vectorAssembler: VectorAssembler = ???

    val assembledTrainingDF: DataFrame = ???

    assembledTrainingDF.printSchema()
    assembledTrainingDF.show(6)


    val lr: LinearRegression = ???

    // you can print explanations for all the parameters that can be used for linear regression by uncommenting the following:
    // println(lr.explainParams())

    val lrModel: LinearRegressionModel = ???

    // print out a sample of the predictions
    lrModel.summary.predictions.show(6)


    // Example outputs for task 2:
    //
    // root
    // |-- X: double (nullable = false)
    // |-- label: double (nullable = false)
    // |-- features: vector (nullable = true)
    //
    // +----+-----+--------+
    // |   X|label|features|
    // +----+-----+--------+
    // |0.09|-0.55|  [0.09]|
    // |0.25| 0.41|  [0.25]|
    // |0.53| 1.25|  [0.53]|
    // |0.58| 0.46|  [0.58]|
    // |0.65| 1.02|  [0.65]|
    // |0.69|  0.9|  [0.69]|
    // +----+-----+--------+
    // only showing top 6 rows
    //
    // and
    //
    // +----+-----+--------+--------------------+
    // |   X|label|features|          prediction|
    // +----+-----+--------+--------------------+
    // |0.09|-0.55|  [0.09]|0.056041394095783625|
    // |0.25| 0.41|  [0.25]|  0.2978850018439783|
    // |0.53| 1.25|  [0.53]|  0.7211113154033191|
    // |0.58| 0.46|  [0.58]|  0.7966874428246298|
    // |0.65| 1.02|  [0.65]|   0.902494021214465|
    // |0.69|  0.9|  [0.69]|  0.9629549231515135|
    // +----+-----+--------+--------------------+
    // only showing top 6 rows



    printTaskLine(3)
    // Task 3 - Linear regression - Test the model
    //
    // Apply the trained linear regression model from task 2 to the test dataset.
    //
    // Then calculate the RMSE (root mean square error) for the test dataset predictions
    // using `RegressionEvaluator` from Spark ML library.

    val testPredictions: DataFrame = ???

    testPredictions.show(6)


    val testEvaluator: RegressionEvaluator = ???

    // you can print explanations for all the parameters that can be used for the regression evaluator by uncommenting the following:
    // println(testEvaluator.explainParams())

    val testError: Double = ???
    println(s"The RMSE for the model is ${testError}")
    println("============================================")


    // Example output for task 3:
    //
    // +----+-----+--------+------------------+
    // |   X|label|features|        prediction|
    // +----+-----+--------+------------------+
    // |0.62| 0.47|  [0.62]|0.8571483447616786|
    // | 1.6| 2.31|   [1.6]| 2.338440442219371|
    // |1.93| 3.37|  [1.93]|2.8372428832000223|
    // |1.97| 2.96|  [1.97]|2.8977037851370713|
    // |2.07| 3.19|  [2.07]|3.0488560399796927|
    // |4.72| 6.48|  [4.72]|7.0543907933091665|
    // +----+-----+--------+------------------+
    // only showing top 6 rows
    //
    // The RMSE for the model is 0.4021886546595933
    //
    // 0 for RMSE would indicate perfect fit and the more deviations there are the larger the RMSE will be.
    //
    // You can try a different seed for dividing the data and different parameters for the linear regression to get different results.


    // Visualization of the linear regression exercise (can be done since there are only limited number of source data points)
    // The plot will be created into a PDF file "linear_regression.pdf"
    val trainingData = trainingDF.collect().map({case Row(x: Double, y: Double) => (x, y)}).toSeq
    val testData = testDF.collect().map({case Row(x: Double, y: Double) => (x, y)}).toSeq
    val predictionData = testPredictions.select("X", "prediction").collect().map({case Row(x: Double, y: Double) => (x, y)}).toSeq
    val minX: Double = (trainingData ++ testData).map({case (x, _) => x}).min
    val maxX: Double = (trainingData ++ testData).map({case (x, _) => x}).max
    val minY: Double = (trainingData ++ testData).map({case (_, y) => y}).min
    val maxY: Double = (trainingData ++ testData).map({case (_, y) => y}).max

    val plot = xyplot(
        (trainingData, List(point(color = Color.BLUE)), InLegend("training data")),
        (testData, List(point(color = Color.GREEN)), InLegend("test data")),
        (predictionData, List(line(color = Color.RED)), InLegend("predictions"))
    )(
        par(
            xlab = "x",
            ylab = "y",
            xlim = Some(minX, maxX),
            ylim = Some(minY, maxY),
            noLegend = false
        )
    )
    renderToFile(f = new File("linear_regression.pdf"), elem = plot, width = 800, mimeType = "application/pdf")



    printTaskLine(4)
    //Task 4 - Linear regression - Making new predictions
    //
    // Use the trained `LinearRegressionModel` from task 2 to predict the `y` values
    // for the following `x` values: -2.8, 3.14, 9.9, 123.45

    ???


    // Example output for task 4:
    //
    // +------+-------------------+
    // |     X|         prediction|
    // +------+-------------------+
    // |  -2.8|-4.3122587708559825|
    // |  3.14|  4.666185166795745|
    // |   9.9|  14.88407759415697|
    // |123.45|  186.5174629679539|
    // +------+-------------------+



    printTaskLine(5)
    // Task 5 - Best selling days with static data
    //
    // This task is mostly to create a reference to which the results from the streaming tasks (6-8) can be compared to.
    //
    // At the root folder of the repository the file `data/ex4/static/superstore_sales.csv` contains sales data from a retailer.
    // The data is based on a dataset from https://www.kaggle.com/datasets/soumyashanker/superstore-us
    //
    // - Read the data from the CSV file into data frame called `salesDF`.
    // - Calculate the total sales for each day, and show the eight best selling days.
    //     - Each row has a sales record for a specific product.
    //     - The column `productPrice` contains the price for an individual product.
    //     - The column `productCount` contains the count for how many items were sold in the given sale.

    val salesDF: DataFrame = ???

    val bestDaysDF: DataFrame = ???

    bestDaysDF.show()


    // Example output for task 5:
    //
    // +----------+------------------+
    // | orderDate|        totalSales|
    // +----------+------------------+
    // |2014-03-18|          52148.32|
    // |2014-09-08|           22175.9|
    // |2017-11-04|          19608.77|
    // |2016-11-25|19608.739999999998|
    // |2016-10-02|18580.819999999996|
    // |2017-10-22|          18317.99|
    // |2016-05-23|16754.809999999998|
    // |2015-09-17|           16601.4|
    // +----------+------------------+



    printTaskLine(6)
    // Task 6 - Setting up a streaming data frame
    //
    // Streaming data simulation
    //
    // In this exercise, tasks 6-8, streaming data is simulated by copying CSV files from a source folder to a target folder.
    // The target folder can thus be considered as streaming data with new file appearing after each file is copied.
    // A helper function to handle the file copying is given in task 8 and you don't need to worry about that.
    // The source folder from which the files are copied is under the `data` folder at the root of the repository.
    // The target folder will be the `data/streaming` folder within this project.
    //
    // The task
    //
    // Create a streaming data frame for similar retailer sales data as was used in task 5.
    // The streaming data frame should point to the target folder where the files will be copied to.
    //
    // Hint: Spark cannot infer the schema of streaming data, so you have to give it explicitly.
    // You can assume that the streaming data will have the same format as the static data used in task 5.
    // Also, in this case the streaming data will not contain header rows, unlike the static data used in task 5.

    val myStreamingFolder: String = "data/streaming"
    // Ensure the folder exists when creating the streaming data frame
    Files.createDirectories(Paths.get(myStreamingFolder))


    val salesStreamingDF: DataFrame = ???


    // Note that you cannot really test this task before you have also done the tasks 7 and 8.
    // I.e. there is no checkable output from this task.



    printTaskLine(7)
    // Task 7 - Best selling days with streaming data
    //
    // Find the best selling days using the streaming data frame from task 6.
    // This time also include the count for the total number of individual items sold on each day in the result.
    //
    // Note that in this task with the streaming data you don't need to limit the result
    // only to the best eight selling days like was done in task 5.

    val bestDaysStreamingDF: DataFrame = ???


    // Note that you cannot really test this task before you have also done the task 8.
    // I.e. there is no checkable output from this task.



    printTaskLine(8)
    // Task 8 - Running streaming query
    //
    // Test your streaming data solution from tasks 6 and 7 by creating and starting a streaming query.
    //
    // You are given helper scripts for copying the files (to simulate the streaming data).

    // There can be delays with the streaming data frame processing. You can try to adjust the wait time if you want.
    val waitAfterFirstCopy: FiniteDuration = 5.seconds
    val normalTimeInterval: FiniteDuration = 3.seconds

    def removeFiles(folder: String): Unit = {
        Try {
            Files.list(Paths.get(folder))
                .iterator().asScala
                .foreach(file => Files.delete(file))
        } match {
            case Failure(_: java.nio.file.NoSuchFileException) => // the folder did not exist => do nothing
            case Failure(exception) => throw exception
            case Success(_) => // the files were removed successfully => do nothing
        }
    }

    def copyFiles(targetFolder: String): Unit = {
        val sourceFolder: Path = Paths.get("../../data/ex4/streaming")
        val inputFileList: Seq[Path] = Files.list(sourceFolder).iterator().asScala.toSeq

        inputFileList.zipWithIndex.foreach { case (csvFile, index) =>
            val outputFilePath: Path = Paths.get(targetFolder).resolve(csvFile.getFileName())

            // copy file from the source folder to the target folder
            Files.copy(csvFile, outputFilePath, REPLACE_EXISTING)
            val waitTime: FiniteDuration = if (index == 0) waitAfterFirstCopy else normalTimeInterval
            println(s"Copied file ${csvFile.getFileName()} (${index + 1}/${inputFileList.size}) to ${outputFilePath} - waiting for ${waitTime}")
            Thread.sleep(waitTime.toMillis)
        }
    }


    // remove all files from the target folder before starting the streaming query to have a fresh run each time
    removeFiles(myStreamingFolder)


    // start the streaming query with the console format and limit the output to the first 8 rows
    val myStreamingQuery: StreamingQuery = ???


    // call the helper function to copy files to the target folder to simulate streaming data
    copyFiles(myStreamingFolder)

    // wait and show the processing of the streaming query
    myStreamingQuery.awaitTermination(normalTimeInterval.toMillis)

    // stop the streaming query
    myStreamingQuery.stop()



    // If all copied files were handled by the streaming query,
    // the final state should match the output from task 5 (the orderDate and the totalPrice columns apart from possible rounding errors).
    //
    // The order of the files being copied might differ, and thus also the intermediate results.
    //
    // Example output for task 8:
    //
    // Copied file 0006.csv (1/10) to data/streaming/0006.csv - waiting for 5 seconds
    // -------------------------------------------
    // Batch: 0
    // -------------------------------------------
    // +----------+------------------+----------+
    // | orderDate|        totalPrice|totalCount|
    // +----------+------------------+----------+
    // |2016-02-02|           8749.95|         5|
    // |2014-12-14|           7003.58|         6|
    // |2015-03-16|            6846.5|        10|
    // |2016-07-28|           5343.12|        18|
    // |2016-03-03|           4964.74|        10|
    // |2015-09-20|           4565.88|         6|
    // |2015-06-16|3812.9700000000003|         3|
    // |2017-05-01|            3504.9|         5|
    // +----------+------------------+----------+
    // only showing top 8 rows
    // Copied file 0008.csv (2/10) to data/streaming/0008.csv - waiting for 3 seconds
    // -------------------------------------------
    // Batch: 1
    // -------------------------------------------
    // +----------+------------------+----------+
    // | orderDate|        totalPrice|totalCount|
    // +----------+------------------+----------+
    // |2014-09-08|          14042.99|        19|
    // |2016-02-02|           8749.95|         5|
    // |2014-11-11| 7618.070000000001|        36|
    // |2014-12-14|           7003.58|         6|
    // |2015-03-16|           6864.02|        13|
    // |2016-10-21|6769.7300000000005|        15|
    // |2017-04-01|           6169.94|        15|
    // |2017-01-16|           5471.88|         8|
    // +----------+------------------+----------+
    // only showing top 8 rows
    //
    // ...
    // ...
    // ...
    //
    // Copied file 0005.csv (10/10) to data/streaming/0005.csv - waiting for 3 seconds
    // -------------------------------------------
    // Batch: 9
    // -------------------------------------------
    // +----------+------------------+----------+
    // | orderDate|        totalPrice|totalCount|
    // +----------+------------------+----------+
    // |2014-03-18|          52148.32|        48|
    // |2014-09-08|22175.899999999998|       112|
    // |2017-11-04|          19608.77|        58|
    // |2016-11-25|19608.739999999998|        53|
    // |2016-10-02|18580.819999999996|        27|
    // |2017-10-22|18317.989999999998|        49|
    // |2016-05-23|          16754.81|        23|
    // |2015-09-17|           16601.4|        86|
    // +----------+------------------+----------+
    // only showing top 8 rows



    // Stop the Spark session (DO NOT do this in Databricks!)
    spark.stop()

    // Helper function to separate the task outputs from each other
    def printTaskLine(taskNumber: Int): Unit = {
        println(s"======\nTask $taskNumber\n======")
    }
}
