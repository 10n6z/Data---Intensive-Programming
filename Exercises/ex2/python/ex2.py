"""Exercise 2 for Data-Intensive Programming"""

from typing import List

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession


def main():
    # In Databricks, the Spark session is created automatically, and you should not create it yourself.
    spark: SparkSession = SparkSession.builder \
                                      .appName("ex2") \
                                      .config("spark.driver.host", "localhost") \
                                      .master("local") \
                                      .getOrCreate()

    # suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("WARN")



    # COMP.CS.320 Data-Intensive Programming, Exercise 2
    #
    # This exercise contains basic tasks of data processing using Spark and DataFrames.
    # The tasks that can be done in either Scala or Python.
    # This is the Python version intended for local development.
    #
    # Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    # There is some test code or example output following most of the tasks.
    #
    # Don't forget to submit your solutions to Moodle.


    # Some resources that can help with the tasks in this exercise:
    #
    # - The tutorial notebook from our course (can be found from the ex1 folder of this repository)
    # - Chapter 3 in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
    #     - There are additional code examples in the related GitHub repository: https://github.com/databricks/LearningSparkV2
    #     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    #       `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
    # - Databricks tutorial of using Spark DataFrames: https://docs.databricks.com/en/getting-started/dataframes.html
    # - Apache Spark documentation on all available functions that can be used on DataFrames:
    #     https://spark.apache.org/docs/3.5.0/sql-ref-functions.html
    # - The full Spark Python functions API listing for the functions package might have some additional functions listed that have not been updated in the documentation:
    #     https://spark.apache.org/docs/3.5.0/api/python/reference/pyspark.sql/functions.html



    printTaskLine(1)
    # Task 1 - Create DataFrame
    #
    # In the `data/ex2` folder at the root of the repository is a file `nordics_weather.csv` that contains weather data from Finland, Sweden, and Norway in CSV format.
    # The data is based on a dataset from Kaggle: https://www.kaggle.com/datasets/adamwurdits/finland-norway-and-sweden-weather-data-20152019
    # The Kaggle page has further descriptions on the data and the units used in the data.
    #
    # Read the data from the CSV file into DataFrame called weatherDF. Let Spark infer the schema for the data.
    # Note that the column separator in the CSV file is a semicolon (`;`) instead of the default comma.
    #
    # Print out the schema.
    # Study the schema and compare it to the data in the CSV file. Do they match?

    weatherDF: DataFrame = __MISSING__IMPLEMENTATION__

    # code that prints out the schema for weatherDF
    __MISSING__IMPLEMENTATION__


    # Example output for task 1:
    #
    # root
    #  |-- country: string (nullable = true)
    #  |-- date: date (nullable = true)
    #  |-- temperature_avg: double (nullable = true)
    #  |-- temperature_min: double (nullable = true)
    #  |-- temperature_max: double (nullable = true)
    #  |-- precipitation: double (nullable = true)
    #  |-- snow_depth: double (nullable = true)



    printTaskLine(2)
    # Task 2 - The first items from DataFrame
    #
    # In this task and all the following tasks you can (and should) use the variables defined in the previous tasks.
    #
    # Part 1:
    # - Fetch the first **seven** rows of the weather data frame and print their contents.
    #
    # Part 2:
    # - Fetch the last **six** rows of the weather data frame, but this time only include the `country`, `date`, and `temperature_avg` columns.
    # - Print out the result.

    weatherSample1: List[Row] = __MISSING__IMPLEMENTATION__

    print("The first seven rows of the weather data frame:")
    print(*[list(row.asDict().values()) for row in weatherSample1], sep="\n")  # prints each Row to its own line
    print("==============================")


    weatherSample2: List[Row] = __MISSING__IMPLEMENTATION__

    print("The last six rows of the weather data frame:")
    print(*[list(row.asDict().values()) for row in weatherSample2], sep="\n")


    # Example output for task 2:

    # The first seven rows of the weather data frame:
    # ['Finland', datetime.date(2019, 12, 28), -9.107407407, -15.28888889, -4.703947368, 0.789265537, 116.4210526]
    # ['Finland', datetime.date(2015, 4, 8), 4.025, 1.336129032, 6.196129032, 0.116666667, 486.5833333]
    # ['Sweden', datetime.date(2018, 10, 20), 5.077777778, 1.241743119, 9.210550459, 0.885153584, 0.0]
    # ['Finland', datetime.date(2016, 3, 7), -0.775, -2.065584416, 0.001315789, 2.122613065, 469.6315789]
    # ['Sweden', datetime.date(2017, 11, 29), -1.355555556, -7.81146789, -3.817889908, 2.728667791, 103.3424658]
    # ['Finland', datetime.date(2016, 12, 24), -1.275, -5.344736842, 0.930263158, 4.751041667, 214.8181818]
    # ['Norway', datetime.date(2019, 12, 29), 2.657894737, 0.575, 6.792307692, 19.75630252, 195.8148148]
    # ==============================
    # The last six rows of the weather data frame:
    # ['Norway', datetime.date(2015, 2, 21), -2.742105263]
    # ['Norway', datetime.date(2019, 11, 19), 1.315789474]
    # ['Finland', datetime.date(2015, 12, 9), 2.517857143]
    # ['Norway', datetime.date(2017, 5, 21), 7.710526316]
    # ['Sweden', datetime.date(2015, 7, 28), 14.36]
    # ['Norway', datetime.date(2018, 2, 10), -0.131578947]



    printTaskLine(3)
    # Task 3 - Minimum and maximum
    #
    # Find the minimum temperature and the maximum temperature from the whole data.

    # collecting everything after select, and taking the first row and column
    minTemp: float = __MISSING__IMPLEMENTATION__
    maxTemp: float = __MISSING__IMPLEMENTATION__

    print(f"Min temperature is {minTemp}")
    print(f"Max temperature is {maxTemp}")


    # Example output for task 3:
    #
    # Min temperature is -29.63961039
    # Max temperature is 30.56143791



    printTaskLine(4)
    # Task 4 - Adding a column
    #
    # Add a new column `year` to the weatherDataFrame and print out the schema for the new DataFrame.
    #
    # The type of the new column should be integer and value calculated from column `date`.

    weatherDFWithYear: DataFrame = __MISSING__IMPLEMENTATION__

    # code that prints out the schema for weatherDFWithYear
    __MISSING__IMPLEMENTATION__


    # Example output for task 4:
    #
    # root
    #  |-- country: string (nullable = true)
    #  |-- date: date (nullable = true)
    #  |-- temperature_avg: double (nullable = true)
    #  |-- temperature_min: double (nullable = true)
    #  |-- temperature_max: double (nullable = true)
    #  |-- precipitation: double (nullable = true)
    #  |-- snow_depth: double (nullable = true)
    #  |-- year: integer (nullable = true)



    printTaskLine(5)
    # Task 5 - Aggregated DataFrame 1
    #
    # Find the minimum and the maximum temperature for each year.
    #
    # Sort the resulting DataFrame based on year so that the latest year is the first row in the DataFrame.

    temperatureDF: DataFrame = __MISSING__IMPLEMENTATION__

    temperatureDF.show()


    # Example output for task 5:
    #
    # +----+---------------+---------------+
    # |year|temperature_min|temperature_max|
    # +----+---------------+---------------+
    # |2019|   -26.63708609|    29.47627907|
    # |2018|   -24.00592105|    30.56143791|
    # |2017|        -24.922|    23.14771242|
    # |2016|   -29.63961039|    26.28026906|
    # |2015|   -21.97961783|     25.7285124|
    # +----+---------------+---------------+



    printTaskLine(6)
    # Task 6 - Aggregated DataFrame 2
    #
    # Expanding from task 5, create a DataFrame that separates the data by both year and country.
    # For each year and country pair, the resulting DataFrame should contain the following values:
    # - the number of entries (as in rows in the original data) there are for that year
    # - the minimum temperature (rounded to 1 decimal precision)
    # - the maximum temperature (rounded to 1 decimal precision)
    # - the average snow depth (rounded to whole numbers)
    #
    # Order the DataFrame first by year with the latest year first, and then by country using alphabetical ordering.

    task6DF: DataFrame = __MISSING__IMPLEMENTATION__

    task6DF.show()


    # Example output for task 6:
    #
    # +----+-------+-------+---------------+---------------+--------------+
    # |year|country|entries|temperature_min|temperature_max|snow_depth_avg|
    # +----+-------+-------+---------------+---------------+--------------+
    # |2019|Finland|    365|          -26.6|           28.7|           159|
    # |2019| Norway|    365|          -12.4|           26.0|           108|
    # |2019| Sweden|    365|          -17.3|           29.5|            81|
    # |2018|Finland|    365|          -24.0|           30.6|           178|
    # |2018| Norway|    365|          -13.3|           27.9|           158|
    # |2018| Sweden|    365|          -19.8|           29.8|           136|
    # |2017|Finland|    365|          -24.9|           23.1|           218|
    # |2017| Norway|    365|          -13.3|           21.1|            89|
    # |2017| Sweden|    365|          -21.9|           21.6|            72|
    # |2016|Finland|    366|          -29.6|           25.0|           173|
    # |2016| Norway|    366|          -14.5|           23.6|            96|
    # |2016| Sweden|    366|          -22.4|           26.3|            71|
    # |2015|Finland|    365|          -22.0|           23.8|           186|
    # |2015| Norway|    365|          -11.1|           22.3|           114|
    # |2015| Sweden|    365|          -16.1|           25.7|            71|
    # +----+-------+-------+---------------+---------------+--------------+



    printTaskLine(7)
    # Task 7 - Aggregated DataFrame 3
    #
    # Using the DataFrame created in task 6, `task6DF`, find the following values:
    #
    # - the minimum temperature in Finland for year 2016
    # - the maximum temperature in Sweden for year 2017
    # - the difference between the maximum and the minimum temperature in Norway for year 2018
    # - the average snow depth for year 2015 when taking into account all three countries

    min2016: float = __MISSING__IMPLEMENTATION__
    max2017: float = __MISSING__IMPLEMENTATION__
    difference2018: float = __MISSING__IMPLEMENTATION__
    snow2015: float = __MISSING__IMPLEMENTATION__

    print(f"Min temperature (Finland, 2016):       {min2016} °C")
    print(f"Max temperature (Sweden, 2017):         {max2017} °C")
    print(f"Temperature difference (Norway, 2018):  {difference2018} °C")
    print(f"The average snow depth (2015):          {round(snow2015)} mm")


    # Example output for task 7:
    #
    # Min temperature (Finland, 2016):       -29.6 °C
    # Max temperature (Sweden, 2017):         21.6 °C
    # Temperature difference (Norway, 2018):  41.2 °C
    # The average snow depth (2015):          124 mm



    printTaskLine(8)
    # Task 8 - One more aggregated DataFrame task
    #
    # Part 1:
    # - How many days in each year was the average temperature below -10 °C in Finland?
    # - How many days in total for each country was the average temperature above +5 °C when snow depth was above 100 mm?
    #
    # Part 2:
    # - What are the top 10 days in Finland on which the difference between the maximum and the minimum temperature within the day was the largest?

    daysBelowMinus10DF: DataFrame = __MISSING__IMPLEMENTATION__

    print("The number of days the average temperature in Finland was below -10 °C:")
    daysBelowMinus10DF.show()

    daysAbove5DF: DataFrame = __MISSING__IMPLEMENTATION__

    print("The number of days the temperature in each country was above +5 °C when snow depth was above 100 mm:")
    daysAbove5DF.show()


    differenceDaysDF: DataFrame = __MISSING__IMPLEMENTATION__

    print("The top 10 days in Finland with the largest temperature difference:")
    differenceDaysDF.show()


    # Example output for task 8:
    #
    # The number of days the average temperature in Finland was below -10 °C:
    # +----+-----+
    # |year|count|
    # +----+-----+
    # |2015|   13|
    # |2016|   26|
    # |2017|   10|
    # |2018|   38|
    # |2019|   27|
    # +----+-----+
    #
    # The number of days the temperature in each country was above +5 °C when snow depth was above 100 mm:
    # +-------+-----+
    # |country|count|
    # +-------+-----+
    # |Finland|   75|
    # | Norway|   10|
    # | Sweden|   42|
    # +-------+-----+
    #
    # The top 10 days in Finland with the largest temperature difference:
    # +----------+----------------+
    # |      date|temperature_diff|
    # +----------+----------------+
    # |2018-05-13|           17.94|
    # |2018-05-14|           17.07|
    # |2018-05-12|           16.61|
    # |2018-05-11|           16.52|
    # |2018-05-25|           16.42|
    # |2018-05-15|           16.39|
    # |2018-05-16|           15.99|
    # |2016-01-22|           15.98|
    # |2018-05-28|           15.98|
    # |2019-04-18|           15.74|
    # +----------+----------------+



    # Stop the Spark session (DO NOT do this in Databricks!)
    spark.stop()


# Helper function to separate the task outputs from each other
def printTaskLine(taskNumber: int) -> None:
    print(f"======\nTask {taskNumber}\n======")


if __name__ == "__main__":
    main()
