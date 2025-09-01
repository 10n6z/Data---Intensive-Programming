"""Exercise 3 for Data-Intensive Programming"""

import dataclasses
import re
from typing import List, Tuple

from pyspark.rdd import RDD
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def main():
    # In Databricks, the Spark session is created automatically, and you should not create it yourself.
    spark: SparkSession = SparkSession.builder \
                                      .appName("ex3") \
                                      .config("spark.driver.host", "localhost") \
                                      .master("local") \
                                      .getOrCreate()

    # suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("WARN")



    # COMP.CS.320 Data-Intensive Programming, Exercise 3
    #
    # This exercise is in three parts.
    #
    # - Tasks 1-3 contain additional tasks of data processing using Spark and DataFrames.
    # - Tasks 4-6 are basic tasks of using RDDs with textual data.
    # - Tasks 7-8 are similar basic tasks for textual data but using DataFrames instead.
    #
    # This is the Python version intended for local development.
    #
    # Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    # There is example output following most of the tasks.
    #
    # Don't forget to submit your solutions to Moodle.


    # Some resources that can help with the tasks in this exercise:
    #
    # - The tutorial notebook from our course (can be found from the ex1 folder of this repository)
    # - Chapters 3 and 6 in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
    #     - There are additional code examples in the related GitHub repository: https://github.com/databricks/LearningSparkV2
    #     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    #       `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
    # - Apache Spark documentation on all available functions that can be used on DataFrames:
    #     https://spark.apache.org/docs/3.5.0/sql-ref-functions.html
    # - The full Spark Python functions API listing for the functions package might have some additional functions listed that have not been updated in the documentation:
    #     https://spark.apache.org/docs/3.5.0/api/python/reference/pyspark.sql/functions.html
    # - Apache Spark RDD Programming Guide:
    #     https://spark.apache.org/docs/3.5.0/rdd-programming-guide.html
    # - Apache Spark Datasets and DataFrames documentation:
    #     https://spark.apache.org/docs/3.5.0/sql-programming-guide.html#datasets-and-dataframes



    printTaskLine(1)
    # Task 1 - Load ProCem data
    #
    # Background
    #
    # As part of Tampere University research projects ProCem (https://www.senecc.fi/projects/procem-2) and ProCemPlus (https://www.senecc.fi/projects/procemplus)
    # various data from the Kampusareena building at Hervanta campus was gathered. In addition, data from several other sources were gathered in the projects.
    # The other data sources included, for example, the electricity market prices and the weather measurements from a weather station located at the Sähkötalo'
    # building at Hervanta. The data gathering system developed in the projects is still running and gathering data.
    #
    # A later, still ongoing, research project DELI (https://research.tuni.fi/tase/projects/) has as part of its agenda to research the best ways to manage and
    # share the collected data. In the project some of the ProCem data was uploaded into a Apache IoTDB (https://iotdb.apache.org/) instance to test how well it
    # could be used with the data. IoTDB is a data management system for time series data. Some of the data uploaded to IoTDB is used in tasks 1-3.
    #
    # The IoTDB has a Spark connector plugin that can be used to load data from IoTDB directly into Spark DataFrame. However, to not make things too complicated
    # for the exercise, a ready-made sample of the data has already been extracted and given as a static data for this and the following two tasks.
    #
    # The data
    #
    # The `data/ex3/procem_iotdb.parquet` folder at the repository root contains some ProCem data fetched from IoTDB in Parquet format.
    #
    # Brief explanations on the columns:
    #
    # - `Time`: the UNIX timestamp in millisecond precision
    # - `SolarPower`: the total electricity power produced by the solar panels on Kampusareena (`W`)
    # - `WaterCooling01Power` and `WaterCooling02Power`: the total electricity power used by the two water cooling machineries on Kampusareena (`W`)
    # - `VentilationPower`: the total electricity power used by the ventilation machinery on Kampusareena (`W`)
    # - `Temperature`: the temperature measured by the weather station on top of Sähkötalo (`°C`)
    # - `WindSpeed`: the wind speed measured by the weather station on top of Sähkötalo (`m/s`)
    # - `Humidity`: the humidity measured by the weather station on top of Sähkötalo (`%`)
    # - `ElectricityPrice`: the market price for electricity in Finland (`€/MWh`)
    #
    # Parquet is column-oriented data file format designed for efficient data storage and retrieval. Unlike CSV files the data given in Parquet format is not
    # as easy to preview without Spark. But if really want, for example, the Parquet Visualizer
    # (https://marketplace.visualstudio.com/items?itemName=lucien-martijn.parquet-visualizer) Visual Studio Code extension can be used to browse the data
    # contained in a Parquet file. However, understanding the format is not important for this exercise.
    #
    # The task
    # - Read the data into a DataFrame.
    # - Print out the schema for the resulting DataFrame.
    # - Show/display at least the first 10 rows.

    procemDF: DataFrame = __MISSING__IMPLEMENTATION__

    __MISSING__IMPLEMENTATION__


    # Example output for task 1:
    #
    # root
    # |-- Time: long (nullable = true)
    # |-- SolarPower: double (nullable = true)
    # |-- WaterCooling01Power: double (nullable = true)
    # |-- WaterCooling02Power: double (nullable = true)
    # |-- VentilationPower: double (nullable = true)
    # |-- Temperature: double (nullable = true)
    # |-- WindSpeed: double (nullable = true)
    # |-- Humidity: double (nullable = true)
    # |-- ElectricityPrice: double (nullable = true)
    #
    # +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+
    # |         Time|SolarPower|WaterCooling01Power|WaterCooling02Power|VentilationPower|Temperature|WindSpeed|Humidity|ElectricityPrice|
    # +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+
    # |1716152400000|      NULL|               NULL|               NULL|            NULL|       NULL|     NULL|    NULL|           -0.31|
    # |1716152400168|      NULL|               NULL|               NULL|            NULL|    14.0357|  4.32466| 53.0894|            NULL|
    # |1716152400217|      NULL|               NULL|               NULL|    24744.613281|       NULL|     NULL|    NULL|            NULL|
    # |1716152400277|      NULL|        4370.453613|               NULL|            NULL|       NULL|     NULL|    NULL|            NULL|
    # |1716152400605|      NULL|               NULL|          49.490608|            NULL|       NULL|     NULL|    NULL|            NULL|
    # |1716152400906| -6.500515|               NULL|               NULL|            NULL|       NULL|     NULL|    NULL|            NULL|
    # |1716152401217|      NULL|               NULL|               NULL|    24749.710938|       NULL|     NULL|    NULL|            NULL|
    # |1716152401277|      NULL|        4358.939941|               NULL|            NULL|       NULL|     NULL|    NULL|            NULL|
    # |1716152401607|      NULL|               NULL|          51.819244|            NULL|       NULL|     NULL|    NULL|            NULL|
    # |1716152401906| -6.492893|               NULL|               NULL|            NULL|       NULL|     NULL|    NULL|            NULL|
    # +-------------+----------+-------------------+-------------------+----------------+-----------+---------+--------+----------------+



    printTaskLine(2)
    # Task 2 - Calculate hourly averages
    #
    # Background information:
    #
    # To get the hourly energy from the power: `hourly_energy (kWh) = average_power_for_the_hour (W) / 1000`
    #
    # The market price for electricity in Finland is given separately for each hour and does not change within the hour.
    # Thus, there should be only one value for the price in each hour.
    #
    # The time in the ProCem data is given as UNIX timestamps in millisecond precision, i.e., how many milliseconds has passed since January 1, 1970.
    # `1716152400000` corresponds to `Monday, May 20, 2024 00:00:00.000` in UTC+0300 timezone.
    # Spark offers functions to do the conversion from the timestamps to a more human-readable format.
    #
    # As can be noticed from the data sample in task 1, the data contains a lot of NULL values. These NULL values mean that there is no measurement
    # for that particular timestamp. Both the power and weather measurements are given roughly in one second intervals.
    # Some measurements could be missing from the data, but those are not relevant for this exercise.
    #
    # Using the DataFrame from task 1:
    # - calculate the electrical energy produced by the solar panels for each hour (in `kWh`)
    # - calculate the total combined electrical energy consumed by the water cooling and ventilation machinery for each hour (in `kWh`)
    # - determine the price of the electrical energy for each hour (in `€/MWh`)
    # - calculate the average temperature for each hour (in `°C`)
    #
    # Give the result as a DataFrame where one row contains the hour and the corresponding four values. Order the DataFrame by the hour with the earliest hour first.
    # In the example output, the datetime representation for the hour is given in UTC+0300 timezone which was used in Finland (`Europe/Helsinki`) during May 2024.

    hourlyDF: DataFrame = __MISSING__IMPLEMENTATION__

    hourlyDF2.printSchema()
    hourlyDF2.show(8, False)


    # Example output for task 2:
    #
    # root
    # |-- Time: timestamp (nullable = true)
    # |-- AvgTemperature: double (nullable = true)
    # |-- ProducedEnergy: double (nullable = true)
    # |-- ConsumedEnergy: double (nullable = true)
    # |-- Price: double (nullable = true)
    #
    # +-------------------+------------------+---------------------+------------------+-----+
    # |Time               |AvgTemperature    |ProducedEnergy       |ConsumedEnergy    |Price|
    # +-------------------+------------------+---------------------+------------------+-----+
    # |2024-05-20 00:00:00|13.754773048068902|-0.00583301298888889 |38.668148279083816|-0.31|
    # |2024-05-20 01:00:00|13.209065638888916|-0.005838116895833327|36.66061308699361 |-0.3 |
    # |2024-05-20 02:00:00|11.78702305555553 |-0.005843125166944462|37.91117283675028 |-0.1 |
    # |2024-05-20 03:00:00|10.510957036111114|-0.005829664534999985|35.09305738077221 |-0.03|
    # |2024-05-20 04:00:00|8.989454824999994 |0.0773709636641667   |36.59321714971756 |0.01 |
    # |2024-05-20 05:00:00|8.072131227777806 |0.8554100720902555   |33.8054992999987  |1.41 |
    # |2024-05-20 06:00:00|8.412301513888893 |2.3616457910869446   |54.32778184731544 |4.94 |
    # |2024-05-20 07:00:00|9.190588663888901 |11.20785037204834    |53.04797348656393 |10.44|
    # +-------------------+------------------+---------------------+------------------+-----+



    printTaskLine(3)
    # Task 3 - Calculate daily prices
    #
    # Background information:
    #
    # The energy that is considered to be bought from the electricity market is
    # the difference between the consumed and produced energy.
    #
    # To get the hourly cost for the energy bought from the market:
    #     `hourly_cost (€) = hourly_energy_from_market (kWh) * electricity_price_for_hour (€/MWh) / 1000`
    #
    # Note, that any consumer buying electricity would also have to pay additional fees (taxes, transfer fees, etc.)
    # that are not considered in this exercise.
    # And that the given power consumption is only a part of the overall power consumption at Kampusareena.
    #
    # Using the DataFrame from task 2 as a starting point:
    # - calculate the average daily temperatures (in `°C`)
    # - calculate the total daily energy produced by the solar panels (in `kWh`)
    # - calculate the total daily energy consumed by the water cooling and ventilation machinery (in `kWh`)
    # - calculate the total daily price for the energy that was bought from the electricity market (in `€`)
    #
    # Give the result as a DataFrame where each row contains the date and the corresponding four values rounded to two decimals.
    # Order the DataFrame by the date in chronological order.
    #
    # Finally, calculate the total electricity price for the entire week.

    dailyDF: DataFrame = __MISSING__IMPLEMENTATION__

    dailyDF.show()


    totalPrice: float = __MISSING__IMPLEMENTATION__

    print(f"Total price: {totalPrice} EUR")


    # Example output for task 3:
    #
    # +----------+-----------+--------------+--------------+---------+
    # |      Date|Temperature|ProducedEnergy|ConsumedEnergy|DailyCost|
    # +----------+-----------+--------------+--------------+---------+
    # |2024-05-20|      13.02|         373.9|       1084.76|     9.11|
    # |2024-05-21|      12.91|         369.7|        1154.5|    16.84|
    # |2024-05-22|      17.75|        355.15|       1708.37|    10.63|
    # |2024-05-23|      19.79|        360.76|       1948.03|      5.5|
    # |2024-05-24|      19.68|        258.41|       1978.22|    35.53|
    # |2024-05-25|      20.79|        294.36|       1533.66|     10.8|
    # |2024-05-26|       19.9|        265.03|       1264.05|     3.36|
    # +----------+-----------+--------------+--------------+---------+
    #
    # Total price: 91.77 EUR



    printTaskLine(4)
    # Task 4 - Loading text data into an RDD
    #
    # The `data/ex3/wiki` folder at the repository root contains texts from selected Wikipedia articles in raw text format.
    #
    # - Load all articles into a single RDD. Exclude all empty lines from the RDD.
    # - Count the total number of non-empty lines in the article collection.
    # - Pick the first 8 lines from the created RDD and print them out.

    wikitextsRDD: RDD[str] = __MISSING__IMPLEMENTATION__

    numberOfLines: int = __MISSING__IMPLEMENTATION__
    print(f"The number of lines with text: {numberOfLines}")

    lines8: List[str] = __MISSING__IMPLEMENTATION__

    print("============================================================")
    # Print the first 60 characters of the first 8 lines
    print(*[line[:60] for line in lines8], sep="\n")


    # Example output for task 4:
    #
    # The number of lines with text: 1113
    # ============================================================
    # Artificial intelligence
    # Artificial intelligence (AI), in its broadest sense, is inte
    # Some high-profile applications of AI include advanced web se
    # The various subfields of AI research are centered around par
    # Artificial intelligence was founded as an academic disciplin
    # Goals
    # The general problem of simulating (or creating) intelligence
    # Reasoning and problem-solving



    printTaskLine(5)
    # Task 5 - Counting the number of words
    #
    # Using the RDD from task 4 as a starting point:
    #
    # - Create `wordsRdd` where each row contains one word with the following rules for words:
    #     - The word must have at least one character.
    #     - The word must not contain any numbers, i.e. the digits 0-9.
    # - Calculate the total number of words in the article collection using `wordsRDD`.
    # - Calculate the total number of distinct words in the article collection using the same criteria for the words.
    #
    # You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).
    # In this exercise you can ignore capitalization, parenthesis, and punctuation characters.
    # I.e., `word`, `Word`, `WORD`, `word.`, `(word)`, and `word).` should all be considered as valid and distinct words for this exercise.

    wordsRDD: RDD[str] = __MISSING__IMPLEMENTATION__


    numberOfWords: float = __MISSING__IMPLEMENTATION__
    print(f"The total number of words not containing digits: {numberOfWords}")

    numberOfDistinctWords: float = __MISSING__IMPLEMENTATION__
    print(f"The total number of distinct words not containing digits: {numberOfDistinctWords}")


    # Example output for task 5:
    #
    # The total number of words not containing digits: 44604
    # The total number of distinct words not containing digits: 9463



    printTaskLine(6)
    # Task 6 - Counting word occurrences with RDD
    #
    # - Using the article collection data, create a pair RDD, `wordCountRDD`, where each row contains a distinct word
    #   and the count for how many times the word can be found in the collection.
    #     - Use the same word criteria as in task 5, i.e., ignore words which contain digits.
    #
    # - Using the created pair RDD, find what is the most common 7-letter word that starts with an `s`,
    #   and what is its count in the collection.
    #     - You can modify the given code and find the word and its count separately if that seems easier for you.

    wordCountRDD: RDD[Tuple[str, int]] = __MISSING__IMPLEMENTATION__

    print(f"First row in wordCountRDD: word: '{wordCountRDD.first()[0]}', count: {wordCountRDD.first()[1]}")


    askedWord, wordCount = __MISSING__IMPLEMENTATION__

    print(f"The most common 7-letter word that starts with 's': {askedWord} (appears {wordCount} times)")


    # Example output for task 6:
    #
    # (the values for the first row depend on the code, and can be different than what is shown here)
    #
    # First row in wordCountRDD: word: 'sense,', count: 1
    # The most common 7-letter word that starts with 's': science (appears 93 times)



    printTaskLine(7)
    # Task 7 - Handling text data with DataFrames
    #
    # In this task the same Wikipedia article collection as in the RDD tasks 4-6 is used.
    # The `data/ex3/wiki` folder at the repository root contains texts from selected Wikipedia articles in raw text format.
    #
    # Scala supports Datasets, which are typed DataFrames, and the corresponding Scala task is about using them instead of RDDs to redo tasks 4 and 5.
    # The Datasets are not supported by pyspark, thus this Python task is about using DataFrames instead.
    #
    # Part 1:
    #
    # - Do task 4 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
    #     - Load all articles into a single `DataFrame`. Exclude all empty lines from the DataFrame.
    #     - Count the total number of non-empty lines in the article collection.
    #     - Pick the first 8 lines from the created DataFrame and print them out.
    #
    # Part 2:
    #
    # - Do task 5 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
    # - Using the DataFrame from first part of this task as a starting point:
    #     - Create `wordsDF` where each row contains one word with the following rules for words:
    #         - The word must have at least one character.
    #         - The word must not contain any numbers, i.e. the digits 0-9.
    #     - Calculate the total number of words in the article collection using `wordsDF`.
    #     - Calculate the total number of distinct words in the article collection using the same criteria for the words.
    #
    # You can assume that words in the same line are separated from each other in the article collection by whitespace characters (` `).
    # In this exercise you can ignore capitalization, parenthesis, and punctuation characters.
    # I.e., `word`, `Word`, `word.`, and `(word)` should all be considered as valid and distinct words for this exercise.

    wikitextsDF: DataFrame = __MISSING__IMPLEMENTATION__

    linesInDF: int = __MISSING__IMPLEMENTATION__
    print(f"The number of lines with text: {linesInDF}")

    first8Lines: List[str] = __MISSING__IMPLEMENTATION__

    print("============================================================")
    # Print the first 60 characters of the first 8 lines
    print(*[line[:60] for line in lines8], sep="\n")

    # Use show() to show the first 8 lines
    wikitextsDF.show(8)


    wordsDF: DataFrame = __MISSING__IMPLEMENTATION__


    wordsInDF: int = __MISSING__IMPLEMENTATION__
    print(f"The total number of words not containing digits: {wordsInDF}")

    distinctWordsInDF: int = __MISSING__IMPLEMENTATION__
    print(f"The total number of distinct words not containing digits: {distinctWordsInDF}")


    # Example output for task 7:
    #
    # The number of lines with text: 1113
    # ============================================================
    # Artificial intelligence
    # Artificial intelligence (AI), in its broadest sense, is inte
    # Some high-profile applications of AI include advanced web se
    # The various subfields of AI research are centered around par
    # Artificial intelligence was founded as an academic disciplin
    # Goals
    # The general problem of simulating (or creating) intelligence
    # Reasoning and problem-solving
    # +--------------------+
    # |               value|
    # +--------------------+
    # |Artificial intell...|
    # |Artificial intell...|
    # |Some high-profile...|
    # |The various subfi...|
    # |Artificial intell...|
    # |               Goals|
    # |The general probl...|
    # |Reasoning and pro...|
    # +--------------------+
    #
    # and
    #
    # The total number of words not containing digits: 44604
    # The total number of distinct words not containing digits: 9463



    printTaskLine(8)
    # Task 8 - Counting word occurrences with DataFrame
    #
    # Do task 6 again, but this time using the higher level `DataFrame` instead of the low level `RDD`.
    # Also, this time you should use the given data class `WordCount` for the final result
    # instead of `(str, int)` tuple like in task 6 with the RDD.
    #
    # - Using the article collection data, create a `DataFrame`, `wordCountDF`,
    #   where each row contains a distinct word and the count for how many times the word can be found in the collection.
    #     - Use the same word criteria as in task 7, i.e., ignore words which contain digits.
    #     - Have the column names in the DataFrame match the data class `WordCount`, i.e., `word` and `count`.
    #
    # - Using the created DataFrame, find what is the most common 7-letter word that starts with an `s`,
    #   and what is its count in the collection.
    #     - The straightforward way is to determine the word and its count together.
    #       However, you can determine the word and its count separately if you want.
    #     - Give the final result as an instance of the data class.

    @dataclasses.dataclass(frozen=True)
    class WordCount:
        word: str
        count: int


    wordCountDF: DataFrame = __MISSING__IMPLEMENTATION__

    print(f"First row in wordCountDF: word: '{wordCountDF.first()['word']}', count: {wordCountDF.first()['count']}")


    askedWordCount: WordCount = __MISSING__IMPLEMENTATION__

    print(f"Type of askedWordCount: {type(askedWordCount)}")
    print(f"The most common 7-letter word that starts with 's': {askedWordCount.word} (appears {askedWordCount.count} times)")


    # Example output for task 8:
    #
    # (the values for the first row depend on the code, and can be different than what is shown here)
    #
    # First row in wordCountDF: word: 'By', count: 12
    # Type of askedWordCount: <class '__main__.main.<locals>.WordCount'>
    # The most common 7-letter word that starts with 's': science (appears 93 times)



    # Stop the Spark session (DO NOT do this in Databricks!)
    spark.stop()


# Helper function to separate the task outputs from each other
def printTaskLine(taskNumber: int) -> None:
    print(f"======\nTask {taskNumber}\n======")


if __name__ == "__main__":
    main()
