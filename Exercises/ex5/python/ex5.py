"""Exercise 5 for Data-Intensive Programming"""

from functools import reduce
from glob import glob
from pathlib import Path
from typing import List

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# some helper functions used in this exercise

def getFileList(path: str) -> List[Path]:
    return [Path(filename) for filename in glob(f"{path}/*") + glob(f"{path}/.*")]

def sizeInMB(sizeInBytes: int) -> float:
    return round(sizeInBytes/1024.0/1024.0, 2)

def sizeInKB(filePath: str) -> float:
    return round(sum([file_info.stat().st_size for file_info in getFileList(filePath)])/1024.0, 2)

# print the files and their sizes from the target path
def printStorage(path: str) -> None:
    def getStorageSize(currentPath: str) -> float:
        # using Databricks utilities to get the list of the files in the path
        file_paths: List[Path] = getFileList(currentPath)
        sizes: List[int] = []
        for file_path in file_paths:
            if file_path.is_dir():
                sizes.append(getStorageSize(str(file_path)))
            else:
                print(f"{sizeInMB(file_path.stat().st_size)} MB --- {file_path}")
                sizes.append(file_path.stat().st_size)
        return sum(sizes)

    sizeInBytes: float = getStorageSize(path)
    print(f"Total size: {sizeInMB(sizeInBytes)} MB")

# remove all files and folders from the target path
def cleanTargetFolder(path: str) -> None:
    for file_path in getFileList(path):
        if file_path.is_dir():
            cleanTargetFolder(str(file_path))
            file_path.rmdir()
        else:
            file_path.unlink()

# Print column types in a nice format
def printColumnTypes(inputDF: DataFrame) -> None:
    for column_name, column_type in inputDF.dtypes:
        print(f"{column_name}: {column_type}")

# Returns a limited sample of the input data frame
def getTestDF(inputDF: DataFrame, ids: List[str] = ["Z1", "Z2"], limitRows: int = 2) -> DataFrame:
    origSample: DataFrame = inputDF \
        .filter(~F.col("ID").contains("_")) \
        .limit(limitRows)
    extraSample: DataFrame = reduce(
        lambda df1, df2: df1.union(df2),
        [inputDF.filter(F.col("ID").endswith(id)).limit(limitRows) for id in ids]
     )

    return origSample.union(extraSample)



def main():
    # In Databricks, the Spark session is created automatically, and you should not create it yourself.
    builder = SparkSession.builder \
        .appName("ex5-solution") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("ERROR")

    # reduce the number of shuffle partitions from the default 200 to have more efficient local execution
    spark.conf.set("spark.sql.shuffle.partitions", "5")



    # COMP.CS.320 Data-Intensive Programming, Exercise 5
    #
    # This exercise demonstrates different file formats (CSV, Parquet, Delta) and some of the operations that can be used with them.
    # - Tasks 1-5 concern reading and writing operations with CSV and Parquet
    # - Tasks 6-8 introduces the Delta format
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
    # - Chapters 4 and 9 in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
    #     - There are additional code examples in the related GitHub repository: https://github.com/databricks/LearningSparkV2
    #     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    #       `https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc`
    # - Apache Spark documentation on all available functions that can be used on DataFrames:
    #     https://spark.apache.org/docs/3.5.0/sql-ref-functions.html
    # - The full Spark Python functions API listing for the functions package might have some additional functions listed
    #   that have not been updated in the documentation:
    #     https://spark.apache.org/docs/3.5.0/api/python/reference/pyspark.sql/functions.html
    # - Databricks documentation pages:
    #   - What is Delta Lake?: https://docs.databricks.com/en/delta/index.html
    #   - Delta Lake tutorial: https://docs.databricks.com/en/delta/tutorial.html
    #   - Upsert into Delta Lake: https://docs.databricks.com/en/delta/merge.html#modify-all-unmatched-rows-using-merge
    # - The Delta Spark Python DeltaTable documentation:
    #     https://docs.delta.io/latest/api/python/spark/index.html



    printTaskLine(1)
    # Task 1 - Read data in two formats
    #
    # The `data/ex5` folder at the repository root contains data about car accidents in the USA.
    # The same data is given in multiple formats, and it is a subset of the dataset in Kaggle:
    #     https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents
    #
    # In this task read the data into data frames from both CSV and Parquet source format. The CSV files use `|` as the column separator.
    #
    # Code for displaying the data and information about the source files is already included.

    source_path: str = "../../data/ex5/"
    target_path: str = "data/"
    data_name: str = "accidents"


    source_csv_folder: str = source_path + f"{data_name}_csv"

    # create and display the data from CSV source
    df_csv: DataFrame = __MISSING__IMPLEMENTATION__

    df_csv.show(5, False)


    # Typically, a some more suitable file format would be used, like Parquet.
    # With Parquet column format is stored in the file itself, so it does not need to be given.
    source_parquet_folder: str = source_path + f"{data_name}_parquet"

    # create and display the data from Parquet source
    df_parquet: DataFrame = __MISSING__IMPLEMENTATION__

    df_parquet.show(5, False)


    # print the list of files for the different file formats
    print("CSV files:")
    printStorage(source_csv_folder)

    print("\nParquet files:")
    printStorage(source_parquet_folder)

    # The schemas for both data frames should be the same (as long as the type inferring for the CSV files has worked correctly)
    print("\n== CSV types ==")
    printColumnTypes(df_csv)
    print("\n== Parquet types ==")
    printColumnTypes(df_parquet)


    # Example output for task 1
    #
    # (the same output for both displays):
    #
    # +---------+-------------------+-------------------+---------------------------------------------------------------------------------+---------+------+-----+-------------+
    # |ID       |Start_Time         |End_Time           |Description                                                                      |City     |County|State|Temperature_F|
    # +---------+-------------------+-------------------+---------------------------------------------------------------------------------+---------+------+-----+-------------+
    # |A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Lane blocked.   |Whitehall|Lehigh|PA   |31.0         |
    # |A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Lane blocked.   |Whitehall|Lehigh|PA   |31.0         |
    # |A-3558713|2016-01-14 20:18:33|2017-01-30 13:55:44|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Open.           |Whitehall|Lehigh|PA   |31.0         |
    # |A-3572241|2016-01-14 20:18:33|2017-02-17 23:22:00|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Lane blocked.   |Whitehall|Lehigh|PA   |31.0         |
    # |A-3572395|2016-01-14 20:18:33|2017-02-19 00:38:00|Closed at Fullerton Ave - Road closed due to accident. Roadwork. Traffic problem.|Whitehall|Lehigh|PA   |31.0         |
    # +---------+-------------------+-------------------+---------------------------------------------------------------------------------+---------+------+-----+-------------+
    # only showing top 5 rows
    #
    # and
    #
    # CSV files:
    # 31.97 MB --- ../../data/ex5/accidents_csv/us_traffic_accidents.csv
    # Total size: 31.97 MB

    # Parquet files:
    # 9.56 MB --- ../../data/ex5/accidents_parquet/us_traffic_accidents.parquet
    # Total size: 9.56 MB

    # == CSV types ==
    # ID: string
    # Start_Time: timestamp
    # End_Time: timestamp
    # Description: string
    # City: string
    # County: string
    # State: string
    # Temperature_F: double

    # == Parquet types ==
    # ID: string
    # Start_Time: timestamp
    # End_Time: timestamp
    # Description: string
    # City: string
    # County: string
    # State: string
    # Temperature_F: double



    printTaskLine(2)
    # Task 2 - Write the data to new storage
    #
    # Write the data from `df_csv` and `df_parquet` to the data folder within this project in both CSV and Parquet formats.
    #
    # Note, since the data in both data frames should be the same, you should be able to use either one as the source
    # when writing it to a new folder (regardless of the target format).

    # remove all previously written files from the target folder first
    cleanTargetFolder(target_path)

    # the target paths for both CSV and Parquet
    target_file_csv: str = target_path + data_name + "_csv"
    target_file_parquet: str = target_path + data_name + "_parquet"

    # write the data from task 1 in CSV format to the path given by target_file_csv
    __MISSING__IMPLEMENTATION__

    # write the data from task 1 in Parquet format to the path given by target_file_parquet
    __MISSING__IMPLEMENTATION__

    # Check the written files:
    printStorage(target_file_csv)
    printStorage(target_file_parquet)

    # Both with CSV and Parquet, the data can be divided into multiple files depending on how many workers were doing the writing.
    # If a single file is needed, you can force the output into a single file with "coalesce(1)" before the write command
    # This will make the writing less efficient, especially for larger datasets. (and is not needed in this exercise)
    # There are some additional small metadata files (_SUCCESS, _committed, _started, .crc) that you can ignore in this exercise.


    # Example output for task 2
    #
    # (note that the number of files and the exact filenames will be different for you):
    #
    # 4.26 MB --- data/accidents_csv/part-00001-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv
    # 4.19 MB --- data/accidents_csv/part-00007-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv
    # 4.25 MB --- data/accidents_csv/part-00002-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv
    # 0.0 MB --- data/accidents_csv/_SUCCESS
    # 4.25 MB --- data/accidents_csv/part-00003-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv
    # 4.26 MB --- data/accidents_csv/part-00000-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv
    # 4.22 MB --- data/accidents_csv/part-00004-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv
    # 4.21 MB --- data/accidents_csv/part-00005-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv
    # 4.22 MB --- data/accidents_csv/part-00006-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv
    # 0.03 MB --- data/accidents_csv/.part-00003-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv.crc
    # 0.03 MB --- data/accidents_csv/.part-00004-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv.crc
    # 0.03 MB --- data/accidents_csv/.part-00002-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv.crc
    # 0.03 MB --- data/accidents_csv/.part-00007-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv.crc
    # 0.03 MB --- data/accidents_csv/.part-00000-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv.crc
    # 0.03 MB --- data/accidents_csv/.part-00005-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv.crc
    # 0.03 MB --- data/accidents_csv/.part-00006-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv.crc
    # 0.0 MB --- data/accidents_csv/._SUCCESS.crc
    # 0.03 MB --- data/accidents_csv/.part-00001-8b152853-1222-4427-a942-3ea38b8e1451-c000.csv.crc
    # Total size: 34.12 MB
    # 0.0 MB --- data/accidents_parquet/_SUCCESS
    # 9.55 MB --- data/accidents_parquet/part-00001-aa685dfb-e695-4fac-98fa-82fc701402db-c000.snappy.parquet
    # 0.0 MB --- data/accidents_parquet/part-00000-aa685dfb-e695-4fac-98fa-82fc701402db-c000.snappy.parquet
    # 0.0 MB --- data/accidents_parquet/.part-00000-aa685dfb-e695-4fac-98fa-82fc701402db-c000.snappy.parquet.crc
    # 0.07 MB --- data/accidents_parquet/.part-00001-aa685dfb-e695-4fac-98fa-82fc701402db-c000.snappy.parquet.crc
    # 0.0 MB --- data/accidents_parquet/._SUCCESS.crc
    # Total size: 9.63 MB



    printTaskLine(3)
    # Task 3 - Add new rows to storage
    #
    # First create a new data frame based on the task 1 data that contains the `73` latest incidents
    #   (based on the starting time) in the city of `Los Angeles`.
    # The IDs of the incidents of this data frame should have an added postfix `_Z1`,
    # e.g., `A-3666323` should be replaced with `A-3666323_Z1`.
    #
    # Then append these new 73 rows to both the CSV storage and Parquet storage.
    # I.e., write the new rows in append mode in CSV format to folder given by `target_file_csv`
    # and in Parquet format to folder given by `target_file_parquet`.
    #
    # Finally, read the data from the storages again to check that the appending was successful.

    more_rows_count: int = 73

    # Create a data frame that holds some new rows that will be appended to the storage
    __MISSING__IMPLEMENTATION__

    df_new_rows.show(2)


    # Append the new rows to CSV storage:
    # important to consistently use the same header and column separator options when using CSV storage
    __MISSING__IMPLEMENTATION__

    # Append the new rows to Parquet storage:
    __MISSING__IMPLEMENTATION__


    # Read the merged data from the CSV files to check that the new rows have been stored
    df_new_csv: DataFrame = __MISSING__IMPLEMENTATION__

    # Read the merged data from the Parquet files to check that the new rows have been stored
    df_new_parquet: DataFrame = __MISSING__IMPLEMENTATION__


    old_rows: int = df_parquet.count()
    new_rows: int = df_new_rows.count()
    print(f"Old DF had {old_rows} rows and we are adding {new_rows} rows => we should have {old_rows + new_rows} in the merged data.")

    print(f"Old CSV DF had {df_csv.count()} rows and new DF has {df_new_csv.count()}.")
    print(f"Old Parquet DF had {df_parquet.count()} rows and new DF has {df_new_parquet.count()}.")


    # Example output for task 3:
    #
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    # |          ID|         Start_Time|           End_Time|         Description|       City|     County|State|Temperature_F|
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    # |A-3666323_Z1|2023-03-29 05:48:30|2023-03-29 07:55:41|San Diego Fwy S -...|Los Angeles|Los Angeles|   CA|         49.0|
    # |A-3657191_Z1|2023-03-23 11:37:30|2023-03-23 13:45:00|CA-134 W - Ventur...|Los Angeles|Los Angeles|   CA|         58.0|
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    # only showing top 2 rows
    #
    # and
    #
    # Old DF had 198082 rows and we are adding 73 rows => we should have 198155 in the merged data.
    # Old CSV DF had 198082 rows and new DF has 198155.
    # Old Parquet DF had 198082 rows and new DF has 198155.



    printTaskLine(4)
    # Task 4 - Append modified rows
    #
    # In the previous task, appending new rows was successful because the new data had the same schema as the original data.
    #
    # In this task we try to append data with a modified schema to the CSV and Parquet storages.
    #
    # First create a new data frame based on `df_new_rows` from task 3. The data frame should be modified in the following way:
    #
    # - The values in the `ID` column should have a postfix `_Z2` instead of `_Z1`. E.g., `A-3877306_Z1` should be replaced with `A-3877306_Z2`.
    # - A new column `AddedColumn1` should be added with values `"prefix-CITY"` where `CITY` is replaced by the city of the incident.
    # - A new column `AddedColumn2` should be added with a constant value `"New column"`.
    # - The column `Temperature_F` should be renamed to `Temperature_C` and the Fahrenheit values should be transformed to Celsius values.
    #     - Example of the temperature transformation: `49.0 °F` = `(49.0 - 32) / 9 * 5 °C` = `9.4444 °C`
    # - The column `Description` should be dropped.
    #
    # Then append these modified rows to both the CSV storage and the Parquet storage.

    # Some new rows that have different columns
    df_modified: DataFrame = __MISSING__IMPLEMENTATION__

    df_modified.show(2)


    # Append the new modified rows to CSV storage:
    __MISSING__IMPLEMENTATION__

    # Append the new modified rows to Parquet storage:
    __MISSING__IMPLEMENTATION__


    # Example output for task 4:
    #
    # +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    # |          ID|         Start_Time|           End_Time|       City|     County|State|     Temperature_C|      AddedColumn1|AddedColumn2|
    # +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    # |A-3666323_Z2|2023-03-29 05:48:30|2023-03-29 07:55:41|Los Angeles|Los Angeles|   CA| 9.444444444444445|prefix-Los Angeles|  New column|
    # |A-3657191_Z2|2023-03-23 11:37:30|2023-03-23 13:45:00|Los Angeles|Los Angeles|   CA|14.444444444444445|prefix-Los Angeles|  New column|
    # +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    # only showing top 2 rows



    printTaskLine(5)
    # Task 5 - Check the merged data
    #
    # In this task we check the contents of the CSV and Parquet storages after the two data append operations,
    # the new rows with the same schema in task 3, and the new rows with a modified schema in task 4.
    #
    # Part 1:
    #
    # The task is to first write the code that loads the data from the storages again.
    # And then run the given test code that shows the number of rows, columns, schema, and some sample rows.
    #
    # Part 2:
    #
    # Finally, answer the questions in the final cell of this task.

    # CSV should have been broken in some way
    # Read in the CSV data again from the CSV storage: target_file_csv
    modified_csv_df: DataFrame = __MISSING__IMPLEMENTATION__

    print("== CSV storage:")
    print(f"The number of rows should be correct: {modified_csv_df.count()} (i.e., {df_csv.count()}+2*{more_rows_count})")
    print(f"However, the original data had {len(df_csv.columns)} columns, inserted data had {len(df_modified.columns)} columns. Afterwards we have {len(modified_csv_df.columns)} columns while we should have {len(df_csv.columns) + 3} distinct columns.")

    # show two example rows from each addition
    getTestDF(modified_csv_df).show()

    printColumnTypes(modified_csv_df)


    # Read in the Parquet data again from the Parquet storage: target_file_parquet
    modified_parquet_df: DataFrame = __MISSING__IMPLEMENTATION__

    print("== Parquet storage:")
    print(f"The count for number of rows seems wrong: {df_parquet.count()} (should be: {df_parquet.count()}+2*{more_rows_count})")
    print(f"Actually all {df_parquet.count()+2*more_rows_count} rows should be included but the 2 conflicting schemas can cause the count to be incorrect.")
    print(f"The original data had {len(df_parquet.columns)} columns, inserted data had {len(df_modified.columns)} columns. Afterwards we have {len(modified_parquet_df.columns)} columns while we should have {len(df_parquet.columns) + 3} distinct columns.")

    # show two example rows from each addition
    getTestDF(modified_parquet_df).show()

    print("Unlike the CSV case, the data types for the columns have not been affected. But some columns are just ignored.")
    printColumnTypes(modified_parquet_df)


    # Did you get similar output for the data in CSV storage? If not, what was the difference? ???
    #
    # What is your explanation/guess for why the CSV seems broken and the schema cannot be inferred anymore? ???
    #
    # Did you get similar output for the data in Parquet storage, and which of the 2 alternatives? If not, what was the difference? ???
    #
    # What is your explanation/guess for why not all 11 distinct columns are included in the data frame in the Parquet case? ???


    # Example output for task 5:
    #
    # For CSV:
    #
    # == CSV storage:
    # The number of rows should be correct: 198228 (i.e., 198082+2*73)
    # However, the original data had 8 columns, inserted data had 9 columns. Afterwards we have 8 columns while we should have 11 distinct columns.
    # +------------+--------------------+--------------------+--------------------+-----------+-----------+------------------+------------------+
    # |          ID|          Start_Time|            End_Time|         Description|       City|     County|             State|     Temperature_F|
    # +------------+--------------------+--------------------+--------------------+-----------+-----------+------------------+------------------+
    # |   A-3558690|2016-01-14T22:18:...|2017-01-30T15:25:...|Closed at Fullert...|  Whitehall|     Lehigh|                PA|              31.0|
    # |   A-3558700|2016-01-14T22:18:...|2017-01-30T15:34:...|Closed at Fullert...|  Whitehall|     Lehigh|                PA|              31.0|
    # |A-3666323_Z1|2023-03-29T08:48:...|2023-03-29T10:55:...|San Diego Fwy S -...|Los Angeles|Los Angeles|                CA|              49.0|
    # |A-3657191_Z1|2023-03-23T13:37:...|2023-03-23T15:45:...|CA-134 W - Ventur...|Los Angeles|Los Angeles|                CA|              58.0|
    # |A-3666323_Z2|2023-03-29T08:48:...|2023-03-29T10:55:...|         Los Angeles|Los Angeles|         CA| 9.444444444444445|prefix-Los Angeles|
    # |A-3657191_Z2|2023-03-23T13:37:...|2023-03-23T15:45:...|         Los Angeles|Los Angeles|         CA|14.444444444444445|prefix-Los Angeles|
    # +------------+--------------------+--------------------+--------------------+-----------+-----------+------------------+------------------+
    #
    # ID: string
    # Start_Time: string
    # End_Time: string
    # Description: string
    # City: string
    # County: string
    # State: string
    # Temperature_F: string
    #
    # and for Parquet (alternative 1):
    #
    # == Parquet storage:
    # The count for number of rows seems wrong: 198082 (should be: 198082+2*73)
    # Actually all 198228 rows should be included but the 2 conflicting schemas can cause the count to be incorrect.
    # The original data had 8 columns, inserted data had 9 columns. Afterwards we have 8 columns while we should have 11 distinct columns.
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    # |          ID|         Start_Time|           End_Time|         Description|       City|     County|State|Temperature_F|
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    # |   A-3558690|2016-01-14 22:18:33|2017-01-30 15:25:19|Closed at Fullert...|  Whitehall|     Lehigh|   PA|         31.0|
    # |   A-3558700|2016-01-14 22:18:33|2017-01-30 15:34:02|Closed at Fullert...|  Whitehall|     Lehigh|   PA|         31.0|
    # |A-3666323_Z1|2023-03-29 08:48:30|2023-03-29 10:55:41|San Diego Fwy S -...|Los Angeles|Los Angeles|   CA|         49.0|
    # |A-3657191_Z1|2023-03-23 13:37:30|2023-03-23 15:45:00|CA-134 W - Ventur...|Los Angeles|Los Angeles|   CA|         58.0|
    # |A-3666323_Z2|2023-03-29 08:48:30|2023-03-29 10:55:41|                NULL|Los Angeles|Los Angeles|   CA|         NULL|
    # |A-3657191_Z2|2023-03-23 13:37:30|2023-03-23 15:45:00|                NULL|Los Angeles|Los Angeles|   CA|         NULL|
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+
    #
    # Unlike the CSV case, the data types for the columns have not been affected. But some columns are just ignored.
    # ID: string
    # Start_Time: timestamp
    # End_Time: timestamp
    # Description: string
    # City: string
    # County: string
    # State: string
    # Temperature_F: double
    #
    # Parquet (alternative 2):
    #
    # == Parquet storage:
    # The count for number of rows seems wrong: 198082 (should be: 198082+2*73)
    # Actually all 198228 rows should be included but the 2 conflicting schemas can cause the count to be incorrect.
    # The original data had 8 columns, inserted data had 9 columns. Afterwards we have 9 columns while we should have 11 distinct columns.
    # +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    # |          ID|         Start_Time|           End_Time|       City|     County|State|     Temperature_C|      AddedColumn1|AddedColumn2|
    # +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    # |   A-3558690|2016-01-14 22:18:33|2017-01-30 15:25:19|  Whitehall|     Lehigh|   PA|              NULL|              NULL|        NULL|
    # |   A-3558700|2016-01-14 22:18:33|2017-01-30 15:34:02|  Whitehall|     Lehigh|   PA|              NULL|              NULL|        NULL|
    # |A-3666323_Z1|2023-03-29 08:48:30|2023-03-29 10:55:41|Los Angeles|Los Angeles|   CA|              NULL|              NULL|        NULL|
    # |A-3657191_Z1|2023-03-23 13:37:30|2023-03-23 15:45:00|Los Angeles|Los Angeles|   CA|              NULL|              NULL|        NULL|
    # |A-3666323_Z2|2023-03-29 08:48:30|2023-03-29 10:55:41|Los Angeles|Los Angeles|   CA| 9.444444444444445|prefix-Los Angeles|  New column|
    # |A-3657191_Z2|2023-03-23 13:37:30|2023-03-23 15:45:00|Los Angeles|Los Angeles|   CA|14.444444444444445|prefix-Los Angeles|  New column|
    # +------------+-------------------+-------------------+-----------+-----------+-----+------------------+------------------+------------+
    #
    # Unlike the CSV case, the data types for the columns have not been affected. But some columns are just ignored.
    # ID: string
    # Start_Time: timestamp
    # End_Time: timestamp
    # City: string
    # County: string
    # State: string
    # Temperature_C: double
    # AddedColumn1: string
    # AddedColumn2: string



    printTaskLine(6)
    # Task 6 - Delta - Reading and writing data
    #
    # Delta tables, https://docs.databricks.com/en/delta/index.html, are a storage format that are more advanced.
    # They can be used somewhat like databases.
    #
    # This is not native in Spark, but open format which is more and more commonly used.
    #
    # Delta is more strict with data. We cannot for example have whitespace in column names as you can have in Parquet and CSV.
    # However, in this exercise the example data is given with column names where these additional requirements have already been fulfilled.
    #And thus, you don't have to worry about them in this exercise.
    #
    # Delta technically looks more or less like Parquet with some additional metadata files.
    #
    # In this this task read the source data given in Delta format into a data frame.
    # And then write a copy of the data into the students container to allow modifications in the following tasks.

    source_delta_folder: str = source_path + f"{data_name}_delta"

    # Read the original data in Delta format to a data frame
    df_delta: DataFrame = __MISSING__IMPLEMENTATION__

    print(f"== Number or rows: {df_delta.count()}")
    print("== Columns:")
    printColumnTypes(df_delta)
    print("== Storage files:")
    printStorage(source_delta_folder)


    target_file_delta: str = target_path + data_name + "_delta"

    # write the data from df_delta using the Delta format to the path given by target_file_delta
    __MISSING__IMPLEMENTATION__

    # Check the written files:
    print("== Target files:")
    printStorage(target_file_delta)


    # Example output from task 6:
    #
    # == Number or rows: 198082
    # == Columns:
    # ID: string
    # Start_Time: timestamp
    # End_Time: timestamp
    # Description: string
    # City: string
    # County: string
    # State: string
    # Temperature_F: double
    # == Storage files:
    # 9.56 MB --- ../../data/ex5/accidents_delta/part-00000-35a096d7-a0ee-439a-85e0-aa78e2935f39-c000.snappy.parquet
    # 0.0 MB --- ../../data/ex5/accidents_delta/_delta_log/00000000000000000000.json
    # 0.0 MB --- ../../data/ex5/accidents_delta/_delta_log/00000000000000000000.crc
    # Total size: 9.56 MB
    # == Target files:
    # 9.55 MB --- data/accidents_delta/part-00001-84b9816e-d2fe-4fff-a4e1-3aeaeabbb237-c000.snappy.parquet
    # 0.0 MB --- data/accidents_delta/_delta_log/00000000000000000000.json
    # 0.0 MB --- data/accidents_delta/_delta_log/.00000000000000000000.json.crc
    # 0.0 MB --- data/accidents_delta/part-00000-c7384808-f806-4843-8a78-e2b9911bdb14-c000.snappy.parquet
    # 0.0 MB --- data/accidents_delta/.part-00000-c7384808-f806-4843-8a78-e2b9911bdb14-c000.snappy.parquet.crc
    # 0.07 MB --- data/accidents_delta/.part-00001-84b9816e-d2fe-4fff-a4e1-3aeaeabbb237-c000.snappy.parquet.crc
    # Total size: 9.63 MB



    printTaskLine(7)
    # Task 7 - Delta - Appending data
    #
    # Add the new rows using the same schema, `df_new_rows` from task 3,
    # and the new rows using the modified schema, `df_modified` from task 4, to the Delta storage.
    #
    # Then, read the merged data and study whether the result with Delta is correct without lost or invalid data.

    # Append the new rows using the same schema, df_new_rows, to the Delta storage:
    __MISSING__IMPLEMENTATION__


    # By default, Delta is similar to Parquet. However, we can enable it to handle schema modifications.
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Append the new rows using the modified schema, df_modified, to the Delta storage:
    __MISSING__IMPLEMENTATION__


    # Read the merged data from Delta storage to check that the new rows have been stored
    modified_delta_df: DataFrame = __MISSING__IMPLEMENTATION__

    print(f"The number of rows should be correct: {modified_delta_df.count()} (i.e., {df_delta.count()}+2*{more_rows_count})")
    print(f"The original data had {len(modified_delta_df.columns)} columns, inserted data had {len(df_modified.columns)} columns. Afterwards we have {len(modified_delta_df.columns)} columns while we should have {len(df_delta.columns) + 3} distinct columns.")
    print("Delta handles these perfectly. The columns which were not given values are available with NULL values.")

    # show two example rows from each addition
    getTestDF(modified_delta_df).show()

    printColumnTypes(modified_delta_df)


    # Example output from task 7:
    #
    # The number of rows should be correct: 198228 (i.e., 198082+2*73)
    # The original data had 11 columns, inserted data had 9 columns. Afterwards we have 11 columns while we should have 11 distinct columns.
    # Delta handles these perfectly. The columns which were not given values are available with NULL values.
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+------------------+------------------+------------+
    # |          ID|         Start_Time|           End_Time|         Description|       City|     County|State|Temperature_F|     Temperature_C|      AddedColumn1|AddedColumn2|
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+------------------+------------------+------------+
    # |   A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Closed at Fullert...|  Whitehall|     Lehigh|   PA|         31.0|              NULL|              NULL|        NULL|
    # |   A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Closed at Fullert...|  Whitehall|     Lehigh|   PA|         31.0|              NULL|              NULL|        NULL|
    # |A-3666323_Z1|2023-03-29 05:48:30|2023-03-29 07:55:41|San Diego Fwy S -...|Los Angeles|Los Angeles|   CA|         49.0|              NULL|              NULL|        NULL|
    # |A-3657191_Z1|2023-03-23 11:37:30|2023-03-23 13:45:00|CA-134 W - Ventur...|Los Angeles|Los Angeles|   CA|         58.0|              NULL|              NULL|        NULL|
    # |A-3666323_Z2|2023-03-29 05:48:30|2023-03-29 07:55:41|                NULL|Los Angeles|Los Angeles|   CA|         NULL| 9.444444444444445|prefix-Los Angeles|  New column|
    # |A-3657191_Z2|2023-03-23 11:37:30|2023-03-23 13:45:00|                NULL|Los Angeles|Los Angeles|   CA|         NULL|14.444444444444445|prefix-Los Angeles|  New column|
    # +------------+-------------------+-------------------+--------------------+-----------+-----------+-----+-------------+------------------+------------------+------------+
    #
    # ID: string
    # Start_Time: timestamp
    # End_Time: timestamp
    # Description: string
    # City: string
    # County: string
    # State: string
    # Temperature_F: double
    # Temperature_C: double
    # AddedColumn1: string
    # AddedColumn2: string



    printTaskLine(8)
    # Task 8 - Delta - Full modifications
    #
    # With CSV or Parquet editing existing values is not possible without overwriting the entire dataset.
    #
    # Previously we only added new lines. This way of working only supports adding new data. It does **not** support modifying existing data: updating values or deleting rows.
    # (We could do that manually by adding a primary key and timestamp and always searching for the newest value.)
    #
    # Nevertheless, Delta tables take care of this and many more itself.
    #
    # This task is divided into four parts with separate instructions for each cell.
    # The first three parts ask for some code, and the final part is just for testing.
    #
    # Part 1: In the following cell, add the code to write the `df_delta_small` into Delta storage.

    # Let us first save a smaller data so that it is easier to see what is happening
    delta_table_file: str = target_path + data_name + "_deltatable_small"

    # create a small 6 row data frame with only 5 columns
    df_delta_small: DataFrame = getTestDF(modified_delta_df, ["Z1"], 3) \
        .drop("Description", "End_Time", "County", "AddedColumn1", "AddedColumn2")


    # Write the new small data frame to storage in Delta format to path based on delta_table_file
    __MISSING__IMPLEMENTATION__


    # Create Delta table based on your target folder
    deltatable: DeltaTable = DeltaTable.forPath(spark, delta_table_file)

    # Show the data before the merge that is done in the next part
    print(f"== Before merge, the size of Delta file is {sizeInKB(delta_table_file)} kB and contains {deltatable.toDF().count()} rows.")
    deltatable.toDF().sort(F.desc("Start_Time")).show()


    # Delta tables are more like database type tables. We define them based on data and modify the table itself.
    #
    # We do this by telling Delta what is the primary key of the data.
    # After this we tell it to "merge" new data to the old one.
    # If primary key matches, we update the information. If primary key is new, add a row.
    #
    # Part 2: In the following, add the code to update the `deltatable` with the given updates in `df_delta_update`.
    # The rows should be updated when the `ID` columns match.
    # And if the id from the update is a new one, a new row should be inserted into the `deltatable`.

    # create a 5 row data frame with the same columns with updated values for the temperature
    df_delta_update: DataFrame = df_new_rows \
        .limit(5) \
        .drop("Description", "End_Time", "County") \
        .withColumn("Temperature_F", F.round(F.rand(seed=1) * 100, 1))


    # code for updating the deltatable with df_delta_update
    __MISSING__IMPLEMENTATION__


    # Show the data after the merge
    print(f"== After merge, the size of Delta file is {sizeInKB(delta_table_file)} kB and contains {deltatable.toDF().count()} rows.")
    deltatable.toDF().sort(F.desc("Start_Time")).show()

    # Note: If you execute the previous code multiple times, you can notice that the file size increases every time.
    # However, the amount of rows does not change.


    # Part 3: As a second modification to the test data, do the following modifications to the `deltatable`:
    # - Fill out the proper temperature values given in Celsius degrees for column `Temperature_C` for all incidents the delta table.
    # - Remove all rows where the temperature in Celsius is below `-12 °C`.

    # code for updating the deltatable with the Celsius temperature values
    __MISSING__IMPLEMENTATION__

    # code for removing rows where the temperature is below -12 Celsius degrees from the deltatable
    __MISSING__IMPLEMENTATION__


    # Show the data after the second update
    print(f"== After the second update, the size of Delta file is {sizeInKB(delta_table_file)} kB and contains {deltatable.toDF().count()} rows.")
    deltatable.toDF().sort(F.desc("Start_Time")).show()


    # Part 4: Run the following code as a demonstration on how the additional and unused data can be removed from the storage.
    #
    # The modifications and removals are actually physically done only once something like the vacuuming shown below is done.
    # Before that, the original data still exist in the original files.

    # We can get rid of additional or unused data from the storage by vacuuming
    print(f"== Before vacuum, the size of Delta file is {sizeInKB(delta_table_file)} kB.")

    # Typically we do not want to vacuum all the data, only data older than 30 days or so.
    # We need to tell Delta that we really want to do something stupid
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
    deltatable.vacuum(0)
    print(f"== After vacuum, the size of Delta file is {sizeInKB(delta_table_file)} kB.")

    # you can print the files after vacuuming by uncommenting the following
    # printStorage(delta_table_file)

    # the vacuuming should not change the actual data
    # deltatable.toDF().sort(F.desc("Start_Time")).show()


    # Example output from task 8:
    #
    # == Before merge, the size of Delta file is 7.68 kB and contains 6 rows.
    # +------------+-------------------+-----------+-----+-------------+-------------+
    # |          ID|         Start_Time|       City|State|Temperature_F|Temperature_C|
    # +------------+-------------------+-----------+-----+-------------+-------------+
    # |A-3666323_Z1|2023-03-29 08:48:30|Los Angeles|   CA|         49.0|         NULL|
    # |A-3657191_Z1|2023-03-23 13:37:30|Los Angeles|   CA|         58.0|         NULL|
    # |A-3779912_Z1|2023-01-31 02:58:00|Los Angeles|   CA|         48.0|         NULL|
    # |   A-3558690|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         NULL|
    # |   A-3558700|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         NULL|
    # |   A-3558713|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         NULL|
    # +------------+-------------------+-----------+-----+-------------+-------------+
    #
    # and
    #
    # == After merge, the size of Delta file is 9.58 kB and contains 8 rows.
    # +------------+-------------------+-----------+-----+-------------+-------------+
    # |          ID|         Start_Time|       City|State|Temperature_F|Temperature_C|
    # +------------+-------------------+-----------+-----+-------------+-------------+
    # |A-3666323_Z1|2023-03-29 08:48:30|Los Angeles|   CA|         63.6|         NULL|
    # |A-3657191_Z1|2023-03-23 13:37:30|Los Angeles|   CA|         59.9|         NULL|
    # |A-3779912_Z1|2023-01-31 02:58:00|Los Angeles|   CA|         13.5|         NULL|
    # |A-5230341_Z1|2023-01-30 05:07:00|Los Angeles|   CA|          7.7|         NULL|
    # |A-4842824_Z1|2023-01-30 00:11:00|Los Angeles|   CA|         85.4|         NULL|
    # |   A-3558690|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         NULL|
    # |   A-3558700|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         NULL|
    # |   A-3558713|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         NULL|
    # +------------+-------------------+-----------+-----+-------------+-------------+
    #
    # and
    #
    # == After the second update, the size of Delta file is 15.45 kB and contains 7 rows.
    # +------------+-------------------+-----------+-----+-------------+-------------+
    # |          ID|         Start_Time|       City|State|Temperature_F|Temperature_C|
    # +------------+-------------------+-----------+-----+-------------+-------------+
    # |A-3666323_Z1|2023-03-29 08:48:30|Los Angeles|   CA|         63.6|         17.6|
    # |A-3657191_Z1|2023-03-23 13:37:30|Los Angeles|   CA|         59.9|         15.5|
    # |A-3779912_Z1|2023-01-31 02:58:00|Los Angeles|   CA|         13.5|        -10.3|
    # |A-4842824_Z1|2023-01-30 00:11:00|Los Angeles|   CA|         85.4|         29.7|
    # |   A-3558690|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         -0.6|
    # |   A-3558700|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         -0.6|
    # |   A-3558713|2016-01-14 22:18:33|  Whitehall|   PA|         31.0|         -0.6|
    # +------------+-------------------+-----------+-----+-------------+-------------+
    #
    # and finally (the numbers might not match exactly)
    #
    # == Before vacuum, the size of Delta file is 15.45 kB.
    # Deleted 4 files and directories in a total of 1 directories.
    # == After vacuum, the size of Delta file is 7.89 kB.



    # Stop the Spark session (DO NOT do this in Databricks!)
    spark.stop()


# Helper function to separate the task outputs from each other
def printTaskLine(taskNumber: int) -> None:
    print(f"======\nTask {taskNumber}\n======")


if __name__ == "__main__":
    main()
