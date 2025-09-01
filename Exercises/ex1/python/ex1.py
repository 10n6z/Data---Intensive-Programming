"""Exercise 1 for Data-Intensive Programming"""

from pyspark.sql import SparkSession


def main():
    # Create the Spark session (only needed for the last two tasks)
    # In Databricks, the Spark session is created automatically, and you should not create it yourself.
    spark: SparkSession = SparkSession.builder \
                                      .appName("ex1") \
                                      .config("spark.driver.host", "localhost") \
                                      .master("local") \
                                      .getOrCreate()

    # suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("WARN")



    # COMP.CS.320 Data-Intensive Programming, Exercise 1
    #
    # This exercise is mostly introduction to the Azure Databricks notebook system.
    # There are some basic programming tasks that can be done in either Scala or Python. The final two tasks are very basic Spark related tasks.
    # This is the Python version intended for local development.
    #
    # Each task has its own cell(s) for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary.
    # There are cells with test code or example output following most of the tasks that involve producing code.
    #
    # Don't forget to submit your solutions to Moodle.


    printTaskLine(1)
    # Task 1 - Read tutorial
    #
    # Read the "Basics of using Databricks notebooks" tutorial notebook,
    # https://adb-7895492183558578.18.azuredatabricks.net/?o=7895492183558578#notebook/2974598884121429
    #
    # To get a point from this task, add "done" (or something similar) to the following (after you have read the tutorial).

    # Task 1 is ???



    printTaskLine(2)
    # Task 2 - Basic function
    #
    # Part 1:
    # - Write a simple function `mySum` that takes two integer as parameters and returns their sum.
    #
    # Part 2:
    # - Write a function `myTripleSum` that takes three integers as parameters and returns their sum.

    def mySum(a, b):
        return int(a) + int(b)

    def myTripleSum(a, b, c):
        return int(a) + int(b) + int(c)


    sum41 = mySum(20, 21)
    if sum41 == 41:
        print(f"correct result: 20+21 = {sum41}")
    else:
        print(f"wrong result: {sum41} != 41")
    sum65 = myTripleSum(20, 21, 24)
    if sum65 == 65:
        print(f"myTripleSum: correct result: 20+21+24 = {sum65}")
    else:
        print(f"myTripleSum: wrong result: {sum65} != 65")



    printTaskLine(3)
    # Task 3 - Fibonacci numbers
    #
    # The Fibonacci numbers, F_n, are defined such that each number is the sum of the two preceding numbers.
    # The first two Fibonacci numbers are: F_0 = 0 and F_1 = 1
    #
    # Write a **recursive** function, `fibonacci`, that takes in the index and returns the Fibonacci number.
    # (no need for any optimized solution here)

    def fibonacci(n):
        if n == 0 or n == 1: return n
        return fibonacci(n - 1) + fibonacci(n - 2)


    fibo6 = fibonacci(6)
    if fibo6 == 8:
        print("correct result: fibonacci(6) == 8")
    else:
        print(f"wrong result: {fibo6} != 8")

    fibo11 = fibonacci(11)
    if fibo11 == 89:
        print("correct result: fibonacci(11) == 89")
    else:
        print(f"wrong result: {fibo11} != 89")



    printTaskLine(4)
    # Task 4 - Higher order functions 1
    #
    # - `map` function can be used to transform the elements of a list.
    # - `reduce` function can be used to combine the elements of a list.
    #
    # Part 1:
    # - Using the `myList`as a starting point, use function `map` to calculate the cube of each element, and then use the reduce function to calculate the sum of the cubes.
    #
    # Part 2:
    # - Using functions `map` and `reduce`, find the largest value for f(x)=1+9*x-x^2 when the input values x are the values from `myList`.

    from functools import reduce
    from typing import List

    myList: List[int] = [2, 3, 5, 7, 11, 13, 17, 19]

    cubeSum: int = reduce(lambda a,b: a + b,map(lambda x: pow(x,3) ,myList))

    largestValue: int = reduce(lambda a,b: a if a > b else b,map(lambda x:1+9*x-x*x,myList))

    print(f"Sum of cubes:                    {cubeSum}")
    print(f"Largest value of f(x)=1+9*x-x^2:    {largestValue}")


    # Example output:
    #
    # Sum of cubes:                    15803
    # Largest value of f(x)=1+9*x-x^2:    21



    printTaskLine(5)
    # Task 5 - Higher order functions 2
    #
    # Explain the following Scala code snippet (Python versions given at the end). You can try the snippet piece by piece in a notebook cell or search help from Scaladoc ([https://www.scala-lang.org/api/2.12.x/](https://www.scala-lang.org/api/2.12.x/)).
    #
    # "sheena is a punk rocker she is a punk punk"
    #     .split(" ")
    #     .map(s => (s, 1))
    #     .groupBy(p => p._1)
    #     .mapValues(v => v.length)
    #
    # What about?
    #
    # "sheena is a punk rocker she is a punk punk"
    #     .split(" ")
    #     .map((_, 1))
    #     .groupBy(_._1)
    #     .mapValues(v => v.map(_._2).reduce(_+_))
    #
    #
    # For those that don't want to learn anything about Scala, you can do the explanation using the following Python versions:
    #
    # First code snippet in Python:
    #
    # ```python
    # from itertools import groupby  # itertools.groupby requires the list to be sorted
    # {
    #     r: len(s)
    #     for r, s in {
    #         p: list(v)
    #         for p, v in groupby(
    #             sorted(
    #                 map(
    #                     lambda x: (x, 1),
    #                     "sheena is a punk rocker she is a punk punk".split(" ")
    #                 ),
    #                 key=lambda x: x[0]
    #             ),
    #             lambda x: x[0]
    #         )
    #     }.items()
    # }
    # ```
    #
    # Second code snippet in Python:
    #
    # ```python
    # from functools import reduce
    # {
    #     r: reduce(
    #         lambda x, y: x + y,
    #         map(lambda x: x[1], s)
    #     )
    #     for r, s in {
    #         p: list(v)
    #         for p, v in groupby(
    #             sorted(
    #                 map(
    #                     lambda x: (x, 1),
    #                     "sheena is a punk rocker she is a punk punk".split(" ")
    #                 ),
    #                 key=lambda x: x[0]
    #             ),
    #             lambda x: x[0]
    #         )
    #     }.items()
    # }
    # ```
    #
    # The Python code looks way too complex to be used like this. Normally you would forget functional programming paradigm in this case and code this in a different, more simpler way.

    # ???



    printTaskLine(6)
    # Task 6 - Approximation for fifth root
    #
    # Write a function, `fifthRoot`, that returns an approximate value for the fifth root of the input.
    # Use the Newton's method, https://en.wikipedia.org/wiki/Newton%27s_method,
    # with the initial guess of 1. For the fifth root this Newton's method translates to:
    #
    # y_0 = 1
    # y_{n+1} = 1/5 * (4*y_n + x/y_n^4)
    #
    # where `x` is the input value and `y_n` is the guess for the cube root after `n` iterations.
    #
    # Example steps when `x=32`:
    # y_0 = 1
    # y_1 = 1/5 * (4*1 + 32/1^4) = 7.2
    # y_2 = 1/5 * (4*7.2 + 32/7.2^4) = 5.76238
    # y_3 = 1/5 * (4*5.76238 + 32/5.76238^4) = 4.61571
    # y_4 = 1/5 * (4*4.61571 + 32/4.61571^4) = 3.70667
    # ...
    #
    # You will have to decide yourself on what is the condition for stopping the iterations.
    # (you can add parameters to the function if you think it is necessary)
    #
    # Note, if your code is running for hundreds or thousands of iterations,
    # you are either doing something wrong or trying to calculate too precise values.

    def fifthRoot(x: float) -> float:
        y = 1
        E = 1e-6
        while True:
            yi = (1/5)*(4*y+x/(y*y*y*y))
            if abs(y - yi) < E:
                return min(y,yi)
            y = yi


    print(f"Fifth root of 32:       {fifthRoot(32)}")
    print(f"Fifth root of 3125:     {fifthRoot(3125)}")
    print(f"Fifth root of 10^10:    {fifthRoot(1e10)}")
    print(f"Fifth root of 10^(-10): {fifthRoot(1e-10)}")
    print(f"Fifth root of -243:     {fifthRoot(-243)}")


    # Example output
    # (the exact values are not important, but the results should be close enough)
    #
    # Fifth root of 32:       2.0000000000000244
    # Fifth root of 3125:     5.000000000000007
    # Fifth root of 10^10:    100.00000005161067
    # Fifth root of 10^(-10): 0.010000000000000012
    # Fifth root of -243:     -3.0000000040240726



    printTaskLine(7)
    # Task 7 - First Spark task
    #
    # Create and display a DataFrame with your own data similarly as was done in the tutorial notebook.
    #
    # Then fetch the number of rows from the DataFrame.

    from pyspark.sql import DataFrame

    myData = {
        ("Arsenal","1886",13),
        ("Chelsea","1905",6),
        ("Liverpool","1892",19),
        ("Manchester City","1880",9),
        ("Manchester United","1878",20),
        ("Tottenham Hotspur F.C.","1882",2)
    }

    myDF: DataFrame = spark.createDataFrame(myData).toDF("Name","Founded","Titles")

    myDF.show()

    numberOfRows: int = len(myData)

    print(f"Number of rows in the DataFrame: {numberOfRows}")


    # Example output
    # (the actual data can be totally different):
    #
    # +----------------------+-------+------+
    # |                  Name|Founded|Titles|
    # +----------------------+-------+------+
    # |               Arsenal|   1886|    13|
    # |               Chelsea|   1905|     6|
    # |             Liverpool|   1892|    19|
    # |       Manchester City|   1880|     9|
    # |     Manchester United|   1878|    20|
    # |Tottenham Hotspur F.C.|   1882|     2|
    # +----------------------+-------+------+
    #
    # Number of rows in the DataFrame: 6



    printTaskLine(8)
    # Task 8 - Second Spark task
    #
    # The CSV file `numbers.csv` contains some data on how to spell numbers in different languages. The file is located in the `data`folder.
    #
    # Load the data from the file into a DataFrame and display it.
    #
    # Also, calculate the number of rows in the DataFrame.

    numberDF: DataFrame = spark.read \
        .option("header","true")\
        .option("sep",",")\
        .option("inferSchema","true")\
        .csv("abfss://shared@tunics320f2024gen2.blob.core.windows.net/shared/exercises/ex1/numbers.csv")

    display(numberDF)

    numberOfNumbers: int = __MISSING__IMPLEMENTATION__

    print(f"Number of rows in the number DataFrame: {numberOfNumbers}")


    # Example output:
    #
    # +------+-------+---------+-------+------+
    # |number|English|  Finnish|Swedish|German|
    # +------+-------+---------+-------+------+
    # |     1|    one|     yksi|    ett|  eins|
    # |     2|    two|    kaksi|    twå|  zwei|
    # |     3|  three|    kolme|    tre|  drei|
    # |     4|   four|    neljä|   fyra|  vier|
    # |     5|   five|    viisi|    fem|  fünf|
    # |     6|    six|    kuusi|    sex| sechs|
    # |     7|  seven|seitsemän|    sju|sieben|
    # |     8|  eight|kahdeksan|   åtta|  acht|
    # |     9|   nine| yhdeksän|    nio|  neun|
    # |    10|    ten| kymmenen|    tio|  zehn|
    # +------+-------+---------+-------+------+
    #
    # Number of rows in the number DataFrame: 10



    # Stop the Spark session (DO NOT do this in Databricks!)
    spark.stop()


# Helper function to separate the task outputs from each other
def printTaskLine(taskNumber: int) -> None:
    print(f"======\nTask {taskNumber}\n======")


if __name__ == "__main__":
    main()
