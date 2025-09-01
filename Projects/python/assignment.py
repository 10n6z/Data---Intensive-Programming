"""Exercise 3 for Data-Intensive Programming"""

import re
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession

BASIC: str = "Basic"
ADVANCED: str = "Advanced"


def main():
    # Create a local Spark session
    spark: SparkSession = SparkSession.builder \
        .appName("dip24-assignment") \
        .config("spark.driver.host", "localhost") \
        .master("local") \
        .getOrCreate()

    # suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("WARN")



    # COMP.CS.320 Data-Intensive Programming, Group assignment
    #
    # The instructions for the tasks in the group assignment are given in
    # the markdown file "Assignment-tasks.md" at the root of the repository.
    # This file contains only the starting code not the instructions.
    #
    # A markdown file "Assignment-example-outputs.md" at the root of the repository contains example outputs for the tasks.
    #
    # The tasks that can be done in either Scala or Python.
    # This is the Python version intended for local development.
    #
    # For the local development the source data for the tasks can be located in the data folder at the root of the repository.
    # So, instead of accessing the data from the Azure storage container, use the local files as the source.
    #
    # Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    #
    # Comment out those advanced tasks 2-4 that you did not implement.
    #
    # Don't forget to submit your solutions to Moodle once your group is finished with the assignment.



    printTaskLine(BASIC, 1)
    # Basic Task 1 - Video game sales data

    bestNAPublisher: str = __MISSING__IMPLEMENTATION__

    titlesWithMissingSalesData: int = __MISSING__IMPLEMENTATION__

    bestNAPublisherSales: DataFrame = __MISSING__IMPLEMENTATION__

    print(f"The publisher with the highest total video game sales in North America is: '{bestNAPublisher}'")
    print(f"The number of titles with missing sales data for North America: {titlesWithMissingSalesData}")
    print("Sales data for the publisher:")
    bestNAPublisherSales.show()



    printTaskLine(BASIC, 2)
    # Basic Task 2 - Event data from football matches

    eventDF: DataFrame = __MISSING__IMPLEMENTATION__



    printTaskLine(BASIC, 3)
    # Basic Task 3 - Calculate match results

    matchDF: DataFrame = __MISSING__IMPLEMENTATION__



    printTaskLine(BASIC, 4)
    # Basic Task 4 - Calculate team points in a season

    seasonDF: DataFrame = __MISSING__IMPLEMENTATION__



    printTaskLine(BASIC, 5)
    # Basic Task 5 - English Premier League table

    englandDF: DataFrame = __MISSING__IMPLEMENTATION__

    print("English Premier League table for season 2017-2018")
    englandDF.show(20, False)



    printTaskLine(BASIC, 6)
    # Basic task 6 - Calculate the number of passes

    matchPassDF: DataFrame = __MISSING__IMPLEMENTATION__



    printTaskLine(BASIC, 7)
    # Basic Task 7 - Teams with the worst passes

    lowestPassSuccessRatioDF: DataFrame = __MISSING__IMPLEMENTATION__

    print("The teams with the lowest ratios for successful passes for each league in season 2017-2018:")
    lowestPassSuccessRatioDF.show(5, False)



    printTaskLine(BASIC, 8)
    # Basic task 8 - The best teams

    bestDF: DataFrame = __MISSING__IMPLEMENTATION__

    print("The top 2 teams for each league in season 2017-2018")
    bestDF.show(10, False)



    printTaskLine(BASIC, 9)
    # Basic Task 9 - General information

    # ???



    printTaskLine(ADVANCED, 1)
    # Advanced Task 1 - Optimized and correct solutions to the basic tasks (2 points)
    #
    # Add any additional information as comments here.



    printTaskLine(ADVANCED, 2)
    # Advanced Task 2 - Further tasks with football data (2 points)

    mostMinutesDF: DataFrame = __MISSING__IMPLEMENTATION__

    print("The players with the most minutes played in season 2017-2018 for each player role:")
    mostMinutesDF.show(truncate=False)


    topPlayers: DataFrame = __MISSING__IMPLEMENTATION__

    print("The players with higher than +65 for the plus-minus statistics in season 2017-2018:")
    topPlayers.show(truncate=False)



    printTaskLine(ADVANCED, 3)
    # Advanced Task 3 - Image data and pixel colors (2 points)

    # separates binary image data to an array of hex strings that represent the pixels
    # assumes 8-bit representation for each pixel (0x00 - 0xff)
    # with `channels` attribute representing how many bytes is used for each pixel
    def toPixels(data: bytes, channels: int) -> List[str]:
        return [
            "".join([
                f"{data[index+byte]:02X}"
                for byte in range(0, channels)
            ])
            for index in range(0, len(data), channels)
        ]

    # naive implementation of picking the name of the pixel color based on the input hex representation of the pixel
    # only works for OpenCV type CV_8U (mode=24) compatible input
    def toColorName(hexString: str) -> str:
        # mapping of RGB values to basic color names
        colors: Dict[Tuple[int, int, int], str] = {
            (0, 0, 0):     "Black",  (0, 0, 128):     "Blue",   (0, 0, 255):     "Blue",
            (0, 128, 0):   "Green",  (0, 128, 128):   "Green",  (0, 128, 255):   "Blue",
            (0, 255, 0):   "Green",  (0, 255, 128):   "Green",  (0, 255, 255):   "Blue",
            (128, 0, 0):   "Red",    (128, 0, 128):   "Purple", (128, 0, 255):   "Purple",
            (128, 128, 0): "Green",  (128, 128, 128): "Gray",   (128, 128, 255): "Purple",
            (128, 255, 0): "Green",  (128, 255, 128): "Green",  (128, 255, 255): "Blue",
            (255, 0, 0):   "Red",    (255, 0, 128):   "Pink",   (255, 0, 255):   "Purple",
            (255, 128, 0): "Orange", (255, 128, 128): "Orange", (255, 128, 255): "Pink",
            (255, 255, 0): "Yellow", (255, 255, 128): "Yellow", (255, 255, 255): "White"
        }

        # helper function to round values of 0-255 to the nearest of 0, 128, or 255
        def roundColorValue(value: int) -> int:
            if value < 85:
                return 0
            if value < 170:
                return 128
            return 255

        validString: bool = re.match(r"[0-9a-fA-F]{8}", hexString) is not None
        if validString:
            # for OpenCV type CV_8U (mode=24) the expected order of bytes is BGRA
            blue: int = roundColorValue(int(hexString[0:2], 16))
            green: int = roundColorValue(int(hexString[2:4], 16))
            red: int = roundColorValue(int(hexString[4:6], 16))
            alpha: int = int(hexString[6:8], 16)

            if alpha < 128:
                return "None"  # any pixel with less than 50% opacity is considered as color "None"
            return colors[(red, green, blue)]

        return "None"  # any input that is not in valid format is considered as color "None"


    # The annotations for the four images with the most colored non-transparent pixels
    mostColoredPixels: List[str] = __MISSING__IMPLEMENTATION__

    print("The annotations for the four images with the most colored non-transparent pixels:")
    for image in mostColoredPixels:
        print(f"- {image}")
    print("============================================================")


    # The annotations for the five images having the lowest ratio of colored vs. transparent pixels
    leastColoredPixels: List[str] = __MISSING__IMPLEMENTATION__

    print("The annotations for the five images having the lowest ratio of colored vs. transparent pixels:")
    for image in leastColoredPixels:
        print(f"- {image}")


    # The three most common colors in the Finnish flag image:
    finnishFlagColors: List[str] = __MISSING__IMPLEMENTATION__

    # The percentages of the colored pixels for each common color in the Finnish flag image:
    finnishColorShares: List[float] = __MISSING__IMPLEMENTATION__

    print("The colors and their percentage shares in the image for the Finnish flag:")
    for color, share in zip(finnishFlagColors, finnishColorShares):
        print(f"- color: {color}, share: {share}")
    print("============================================================")


    # The number of images that have their most common three colors as, Blue-Yellow-Black, in that exact order:
    blueYellowBlackCount: int = __MISSING__IMPLEMENTATION__

    print(f"The number of images that have, Blue-Yellow-Black, as the most common colors: {blueYellowBlackCount}")


    # The annotations for the five images with the most red pixels among the image group activities:
    redImageNames: List[str] = __MISSING__IMPLEMENTATION__

    # The number of red pixels in the five images with the most red pixels among the image group activities:
    redPixelAmounts: List[int] = __MISSING__IMPLEMENTATION__

    print("The annotations and red pixel counts for the five images with the most red pixels among the image group 'activities':")
    for color, pixel_count in zip(redImageNames, redPixelAmounts):
        print(f"- {color} (red pixels: {pixel_count})")



    printTaskLine(ADVANCED, 4)
    # Advanced Task 4 - Machine learning tasks (2 points)
    #
    # the structure of the code and the output format is left to the group's discretion
    # the example output can be used as inspiration

    # ???



    # Stop the Spark session since all implemented tasks have finished executing.
    spark.stop()



def printTaskLine(taskType: str, taskNumber: int) -> None:
    """Helper function to separate the task outputs from each other."""
    taskTitle: str = f"{taskType} Task {taskNumber}"
    separatingLine: str = "=" * len(taskTitle)
    print(f"{separatingLine}\n{taskTitle}\n{separatingLine}")


if __name__ == "__main__":
    main()
