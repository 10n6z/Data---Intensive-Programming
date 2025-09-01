package dip24.assignment

import org.apache.spark.sql.{DataFrame, SparkSession}


object Assignment extends App {
    // Create a local Spark session
    val spark = SparkSession.builder()
        .appName("dip24-assignment")
        .config("spark.driver.host", "localhost")
        .master("local")
        .getOrCreate()

    // suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())


    // COMP.CS.320 Data-Intensive Programming, Group assignment
    //
    // The instructions for the tasks in the group assignment are given in
    // the markdown file "Assignment-tasks.md" at the root of the repository.
    // This file contains only the starting code not the instructions.
    //
    // A markdown file "Assignment-example-outputs.md" at the root of the repository contains example outputs for the tasks.
    //
    // The tasks that can be done in either Scala or Python.
    // This is the Scala version intended for local development.
    //
    // For the local development the source data for the tasks can be located in the data folder at the root of the repository.
    // So, instead of accessing the data from the Azure storage container, use the local files as the source.
    //
    // Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    //
    // Comment out those advanced tasks 2-4 that you did not implement.
    //
    // Don't forget to submit your solutions to Moodle once your group is finished with the assignment.



    printTaskLine(BASIC, 1)
    // Basic Task 1 - Video game sales data

    val bestNAPublisher: String = ???

    val titlesWithMissingSalesData: Long = ???

    val bestNAPublisherSales: DataFrame = ???

    println(s"The publisher with the highest total video game sales in North America is: '${bestNAPublisher}'")
    println(s"The number of titles with missing sales data for North America: ${titlesWithMissingSalesData}")
    println("Sales data for the publisher:")
    bestNAPublisherSales.show()



    printTaskLine(BASIC, 2)
    // Basic Task 2 - Event data from football matches

    val eventDF: DataFrame = ???



    printTaskLine(BASIC, 3)
    // Basic Task 3 - Calculate match results

    val matchDF: DataFrame = ???



    printTaskLine(BASIC, 4)
    // Basic Task 4 - Calculate team points in a season

    val seasonDF: DataFrame = ???



    printTaskLine(BASIC, 5)
    // Basic Task 5 - English Premier League table

    val englandDF: DataFrame = ???

    println("English Premier League table for season 2017-2018")
    englandDF.show(20, false)



    printTaskLine(BASIC, 6)
    // Basic task 6 - Calculate the number of passes

    val matchPassDF: DataFrame = ???



    printTaskLine(BASIC, 7)
    // Basic Task 7 - Teams with the worst passes

    val lowestPassSuccessRatioDF: DataFrame = ???

    println("The teams with the lowest ratios for successful passes for each league in season 2017-2018:")
    lowestPassSuccessRatioDF.show(5, false)



    printTaskLine(BASIC, 8)
    // Basic task 8 - The best teams

    val bestDF: DataFrame = ???

    println("The top 2 teams for each league in season 2017-2018")
    bestDF.show(10, false)



    printTaskLine(BASIC, 9)
    // Basic Task 9 - General information

    // ???



    printTaskLine(ADVANCED, 1)
    // Advanced Task 1 - Optimized and correct solutions to the basic tasks (2 points)
    //
    // Add any additional information as comments here.



    printTaskLine(ADVANCED, 2)
    // Advanced Task 2 - Further tasks with football data (2 points)

    val mostMinutesDF: DataFrame = ???

    println("The players with the most minutes played in season 2017-2018 for each player role:")
    mostMinutesDF.show(false)


    val topPlayers: DataFrame = ???

    println("The players with higher than +65 for the plus-minus statistics in season 2017-2018:")
    topPlayers.show(false)



    printTaskLine(ADVANCED, 3)
    // Advanced Task 3 - Image data and pixel colors (2 points)

    // separates binary image data to an array of hex strings that represent the pixels
    // assumes 8-bit representation for each pixel (0x00 - 0xff)
    // with `channels` attribute representing how many bytes are used for each pixel
    def toPixels(data: Array[Byte], channels: Int): Array[String] = {
        data
            .grouped(channels)
            .map(dataBytes =>
                dataBytes
                    .map(byte => f"${byte & 0xff}%02X")
                    .mkString("")
            )
            .toArray
    }

    // naive implementation of picking the name of the pixel color based on the input hex representation of the pixel
    // only works for OpenCV type CV_8U (mode=24) compatible input
    def toColorName(hexString: String): String = {
        // mapping of RGB values to basic color names
        val colors: Map[(Int, Int, Int), String] = Map(
            (0, 0, 0)     -> "Black",  (0, 0, 128)     -> "Blue",   (0, 0, 255)     -> "Blue",
            (0, 128, 0)   -> "Green",  (0, 128, 128)   -> "Green",  (0, 128, 255)   -> "Blue",
            (0, 255, 0)   -> "Green",  (0, 255, 128)   -> "Green",  (0, 255, 255)   -> "Blue",
            (128, 0, 0)   -> "Red",    (128, 0, 128)   -> "Purple", (128, 0, 255)   -> "Purple",
            (128, 128, 0) -> "Green",  (128, 128, 128) -> "Gray",   (128, 128, 255) -> "Purple",
            (128, 255, 0) -> "Green",  (128, 255, 128) -> "Green",  (128, 255, 255) -> "Blue",
            (255, 0, 0)   -> "Red",    (255, 0, 128)   -> "Pink",   (255, 0, 255)   -> "Purple",
            (255, 128, 0) -> "Orange", (255, 128, 128) -> "Orange", (255, 128, 255) -> "Pink",
            (255, 255, 0) -> "Yellow", (255, 255, 128) -> "Yellow", (255, 255, 255) -> "White"
        )

        // helper function to round values of 0-255 to the nearest of 0, 128, or 255
        def roundColorValue(value: Int): Int = {
            if (value < 85) 0
            else if (value < 170) 128
            else 255
        }

        hexString.matches("[0-9a-fA-F]{8}") match {
            case true => {
                // for OpenCV type CV_8U (mode=24) the expected order of bytes is BGRA
                val blue: Int = roundColorValue(Integer.parseInt(hexString.substring(0, 2), 16))
                val green: Int = roundColorValue(Integer.parseInt(hexString.substring(2, 4), 16))
                val red: Int = roundColorValue(Integer.parseInt(hexString.substring(4, 6), 16))
                val alpha: Int = Integer.parseInt(hexString.substring(6, 8), 16)

                if (alpha < 128) "None"  // any pixel with less than 50% opacity is considered as color "None"
                else colors((red, green, blue))
            }
            case false => "None"  // any input that is not in valid format is considered as color "None"
        }
    }


    // The annotations for the four images with the most colored non-transparent pixels
    val mostColoredPixels: Array[String] = ???

    println("The annotations for the four images with the most colored non-transparent pixels:")
    mostColoredPixels.foreach(image => println(s"- ${image}"))
    println("============================================================")


    // The annotations for the five images having the lowest ratio of colored vs. transparent pixels
    val leastColoredPixels: Array[String] = ???

    println("The annotations for the five images having the lowest ratio of colored vs. transparent pixels:")
    leastColoredPixels.foreach(image => println(s"- ${image}"))
    println("============================================================")


    // The three most common colors in the Finnish flag image:
    val finnishFlagColors: Array[String] = ???

    // The percentages of the colored pixels for each common color in the Finnish flag image:
    val finnishColorShares: Array[Double] = ???

    println("The colors and their percentage shares in the image for the Finnish flag:")
    finnishFlagColors.zip(finnishColorShares).foreach({case (color, share) => println(s"- color: ${color}, share: ${share}")})
    println("============================================================")


    // The number of images that have their most common three colors as, Blue-Yellow-Black, in that exact order:
    val blueYellowBlackCount: Long = ???

    println(s"The number of images that have, Blue-Yellow-Black, as the most common colors: ${blueYellowBlackCount}")
    println("============================================================")


    // The annotations for the five images with the most red pixels among the image group activities:
    val redImageNames: Array[String] = ???

    // The number of red pixels in the five images with the most red pixels among the image group activities:
    val redPixelAmounts: Array[Long] = ???

    println("The annotations and red pixel counts for the five images with the most red pixels among the image group 'activities':")
    redImageNames.zip(redPixelAmounts).foreach({case (color, count) => println(s"- ${color} (red pixels: ${count})")})



    printTaskLine(ADVANCED, 4)
    // Advanced Task 4 - Machine learning tasks (2 points)
    //
    // the structure of the code and the output format is left to the group's discretion
    // the example output can be used as inspiration

    // ???



    // Stop the Spark session since all implemented tasks have finished executing.
    spark.stop()


    final val BASIC = "Basic"
    final val ADVANCED = "Advanced"

    // Helper function to separate the task outputs from each other
    def printTaskLine(taskType: String, taskNumber: Int): Unit = {
        val taskTitle: String = s"${taskType} Task ${taskNumber}"
        val separatingLine: String = "=".repeat(taskTitle.length())
        println(s"${separatingLine}\n${taskTitle}\n${separatingLine}")
    }
}
