# Data-Intensive Programming - Group assignment

## Example outputs

This notebook contains some example outputs and additional hints for the assignment tasks.

Depending on the task your output might not have to match these examples exactly. Instead, these are provided to help you confirm that you are on the right track with the tasks.

## Basic Task 1

Example output from the task:

```text
The publisher with the highest total video game sales in North America is: 'Activision'
The number of titles with missing sales data for North America: 230
Sales data for the publisher:
+----+--------+------------+
|year|na_total|global_total|
+----+--------+------------+
|2006|   14.55|       19.99|
|2007|    26.9|       42.11|
|2008|   39.21|       63.38|
|2009|   45.08|       74.95|
|2010|   37.92|       60.08|
|2011|   28.63|       51.29|
|2012|   23.08|        46.0|
|2013|   20.92|       39.64|
|2014|   21.51|       42.45|
|2015|   19.67|       38.98|
+----+--------+------------+
```

## Basic Task 2

No visible output is required from this task.

Some hints about the `eventDF` data frame:

- There should be `3071394` rows in the data frame.
- Not all columns will be needed in the following tasks. Consider dropping the columns that are not needed.

## Basic Task 3

No visible output is required from this task.

For testing purposes, a test output that you should be able to generate using `matchDF`:

```text
Total number of matches: 1826
Matches without any goals: 130
Most goals in total in a single game: 9
Total amount of goals: 4947
+-------+----------------------+---------+------------+--------------+-------------+-------------+
|matchId|competition           |season   |homeTeam    |awayTeam      |homeTeamGoals|awayTeamGoals|
+-------+----------------------+---------+------------+--------------+-------------+-------------+
|2499806|English Premier League|2017-2018|Swansea City|Leicester City|1            |2            |
|2499920|English Premier League|2017-2018|Chelsea     |Stoke City    |5            |0            |
|2500056|English Premier League|2017-2018|Swansea City|Everton       |1            |1            |
|2500894|French Ligue 1        |2017-2018|PSG         |Dijon         |8            |0            |
|2516792|German Bundesliga     |2017-2018|Stuttgart   |Augsburg      |0            |0            |
|2516861|German Bundesliga     |2017-2018|Schalke 04  |Köln          |2            |2            |
|2565711|Spanish La Liga       |2017-2018|Real Madrid |Barcelona     |0            |3            |
|2576132|Italian Serie A       |2017-2018|Juventus    |Roma          |1            |0            |
+-------+----------------------+---------+------------+--------------+-------------+-------------+
```

## Basic Task 4

No visible output is required from this task.

For testing purposes, a test output that you should be able to generate using `seasonDF`:

```text
Total number of rows: 98
Teams with more than 70 points in a season: 18
Lowest amount points in a season: 20
Total amount of points: 5031
Total amount of goals scored: 4947
Total amount of goals conceded: 4947
+----------------------+---------+----------+-----+----+-----+------+-----------+-------------+------+
|competition           |season   |team      |games|wins|draws|losses|goalsScored|goalsConceded|points|
+----------------------+---------+----------+-----+----+-----+------+-----------+-------------+------+
|French Ligue 1        |2017-2018|Lille     |38   |10  |8    |20    |41         |67           |38    |
|Spanish La Liga       |2017-2018|Getafe    |38   |15  |10   |13    |42         |33           |55    |
|Italian Serie A       |2017-2018|Torino    |38   |13  |15   |10    |54         |46           |54    |
|English Premier League|2017-2018|Arsenal   |38   |19  |6    |13    |74         |51           |63    |
|German Bundesliga     |2017-2018|Schalke 04|34   |18  |9    |7     |53         |37           |63    |
|English Premier League|2017-2018|Burnley   |38   |14  |12   |12    |36         |39           |54    |
+----------------------+---------+----------+-----+----+-----+------+-----------+-------------+------+
```

## Basic Task 5

Example output from the task:

```text
English Premier League table for season 2017-2018
+---+----------------------+---+---+---+---+---+---+---+---+
|Pos|Team                  |Pld|W  |D  |L  |GF |GA |GD |Pts|
+---+----------------------+---+---+---+---+---+---+---+---+
|1  |Manchester City       |38 |32 |4  |2  |106|27 |+79|100|
|2  |Manchester United     |38 |25 |6  |7  |68 |28 |+40|81 |
|3  |Tottenham Hotspur     |38 |23 |8  |7  |74 |36 |+38|77 |
|4  |Liverpool             |38 |21 |12 |5  |84 |38 |+46|75 |
|5  |Chelsea               |38 |21 |7  |10 |62 |38 |+24|70 |
|6  |Arsenal               |38 |19 |6  |13 |74 |51 |+23|63 |
|7  |Burnley               |38 |14 |12 |12 |36 |39 |-3 |54 |
|8  |Everton               |38 |13 |10 |15 |44 |58 |-14|49 |
|9  |Leicester City        |38 |12 |11 |15 |56 |60 |-4 |47 |
|10 |Newcastle United      |38 |12 |8  |18 |39 |47 |-8 |44 |
|11 |Crystal Palace        |38 |11 |11 |16 |45 |55 |-10|44 |
|12 |AFC Bournemouth       |38 |11 |11 |16 |45 |61 |-16|44 |
|13 |West Ham United       |38 |10 |12 |16 |48 |68 |-20|42 |
|14 |Watford               |38 |11 |8  |19 |44 |64 |-20|41 |
|15 |Brighton & Hove Albion|38 |9  |13 |16 |34 |54 |-20|40 |
|16 |Huddersfield Town     |38 |9  |10 |19 |28 |58 |-30|37 |
|17 |Southampton           |38 |7  |15 |16 |37 |56 |-19|36 |
|18 |Swansea City          |38 |8  |9  |21 |28 |56 |-28|33 |
|19 |Stoke City            |38 |7  |12 |19 |35 |68 |-33|33 |
|20 |West Bromwich Albion  |38 |6  |13 |19 |31 |56 |-25|31 |
+---+----------------------+---+---+---+---+---+---+---+---+
```

## Basic Task 6

No visible output is required from this task.

For testing purposes, a test output that you should be able to generate using `matchPassDF`:

```text
Total number of rows: 3652
Team-match pairs with more than 700 total passes: 119
Team-match pairs with more than 600 successful passes: 160
+-------+-----------+----------------------+---------+----------------+-----------+
|matchId|team       |competition           |season   |successfulPasses|totalPasses|
+-------+-----------+----------------------+---------+----------------+-----------+
|2499920|Chelsea    |English Premier League|2017-2018|575             |637        |
|2500894|Dijon      |French Ligue 1        |2017-2018|203             |263        |
|2516792|Stuttgart  |German Bundesliga     |2017-2018|487             |565        |
|2516861|Schalke 04 |German Bundesliga     |2017-2018|410             |478        |
|2565711|Real Madrid|Spanish La Liga       |2017-2018|369             |433        |
|2576132|Roma       |Italian Serie A       |2017-2018|419             |488        |
+-------+-----------+----------------------+---------+----------------+-----------+
```

## Basic Task 7

Example output from the task:

```text
The teams with the lowest ratios for successful passes for each league in season 2017-2018:
+----------------------+----------+----------------+
|competition           |team      |passSuccessRatio|
+----------------------+----------+----------------+
|Spanish La Liga       |Getafe    |72.37           |
|Italian Serie A       |Crotone   |74.74           |
|English Premier League|Stoke City|76.28           |
|German Bundesliga     |Augsburg  |76.44           |
|French Ligue 1        |Toulouse  |77.51           |
+----------------------+----------+----------------+
```

## Basic Task 8

Example output from the task:

```text
The top 2 teams for each league in season 2017-2018
+-----------------+----------------------+---+---+---+---+---+---+---+---+---+----+---------+
|Team             |competition           |Pos|Pld|W  |D  |L  |GF |GA |GD |Pts|Avg |PassRatio|
+-----------------+----------------------+---+---+---+---+---+---+---+---+---+----+---------+
|Manchester City  |English Premier League|1  |38 |32 |4  |2  |106|27 |+79|100|2.63|89.62    |
|Juventus         |Italian Serie A       |1  |38 |30 |5  |3  |86 |24 |+62|95 |2.5 |87.96    |
|Bayern München   |German Bundesliga     |1  |34 |27 |3  |4  |92 |28 |+64|84 |2.47|87.83    |
|PSG              |French Ligue 1        |1  |38 |29 |6  |3  |108|29 |+79|93 |2.45|89.16    |
|Barcelona        |Spanish La Liga       |1  |38 |28 |9  |1  |99 |29 |+70|93 |2.45|88.35    |
|Napoli           |Italian Serie A       |2  |38 |28 |7  |3  |77 |29 |+48|91 |2.39|87.87    |
|Manchester United|English Premier League|2  |38 |25 |6  |7  |68 |28 |+40|81 |2.13|84.79    |
|Monaco           |French Ligue 1        |2  |38 |24 |8  |6  |85 |45 |+40|80 |2.11|82.52    |
|Atlético Madrid  |Spanish La Liga       |2  |38 |23 |10 |5  |58 |22 |+36|79 |2.08|82.51    |
|Schalke 04       |German Bundesliga     |2  |34 |18 |9  |7  |53 |37 |+16|63 |1.85|81.96    |
+-----------------+----------------------+---+---+---+---+---+---+---+---+---+----+---------+
```

## Advanced Task 1

Hints have already been given in the task instructions.

## Advanced Task 2

##### Some intermediate results that can be helpful to check your solution.

Your code does not have to have similar data frames, but these show some intermediate steps that can be taken.

For the match lengths:

```text
matchLength column gives the match length in minutes based on the latest match event:
+-------+-----------+
|matchId|matchLength|
+-------+-----------+
|2499963|         95|
|2576070|         96|
|2565891|         93|
+-------+-----------+
```

For the players time on the pitch in a match:

```text
The startMinute and endMinute columns tell when the player went on and off the pitch.
The minutes column tells how many minutes the player was on the pitch during the match.
+-------+--------+---------------+---------+----------+-----------+---------+-------+
|matchId|playerId|competition    |season   |playerTeam|startMinute|endMinute|minutes|
+-------+--------+---------------+---------+----------+-----------+---------+-------+
|2575959|20820   |Italian Serie A|2017-2018|Atalanta  |70         |95       |25     |
|2575959|20879   |Italian Serie A|2017-2018|Roma      |0          |95       |95     |
|2575959|21620   |Italian Serie A|2017-2018|Atalanta  |0          |70       |70     |
|2575959|25405   |Italian Serie A|2017-2018|Roma      |NULL       |NULL     |0      |
+-------+--------+---------------+---------+----------+-----------+---------+-------+
```

For players plus-minus statistics regarding a single match event:

```text
The eventTime column is the time of the event in minutes from the start of the match.
+---------+-------+--------+--------------+-----------------+--------+-----------+---------+---------------+
|eventId  |matchId|playerId|playerTeam    |eventTime        |goalTeam|startMinute|endMinute|playerPlusMinus|
+---------+-------+--------+--------------+-----------------+--------+-----------+---------+---------------+
|177960866|2499719|7868    |Arsenal       |84.60682721666667|Arsenal |0          |96       |1              |
|177960866|2499719|7879    |Arsenal       |84.60682721666667|Arsenal |75         |96       |1              |
|177960866|2499719|7945    |Arsenal       |84.60682721666667|Arsenal |0          |75       |0              |
|177960866|2499719|8013    |Leicester City|84.60682721666667|Arsenal |0          |88       |-1             |
+---------+-------+--------+--------------+-----------------+--------+-----------+---------+---------------+
```

##### Example of the final outputs from the task:

```text
The players with the most minutes played in season 2017-2018 for each player role:
+----------+-----------------------+------------------+-------+
|role      |player                 |birthArea         |minutes|
+----------+-----------------------+------------------+-------+
|Goalkeeper|Asmir Begović          |Bosnia-Herzegovina|3633   |
|Defender  |Harry  Maguire         |England           |3609   |
|Midfielder|Jack Cork              |England           |3601   |
|Forward   |Gerard Moreno Balaguero|Spain             |3552   |
+----------+-----------------------+------------------+-------+
```

and

```text
The players with higher than +65 for the plus-minus statistics in season 2017-2018:
+------------------------------+---------+----------+---------+
|player                        |birthArea|role      |plusMinus|
+------------------------------+---------+----------+---------+
|Alphonse Aréola               |France   |Goalkeeper|76       |
|Ederson Santana de Moraes     |Brazil   |Goalkeeper|73       |
|Edinson Roberto Cavani Gómez  |Uruguay  |Forward   |72       |
|Kevin De Bruyne               |Belgium  |Midfielder|71       |
|Lionel Andrés Messi Cuccittini|Argentina|Forward   |70       |
|Kyle Walker                   |England  |Defender  |66       |
|Marc-André ter Stegen         |Germany  |Goalkeeper|66       |
+------------------------------+---------+----------+---------+
```

## Advanced Task 3

Note that the images read into an `image` column can be displayed with the Databricks `display` command.

Example outputs from the task:

Note that the Finnish flag image has a larger size than all the other images, and thus it should naturally be at the top of the first list.

```text
The annotations for the four images with the most colored non-transparent pixels:
- flag: Finland
- bowling
- bullseye
- volleyball
============================================================
The annotations for the five images having the lowest ratio of colored vs. transparent pixels:
- seedling
- magic wand
- herb
- lizard
- fireworks
```

All flag images have a black border, which is why the Finnish flag image has black pixels:

```text
The colors and their percentage shares in the image for the Finnish flag:
- color: White, share: 56.47
- color: Blue, share: 27.15
- color: Black, share: 15.53
============================================================
The number of images that have, Blue-Yellow-Black, as the most common colors: 6
```


```text
The annotations and red pixel counts for the five images with the most red pixels among the image group 'activities':
- red envelope (red pixels: 1765)
- admission tickets (red pixels: 1191)
- flower playing cards (red pixels: 892)
- reminder ribbon (red pixels: 764)
- balloon (red pixels: 537)
```

## Advanced Task 4

The Spark machine learning pipelines, [https://spark.apache.org/docs/3.5.0/ml-pipeline.html](https://spark.apache.org/docs/3.5.0/ml-pipeline.html), might simplify the task code, but their use is not compulsory.

All the example runs have been made using the default classifier parameters and the seed value of 1.
Also, the same seed value has been used for splitting the input data into training (80 %) and test (20 %) sets.
Some more carefully chosen parameters could have given better results.

Example outputs from the task (your output format does not have to match these) (also, exactly the same numbers are not required):

Training the asked models and evaluating their accuracy using the built-in evaluator:

```text
Training a 'RandomForest' model to predict 'month' based on input 'temperature,humidity,wind_speed'.
The accuracy of the model is 0.45013439330397403
Training a 'RandomForest' model to predict 'hour' based on input 'temperature,humidity,wind_speed'.
The accuracy of the model is 0.07647723193570134
Training a 'RandomForest' model to predict 'month' based on input 'power_tenants,power_maintenance,power_solar_panels'.
The accuracy of the model is 0.2806155047564958
Training a 'RandomForest' model to predict 'hour' based on input 'power_tenants,power_maintenance,power_solar_panels'.
The accuracy of the model is 0.2429628709356808
Training a 'RandomForest' model to predict 'month' based on input 'temperature,humidity,wind_speed,power_tenants,power_maintenance,power_solar_panels,electricity_price'.
The accuracy of the model is 0.5219977802441731
Training a 'RandomForest' model to predict 'hour' based on input 'temperature,humidity,wind_speed,power_tenants,power_maintenance,power_solar_panels,electricity_price'.
The accuracy of the model is 0.26244617092119865
```

Gathering the asked additional accuracy test results for the previous models into a data frame:

```text
+------------+--------------------------------------------------------------+-----+-------+----------+----------+--------+
|classifier  |input                                                         |label|correct|within_one|within_two|avg_prob|
+------------+--------------------------------------------------------------+-----+-------+----------+----------+--------+
|RandomForest|temperat,humidity,wind_spe,power_te,power_ma,power_so,electric|month|52.2   |77.48     |85.49     |0.1357  |
|RandomForest|temperat,humidity,wind_spe                                    |month|45.01  |73.06     |83.14     |0.1454  |
|RandomForest|power_te,power_ma,power_so                                    |month|28.06  |53.71     |70.19     |0.1067  |
|RandomForest|temperat,humidity,wind_spe,power_te,power_ma,power_so,electric|hour |26.24  |53.64     |67.29     |0.1252  |
|RandomForest|power_te,power_ma,power_so                                    |hour |24.3   |50.77     |65.16     |0.1334  |
|RandomForest|temperat,humidity,wind_spe                                    |hour |7.65   |21.17     |33.09     |0.0513  |
+------------+--------------------------------------------------------------+-----+-------+----------+----------+--------+
```

An example of an additional model training experiment with two different classifiers. In this case, unlike the previous cases, the input data has been normalized as part of the data preparation. Your task code **should** use some other combination of columns and/or classifiers.

```text
Training a 'RandomForest' model to predict 'dayofweek' based on input 'power_tenants,power_maintenance,electricity_price'.
The accuracy of the model is 0.2897500487839492
Training a 'NaiveBayes' model to predict 'dayofweek' based on input 'power_tenants,power_maintenance,electricity_price'.
The accuracy of the model is 0.11353355448723634
+------------+--------------------------+---------+-------+----------+----------+--------+
|classifier  |input                     |label    |correct|within_one|within_two|avg_prob|
+------------+--------------------------+---------+-------+----------+----------+--------+
|RandomForest|power_te,power_ma,electric|dayofweek|28.98  |58.32     |80.35     |0.1247  |
|NaiveBayes  |power_te,power_ma,electric|dayofweek|11.35  |49.86     |73.51     |0.1467  |
+------------+--------------------------+---------+-------+----------+----------+--------+
```

Note that in the last example, the accuracy for correct answers with Naive Bayes model is less than a random guess: 1/7 = 0.1429 = 14.29 %
