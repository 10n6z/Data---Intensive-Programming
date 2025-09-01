# Data-Intensive Programming - Group assignment

In all tasks, add your solutions to the cells following the task instructions.

The example outputs, and some additional hints are given in a separate notebook in the same folder as this one.

Don't forget to **submit your solutions to Moodle** once your group is finished with the assignment.

## Basic tasks (compulsory)

There are in total nine basic tasks that every group must implement in order to have an accepted assignment.

The basic task 1 is a separate task, and it deals with video game sales data. The task asks you to do some basic aggregation operations with Spark data frames.

The other basic coding tasks (basic tasks 2-8) are all related and deal with data from [https://figshare.com/collections/Soccer_match_event_dataset/4415000/5](https://figshare.com/collections/Soccer_match_event_dataset/4415000/5) that contains information about events in [football](https://en.wikipedia.org/wiki/Association_football) matches in five European leagues during the season 2017-18. The tasks ask you to calculate the results of the matches based on the given data as well as do some further calculations. Special knowledge about football or the leagues is not required, and the task instructions should be sufficient in order to gain enough context for the tasks.

Finally, the basic task 9 asks some information on your assignment working process.

## Advanced tasks (optional)

There are in total of four advanced tasks that can be done to gain some course points. Despite the name, the advanced tasks may or may not be harder than the basic tasks.

The advanced task 1 asks you to do all the basic tasks in an optimized way. It is possible that you gain some points from this without directly trying by just implementing the basic tasks efficiently. Logic errors and other issues that cause the basic tasks to give wrong results will be taken into account in the grading of the first advanced task. A maximum of 2 points will be given based on advanced task 1.

The other three advanced tasks are separate tasks and their implementation does not affect the grade given for the advanced task 1.
Only two of the three available tasks will be graded and each graded task can provide a maximum of 2 points to the total.
If you attempt all three tasks, clearly mark which task you want to be used in the grading. Otherwise, the grader will randomly pick two of the tasks and ignore the third.

Advanced task 2 continues with the football data and contains further questions that are done with the help of some additional data.
Advanced task 3 deals with some image data and the questions are mostly related to the colors of the pixels in the images.
Advanced task 4 asks you to do some classification related machine learning tasks with Spark.

It is possible to gain partial points from the advanced tasks. I.e., if you have not completed the task fully but have implemented some part of the task, you might gain some appropriate portion of the points from the task. Logic errors, very inefficient solutions, and other issues will be taken into account in the task grading.

## Assignment grading

Failing to do the basic tasks, means failing the assignment and thus also failing the course!
"A close enough" solutions might be accepted => even if you fail to do some parts of the basic tasks, submit your work to Moodle.

Accepted assignment submissions will be graded from 0 to 6 points.

The maximum grade that can be achieved by doing only the basic tasks is 2/6 points (through advanced task 1).

## Short summary

##### Minimum requirements (points: 0-2 out of maximum of 6):

- All basic tasks implemented (at least in "a close enough" manner)
- Moodle submission for the group

##### For those aiming for higher points (0-6):

- All basic tasks implemented
- Optimized solutions for the basic tasks (advanced task 1) (0-2 points)
- Two of the other three advanced tasks (2-4) implemented
    - Clearly marked which of the two tasks should be graded
    - Each graded advanced task will give 0-2 points
- Moodle submission for the group

## Basic Task 1 - Video game sales data

The CSV file `assignment/sales/video_game_sales.csv` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains video game sales data (based on [https://www.kaggle.com/datasets/patkle/video-game-sales-data-from-vgchartzcom](https://www.kaggle.com/datasets/patkle/video-game-sales-data-from-vgchartzcom)).

Load the data from the CSV file into a data frame. The column headers and the first few data lines should give sufficient information about the source dataset. The numbers in the sales columns are given in millions.

Using the data, find answers to the following:

- Which publisher has the highest total sales in video games in North America considering games released in years 2006-2015?
- How many titles in total for this publisher do not have sales data available for North America considering games released in years 2006-2015?
- Separating games released in different years and considering only this publisher and only games released in years 2006-2015, what are the total sales, in North America and globally, for each year?
    - I.e., what are the total sales (in North America and globally) for games released by this publisher in year 2006? And the same for year 2007? ...

## Basic Task 2 - Event data from football matches

A parquet file in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/football/events.parquet` based on [https://figshare.com/collections/Soccer_match_event_dataset/4415000/5](https://figshare.com/collections/Soccer_match_event_dataset/4415000/5) contains information about events in [football](https://en.wikipedia.org/wiki/Association_football) matches during the season 2017-18 in five European top-level leagues: English Premier League, Italian Serie A, Spanish La Liga, German Bundesliga, and French Ligue 1.

#### Background information

In the considered leagues, a season is played in a double round-robin format where each team plays against all other teams twice. Once as a home team in their own stadium and once as an away team in the other team's stadium. A season usually starts in August and ends in May.

Each league match consists of two halves of 45 minutes each. Each half runs continuously, meaning that the clock is not stopped when the ball is out of play. The referee of the match may add some additional time to each half based on game stoppages. [https://en.wikipedia.org/wiki/Association_football#90-minute_ordinary_time](https://en.wikipedia.org/wiki/Association_football#90-minute_ordinary_time)

The team that scores more goals than their opponent wins the match.

**Columns in the data**

Each row in the given data represents an event in a specific match. An event can be, for example, a pass, a foul, a shot, or a save attempt.

Simple explanations for the available columns. Not all of these will be needed in this assignment.

| column name | column type | description |
| ----------- | ----------- | ----------- |
| competition | string | The name of the competition |
| season | string | The season the match was played |
| matchId | integer | A unique id for the match |
| eventId | integer | A unique id for the event |
| homeTeam | string | The name of the home team |
| awayTeam | string | The name of the away team |
| event | string | The main category for the event |
| subEvent | string | The subcategory for the event |
| eventTeam | string | The name of the team that initiated the event |
| eventPlayerId | integer | The id for the player who initiated the event |
| eventPeriod | string | `1H` for events in the first half, `2H` for events in the second half |
| eventTime | double | The event time in seconds counted from the start of the half |
| tags | array of strings | The descriptions of the tags associated with the event |
| startPosition | struct | The event start position given in `x` and `y` coordinates in range 0-100 |
| enPosition | struct | The event end position given in `x` and `y` coordinates in range 0-100 |

The used event categories can be seen from `assignment/football/metadata/eventid2name.csv`.
And all available tag descriptions from `assignment/football/metadata/tags2name.csv`.
You don't need to access these files in the assignment, but they can provide context for the following basic tasks that will use the event data.

#### The task

In this task you should load the data with all the rows into a data frame. This data frame object will then be used in the following basic tasks 3-8.

## Basic Task 3 - Calculate match results

Create a match data frame for all the matches included in the event data frame created in basic task 2.

The resulting data frame should contain one row for each match and include the following columns:

| column name   | column type | description |
| ------------- | ----------- | ----------- |
| matchId       | integer     | A unique id for the match |
| competition   | string      | The name of the competition |
| season        | string      | The season the match was played |
| homeTeam      | string      | The name of the home team |
| awayTeam      | string      | The name of the away team |
| homeTeamGoals | integer     | The number of goals scored by the home team |
| awayTeamGoals | integer     | The number of goals scored by the away team |

The number of goals scored for each team should be determined by the available event data.
There are two events related to each goal:

- One event for the player that scored the goal. This includes possible own goals.
- One event for the goalkeeper that tried to stop the goal.

You need to choose which types of events you are counting.
If you count both of the event types mentioned above, you will get double the amount of actual goals.

## Basic Task 4 - Calculate team points in a season

Create a season data frame that uses the match data frame from the basic task 3 and contains aggregated seasonal results and statistics for all the teams in all leagues. While the used dataset only includes data from a single season for each league, the code should be written such that it would work even if the data would include matches from multiple seasons for each league.

###### Game result determination

- Team wins the match if they score more goals than their opponent.
- The match is considered a draw if both teams score equal amount of goals.
- Team loses the match if they score fewer goals than their opponent.

###### Match point determination

- The winning team gains 3 points from the match.
- Both teams gain 1 point from a drawn match.
- The losing team does not gain any points from the match.

The resulting data frame should contain one row for each team per league and season. It should include the following columns:

| column name    | column type | description |
| -------------- | ----------- | ----------- |
| competition    | string      | The name of the competition |
| season         | string      | The season |
| team           | string      | The name of the team |
| games          | integer     | The number of games the team played in the given season |
| wins           | integer     | The number of wins the team had in the given season |
| draws          | integer     | The number of draws the team had in the given season |
| losses         | integer     | The number of losses the team had in the given season |
| goalsScored    | integer     | The total number of goals the team scored in the given season |
| goalsConceded  | integer     | The total number of goals scored against the team in the given season |
| points         | integer     | The total number of points gained by the team in the given season |

## Basic Task 5 - English Premier League table

Using the season data frame from basic task 4 calculate the final league table for `English Premier League` in season `2017-2018`.

The result should be given as data frame which is ordered by the team's classification for the season.

A team is classified higher than the other team if one of the following is true:

- The team has a higher number of total points than the other team
- The team has an equal number of points, but have a better goal difference than the other team
- The team has an equal number of points and goal difference, but have more goals scored in total than the other team

Goal difference is the difference between the number of goals scored for and against the team.

The resulting data frame should contain one row for each team.
It should include the following columns (several columns renamed trying to match the [league table in Wikipedia](https://en.wikipedia.org/wiki/2017%E2%80%9318_Premier_League#League_table)):

| column name | column type | description |
| ----------- | ----------- | ----------- |
| Pos         | integer     | The classification of the team |
| Team        | string      | The name of the team |
| Pld         | integer     | The number of games played |
| W           | integer     | The number of wins |
| D           | integer     | The number of draws |
| L           | integer     | The number of losses |
| GF          | integer     | The total number of goals scored by the team |
| GA          | integer     | The total number of goals scored against the team |
| GD          | string      | The goal difference |
| Pts         | integer     | The total number of points gained by the team |

The goal difference should be given as a string with an added `+` at the beginning if the difference is positive, similarly to the table in the linked Wikipedia article.

## Basic task 6: Calculate the number of passes

This task involves going back to the event data frame and counting the number of passes each team made in each match. A pass is considered successful if it is marked as `Accurate`.

Using the event data frame from basic task 2, calculate the total number of passes as well as the total number of successful passes for each team in each match.
The resulting data frame should contain one row for each team in each match, i.e., two rows for each match. It should include the following columns:

| column name | column type | description |
| ----------- | ----------- | ----------- |
| matchId     | integer     | A unique id for the match |
| competition | string      | The name of the competition |
| season      | string      | The season |
| team        | string      | The name of the team |
| totalPasses | integer     | The total number of passes the team attempted in the match |
| successfulPasses | integer | The total number of successful passes made by the team in the match |

You can assume that each team had at least one pass attempt in each match they played.

## Basic Task 7: Teams with the worst passes

Using the match pass data frame from basic task 6 find the teams with the lowest average ratio for successful passes over the season `2017-2018` for each league.

The ratio for successful passes over a single match is the number of successful passes divided by the number of total passes.
The average ratio over the season is the average of the single match ratios.

Give the result as a data frame that has one row for each league-team pair with the following columns:

| column name | column type | description |
| ----------- | ----------- | ----------- |
| competition | string      | The name of the competition |
| team        | string      | The name of the team |
| passSuccessRatio | double | The average ratio for successful passes over the season given as percentages rounded to two decimals |

Order the data frame so that the team with the lowest ratio for passes is given first.

## Basic task 8: The best teams

For this task the best teams are determined by having the highest point average per match.

Using the data frames created in the previous tasks find the two best teams from each league in season `2017-2018` with their full statistics.

Give the result as a data frame with the following columns:

| column name | column type | description |
| ----------- | ----------- | ----------- |
| Team        | string      | The name of the team |
| League      | string      | The name of the league |
| Pos         | integer     | The classification of the team within their league |
| Pld         | integer     | The number of games played |
| W           | integer     | The number of wins |
| D           | integer     | The number of draws |
| L           | integer     | The number of losses |
| GF          | integer     | The total number of goals scored by the team |
| GA          | integer     | The total number of goals scored against the team |
| GD          | string      | The goal difference |
| Pts         | integer     | The total number of points gained by the team |
| Avg         | double      | The average points per match gained by the team |
| PassRatio   | double      | The average ratio for successful passes over the season given as percentages rounded to two decimals |

Order the data frame so that the team with the highest point average per match is given first.

## Basic Task 9: General information

Answer **briefly** to the following questions.

Remember that using AI and collaborating with other students outside your group is allowed as long as the usage and collaboration is documented.
However, every member of the group should have some contribution to the assignment work.

- Who were your group members and their contributions to the work?
    - Solo groups can ignore this question.

- Did you use AI tools while doing the assignment?
    - Which ones and how did they help?

- Did you work with students outside your assignment group?
    - Who or which group? (only extensive collaboration need to reported)

## Advanced tasks

The implementation of the basic tasks is compulsory for every group.

Doing the following advanced tasks you can gain course points which can help in getting a better grade from the course.
Partial solutions can give partial points.

The advanced task 1 will be considered in the grading for every group based on their solutions for the basic tasks.

The advanced tasks 2, 3, and 4 are separate tasks. The solutions used in these other advanced tasks do not affect the grading of advanced task 1. Instead, a good use of optimized methods can positively influence the grading of each specific task, while very non-optimized solutions can have a negative effect on the task grade.

While you can attempt all three tasks (advanced tasks 2-4), only two of them will be graded and contribute towards the course grade.
Clearly mark which two you want to be graded.

## Advanced Task 1 - Optimized and correct solutions to the basic tasks (2 points)

Use the tools Spark offers effectively and avoid unnecessary operations in the code for the basic tasks.

A couple of things to consider (**not** even close to a complete list):

- Consider using explicit schemas when dealing with CSV data sources.
- Consider only including those columns from a data source that are actually needed.
- Filter unnecessary rows whenever possible to get smaller datasets.
- Avoid collect or similar expensive operations for large datasets.
- Consider using explicit caching if some data frame is used repeatedly.
- Avoid unnecessary shuffling (for example sorting) operations.
- Avoid unnecessary actions (count, etc.) that are not needed for the task.

In addition to the effectiveness of your solutions, the correctness of the solution logic will be taken into account when determining the grade for this advanced task 1.
"A close enough" solution with some logic fails might be enough to have an accepted group assignment, but those failings might lower the score for this task.

It is okay to have your own test code that would fall into category of "ineffective usage" or "unnecessary operations" while doing the assignment tasks. However, for the final Moodle submission you should comment out or delete such code (and test that you have not broken anything when doing the final modifications).

Note, that you should not do the basic tasks again for this advanced task, but instead modify your basic task code with more efficient versions.

You can create a text cell below this one and describe what optimizations you have done. This might help the grader to better recognize how skilled your work with the basic tasks has been.

## Advanced Task 2 - Further tasks with football data (2 points)

This advanced task continues with football event data from the basic tasks. In addition, there are two further related datasets that are used in this task.

A Parquet file at folder `assignment/football/matches.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains information about which players were involved on each match including information on the substitutions made during the match.

Another Parquet file at folder `assignment/football/players.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains information about the player names, default roles when playing, and their birth areas.

#### Columns in the additional data

The match dataset (`assignment/football/matches.parquet`) has one row for each match and each row has the following columns:

| column name  | column type | description |
| ------------ | ----------- | ----------- |
| matchId      | integer     | A unique id for the match |
| competition  | string      | The name of the league |
| season       | string      | The season the match was played |
| roundId      | integer     | A unique id for the round in the competition |
| gameWeek     | integer     | The gameWeek of the match |
| date         | date        | The date the match was played |
| status       | string      | The status of the match, `Played` if the match has been played |
| homeTeamData | struct      | The home team data, see the table below for the attributes in the struct |
| awayTeamData | struct      | The away team data, see the table below for the attributes in the struct |
| referees     | struct      | The referees for the match |

Both team data columns have the following inner structure:

| column name  | column type | description |
| ------------ | ----------- | ----------- |
| team         | string      | The name of the team |
| coachId      | integer     | A unique id for the coach of the team |
| lineup       | array of integers | A list of the player ids who start the match on the field for the team |
| bench        | array of integers | A list of the player ids who start the match on the bench, i.e., the reserve players for the team |
| substitution1 | struct     | The first substitution the team made in the match, see the table below for the attributes in the struct |
| substitution2 | struct     | The second substitution the team made in the match, see the table below for the attributes in the struct |
| substitution3 | struct     | The third substitution the team made in the match, see the table below for the attributes in the struct |

Each substitution structs have the following inner structure:
| column name  | column type | description |
| ------------ | ----------- | ----------- |
| playerIn     | integer     | The id for the player who was substituted from the bench into the field, i.e., this player started playing after this substitution |
| playerOut    | integer     | The id for the player who was substituted from the field to the bench, i.e., this player stopped playing after this substitution |
| minute       | integer     | The minute from the start of the match the substitution was made.<br>Values of 45 or less indicate that the substitution was made in the first half of the match,<br>and values larger than 45 indicate that the substitution was made on the second half of the match. |

The player dataset (`assignment/football/players.parquet`) has the following columns:

| column name  | column type | description |
| ------------ | ----------- | ----------- |
| playerId     | integer     | A unique id for the player |
| firstName    | string      | The first name of the player |
| lastName     | string      | The last name of the player |
| birthArea    | string      | The birth area (nation or similar) of the player |
| role         | string      | The main role of the player, either `Goalkeeper`, `Defender`, `Midfielder`, or `Forward` |
| foot         | string      | The stronger foot of the player |

#### Background information

In a football match both teams have 11 players on the playing field or pitch at the start of the match. Each team also have some number of reserve players on the bench at the start of the match. The teams can make up to three substitution during the match where they switch one of the players on the field to a reserve player. (Currently, more substitutions are allowed, but at the time when the data is from, three substitutions were the maximum.) Any player starting the match as a reserve and who is not substituted to the field during the match does not play any minutes and are not considered involved in the match.

For this task the length of each match should be estimated with the following procedure:

- Only the additional time added to the second half of the match should be considered. I.e., the length of the first half is always considered to be 45 minutes.
- The length of the second half is to be considered as the last event of the half rounded upwards towards the nearest minute.
    - I.e., if the last event of the second half happens at 2845 seconds (=47.4 minutes) from the start of the half, the length of the half should be considered as 48 minutes. And thus, the full length of the entire match as 93 minutes.

A personal plus-minus statistics for each player can be calculated using the following information:

- If a goal was scored by the player's team when the player was on the field, `add 1`
- If a goal was scored by the opponent's team when the player was on the field, `subtract 1`
- If a goal was scored when the player was a reserve on the bench, `no change`
- For any event that is not a goal, or is in a match that the player was not involved in, `no change`
- Any substitutions is considered to be done at the start of the given minute.
    - I.e., if the player is substituted from the bench to the field at minute 80 (minute 35 on the second half), they were considered to be on the pitch from second 2100.0 on the 2nd half of the match.
- If a goal was scored in the additional time of the first half of the match, i.e., the goal event period is `1H` and event time is larger than 2700 seconds, some extra considerations should be taken into account:
    - If a player is substituted into the field at the beginning of the second half, `no change`
    - If a player is substituted off the field at the beginning of the second half, either `add 1` or `subtract 1` depending on team that scored the goal
    - Any player who is substituted into the field at minute 45 or later is only playing on the second half of the match.
    - Any player who is substituted off the field at minute 45 or later is considered to be playing the entire first half including the additional time.

### Tasks

The target of the task is to use the football event data and the additional datasets to determine the following:

- The players with the most total minutes played in season 2017-2018 for each player role
    - I.e., the player in Goalkeeper role who has played the longest time across all included leagues. And the same for the other player roles (Defender, Midfielder, and Forward)
    - Give the result as a data frame that has the following columns:
        - `role`: the player role
        - `player`: the full name of the player, i.e., the first name combined with the last name
        - `birthArea`: the birth area of the player
        - `minutes`: the total minutes the player played during season 2017-2018
- The players with higher than `+65` for the total plus-minus statistics in season 2017-2018
    - Give the result as a data frame that has the following columns:
        - `player`: the full name of the player, i.e., the first name combined with the last name
        - `birthArea`: the birth area of the player
        - `role`: the player role
        - `plusMinus`: the total plus-minus statistics for the player during season 2017-2018

It is advisable to work towards the target results using several intermediate steps.

## Advanced Task 3 - Image data and pixel colors (2 points)

This advanced task involves loading in PNG image data and complementing JSON metadata into Spark data structure. And then determining the colors of the pixels in the images, and finding the answers to several color related questions.

The folder `assignment/openmoji/color` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains collection of PNG images from [OpenMoji](https://openmoji.org/) project.

The JSON Lines formatted file `assignment/openmoji/openmoji.jsonl` contains metadata about the image collection. Only a portion of the images are included as source data for this task, so the metadata file contains also information about images not considered in this task.

#### Data description and helper functions

The image data considered in this task can be loaded into a Spark data frame using the `image` format: [https://spark.apache.org/docs/3.5.0/ml-datasource.html](https://spark.apache.org/docs/3.5.0/ml-datasource.html). The resulting data frame contains a single column which includes information about the filename, image size as well as the binary data representing the image itself. The Spark documentation page contains more detailed information about the structure of the column.

Instead of using the images as source data for machine learning tasks, the binary image data is accessed directly in this task.
You are given two helper functions to help in dealing with the binary data:

- Function `toPixels` takes in the binary image data and the number channels used to represent each pixel.
    - In the case of the images used in this task, the number of channels match the number bytes used for each pixel.
    - As output the function returns an array of strings where each string is hexadecimal representation of a single pixel in the image.
- Function `toColorName` takes in a single pixel represented as hexadecimal string.
    - As output the function returns a string with the name of the basic color that most closely represents the pixel.
    - The function uses somewhat naive algorithm to determine the name of the color, and does not always give correct results.
    - Many of the pixels in this task have a lot of transparent pixels. Any such pixel is marked as the color `None` by the function.

With the help of the given functions it is possible to transform the binary image data to an array of color names without using additional libraries or knowing much about image processing.

The metadata file given in JSON Lines format can be loaded into a Spark data frame using the `json` format: [https://spark.apache.org/docs/3.5.0/sql-data-sources-json.html](https://spark.apache.org/docs/3.5.0/sql-data-sources-json.html). The attributes used in the JSON data are not described here, but are left for you to explore. The original regular JSON formatted file can be found at [https://github.com/hfg-gmuend/openmoji/blob/master/data/openmoji.json](https://github.com/hfg-gmuend/openmoji/blob/master/data/openmoji.json).

### Tasks

The target of the task is to combine the image data with the JSON data, determine the image pixel colors, and the find the answers to the following questions:

- Which four images have the most colored non-transparent pixels?
- Which five images have the lowest ratio of colored vs. transparent pixels?
- What are the three most common colors in the Finnish flag image (annotation: `flag: Finland`)?
    - And how many percentages of the colored pixels does each color have?
- How many images have their most common three colors as, `Blue`-`Yellow`-`Black`, in that order?
- Which five images have the most red pixels among the image group `activities`?
    - And how many red pixels do each of these images have?

It might be advisable to test your work-in-progress code with a limited number of images before using the full image set.
You are free to choose your own approach to the task: user defined functions with data frames, RDDs/Datasets, or combination of both.

## Advanced Task 4 - Machine learning tasks (2 points)

This advanced task involves experimenting with the classifiers provided by the Spark machine learning library. Time series data collected in the [ProCem](https://www.senecc.fi/projects/procem-2) research project is used as the training and test data. Similar data in a slightly different format was used in the first tasks of weekly exercise 3.

The folder `assignment/energy/procem_13m.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains the time series data in Parquet format.

#### Data description

The dataset contains time series data from a period of 13 months (from the beginning of May 2023 to the end of May 2024). Each row contains the average of the measured values for a single minute. The following columns are included in the data:

| column name        | column type   | description |
| ------------------ | ------------- | ----------- |
| time               | long          | The UNIX timestamp in second precision |
| temperature        | double        | The temperature measured by the weather station on top of Sähkötalo (`°C`) |
| humidity           | double        | The humidity measured by the weather station on top of Sähkötalo (`%`) |
| wind_speed         | double        | The wind speed measured by the weather station on top of Sähkötalo (`m/s`) |
| power_tenants      | double        | The total combined electricity power used by the tenants on Kampusareena (`W`) |
| power_maintenance  | double        | The total combined electricity power used by the building maintenance systems on Kampusareena (`W`) |
| power_solar_panels | double        | The total electricity power produced by the solar panels on Kampusareena (`W`) |
| electricity_price  | double        | The market price for electricity in Finland (`€/MWh`) |

There are some missing values that need to be removed before using the data for training or testing. However, only the minimal amount of rows should be removed for each test case.

### Tasks

- The main task is to train and test a machine learning model with [Random forest classifier](https://spark.apache.org/docs/3.5.0/ml-classification-regression.html#random-forests) in six different cases:
    - Predict the month (1-12) using the three weather measurements (temperature, humidity, and wind speed) as input
    - Predict the month (1-12) using the three power measurements (tenants, maintenance, and solar panels) as input
    - Predict the month (1-12) using all seven measurements (weather values, power values, and price) as input
    - Predict the hour of the day (0-23) using the three weather measurements (temperature, humidity, and wind speed) as input
    - Predict the hour of the day (0-23) using the three power measurements (tenants, maintenance, and solar panels) as input
    - Predict the hour of the day (0-23) using all seven measurements (weather values, power values, and price) as input
- For each of the six case you are asked to:
    1. Clean the source dataset from rows with missing values.
    2. Split the dataset into training and test parts.
    3. Train the ML model using a Random forest classifier with case-specific input and prediction.
    4. Evaluate the accuracy of the model with Spark built-in multiclass classification evaluator.
    5. Further evaluate the accuracy of the model with a custom build evaluator which should do the following:
        - calculate the percentage of correct predictions
            - this should correspond to the accuracy value from the built-in accuracy evaluator
        - calculate the percentage of predictions that were at most one away from the correct predictions taking into account the cyclic nature of the month and hour values:
            - if the correct month value was `5`, then acceptable predictions would be `4`, `5`, or `6`
            - if the correct month value was `1`, then acceptable predictions would be `12`, `1`, or `2`
            - if the correct month value was `12`, then acceptable predictions would be `11`, `12`, or `1`
        - calculate the percentage of predictions that were at most two away from the correct predictions taking into account the cyclic nature of the month and hour values:
            - if the correct month value was `5`, then acceptable predictions would be from `3` to `7`
            - if the correct month value was `1`, then acceptable predictions would be from `11` to `12` and from `1` to `3`
            - if the correct month value was `12`, then acceptable predictions would be from `10` to `12` and from `1` to `2`
        - calculate the average probability the model predicts for the correct value
            - the probabilities for a single prediction can be found from the `probability` column after the predictions have been made with the model
- As the final part of this advanced task, you are asked to do the same experiments (training+evaluation) with two further cases of your own choosing:
    - you can decide on the input columns yourself
    - you can decide the predicted attribute yourself
    - you can try some other classifier other than the random forest one if you want

In all cases you are free to choose the training parameters as you wish.
Note that it is advisable that while you are building your task code to only use a portion of the full 13-month dataset in the initial experiments.
