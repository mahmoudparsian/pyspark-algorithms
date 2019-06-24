#!/usr/bin/python
#-----------------------------------------------------
# Basic Tutorial with DataFrame 
# Input: WorldCupPlayers.csv
#------------------------------------------------------
# Input Parameters:
#    WorldCupPlayers.csv
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

if __name__ == '__main__':

    if len(sys.argv) != 2:  
        print("Usage: dataframe_tutorial_with_worldcup.py <input-path>", file=sys.stderr)
        exit(-1)

    # create an instance of SparkSession
    spark = SparkSession\
        .builder\
        .appName("dataframe_tutorial_with_worldcup")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    #===============================================
    # In this tutorial we will read a CSV file
    # (WorldCupPlayers.csv), and create a DataFrame 
    # and then manipulate it by using PySpark API.
    #
    # Input has 9 columns:
    #
    # Columns of WorldCupPlayers.csv
    #
    #  (1)  RoundID: Unique ID of the round
    #  (2)  MatchID: Unique ID of the match
    #  (3)  Team Initials: Player's team initials
    #  (4)  Coach Name: Name and country of the team coach
    #  (5)  Line-up: S=Line-up, N=Substitute
    #  (6)  Shirt Number: Shirt number if available
    #  (7)  Player Name: Name of the player
    #  (8)  Position: C=Captain, GK=Goalkeeper
    #  (9)  Event: G=Goal, OG=Own Goal, Y=Yellow Card, 
    #           R=Red Card, SY = Red Card by second yellow, 
    #           P=Penalty, MP=Missed Penalty, 
    #           I = Substitution In, O=Substitute Out
    #
    #===============================================

    #===============================================
    # Pyspark Dataframes Example: FIFA World Cup Dataset
    # In this tutorial, ee are going to load this data 
    # which is in CSV format into a DataFrame and then 
    # we will learn about the different transformations 
    # and actions that can be performed on this dataframe.
    #===============================================



    #=========================================
    # Reading Data from CSV file
    # Here we are going to use the 
    # spark.read.csv method to load 
    # the data into a dataframe worldcup_df. 
    #=========================================
    worldcup_df = spark.read.csv(\
        input_path,\
        inferSchema = True,\
        header = True)
    #
    print('number of rows: worldcup_df.count():', worldcup_df.count())
    print('worldcup_df.show(10, truncate=False):\n')
    worldcup_df.show(10, truncate=False)
    # Schema of Dataframe
    print('worldcup_df.printSchema():\n')
    worldcup_df.printSchema()


    #=========================================
    # Column Names and Count (Rows and Column)
    #=========================================
    print('Column Names:\n', worldcup_df.columns)
    print('Row Count:\n', worldcup_df.count())
    print('Column Count:\n', len(worldcup_df.columns))
  

    #=========================================
    # Describing a Particular Column
    #=========================================
    print('Describing a Particular Column\n')
    worldcup_df.describe('Coach Name').show()
    worldcup_df.describe('Position').show()


    #=========================================
    # Selecting Multiple Columns
    #=========================================
    print('Selecting Multiple Columns\n')
    worldcup_df.select('Player Name', 'Shirt Number', 'Coach Name').show()


    #=========================================
    # Selecting Distinct Multiple Columns
    #=========================================
    print('Selecting Multiple Columns\n')
    worldcup_df.select('Player Name', 'Shirt Number', 'Coach Name').distinct().show()

    #=========================================
    # Filtering Data
    #=========================================
    print('Filtering Data\n')
    worldcup_df.filter(worldcup_df.MatchID=='1096').show()
    worldcup_df.filter(worldcup_df.MatchID=='1096').count()  


    #=========================================
    # Filtering Data (Multiple Parameters)
    #=========================================
    print('Filtering Data (Multiple Parameters)\n') 
    worldcup_df.filter((worldcup_df.MatchID=='1096') & (worldcup_df.Position=='GK')).show()

    #=========================================
    # Sorting Data (OrderBy)
    #=========================================
    print('Sorting Data (OrderBy)\n') 
    worldcup_df.orderBy(worldcup_df.MatchID).show()


    # done!
    spark.stop()
