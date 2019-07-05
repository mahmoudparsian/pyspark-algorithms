"""
Collaborative Filtering Classification Example.

@author Mahmoud Parsian

"""
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import Rating
from pyspark.mllib.recommendation import MatrixFactorizationModel

#------------------------------------------------
# Create a Rating object
# rating_record : "userID,productID,rating"
def create_rating(rating_record):
    tokens = rating_record.split(',')
    userID = int(tokens[0])
    productID = int(tokens[1])
    rating = float(tokens[2])
    return Rating(userID, productID, rating)
#end-def
#------------------------------------------------

# STEP-1: create an instance of a SparkSession object
spark = SparkSession.builder.getOrCreate()

# STEP-2: read input parameters
input_path = sys.argv[1]
rank = int(sys.argv[2])
num_of_iterations = int(sys.argv[3])


# STEP-3: create an RDD from input
# create an RDD[String]
data = spark.sparkContext.textFile(input_path)

# STEP-4: create Rating elements from created RDD
# create RDD[Rating]
ratings = data.map(create_rating)

# STEP-5: Build the recommendation model 
# using ALS (Alternating Least Squares)
rank = 10
num_of_iterations = 10
model = ALS.train(ratings, rank, num_of_iterations)

# STEP-6: Evaluate the model on training data
test_data = ratings.map(lambda r: (r[0], r[1]))
predictions = model.predictAll(test_data)\
                   .map(lambda r: ((r[0], r[1]), r[2]))
rates_and_predictions = ratings.map(lambda r: ((r[0], r[1]), r[2]))\
                               .join(predictions)
MSE = rates_and_predictions.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

# STEP-7: Save and load model
# This step may be used for big data set 
# to save time in building a model
saved_path = "/tmp/myCollaborativeFilter"
model.save(spark.sparkContext, saved_path)
#
same_model = MatrixFactorizationModel.load(spark.sparkContext, saved_path)

