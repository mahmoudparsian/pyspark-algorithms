#!/usr/bin/python
#-----------------------------------------------------
# This is a LR model builder in PySpark for email 
# prediction whether a given email should go to 
# InBoxFolder (non-spam email) or JunkFolder (spam email).
# The goal is to show how "to build an LR model".
#------------------------------------------------------
# Input Parameters:
#    argv[0]: String, is the name of the Python program
#    argv[1]: String, non-spam emails [Training Data]
#    argv[2]: String, spam emails     [Training Data]
#    argv[3]: String, output path for the built model
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
#
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.classification import LogisticRegressionWithSGD
#
#===============================================
# This function returns a LabeledPoint
#
# Parameters:
#   label: as 0 (for non-spam), 1 (for spam)
#   email: a String of words representing an email
#   tf: an instance of HashingTF
#
def createLabeledPoint(label, email, tf):
    print("createLabeledPoint() label = ", label)
    print("createLabeledPoint() email = ", email)
    print("createLabeledPoint() tf = ", tf)
    # featurize email
    features = tf.transform(email)
    # create a LabeledPoint
    return LabeledPoint(label, features)
#end-def
#===============================================

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print("Usage: ", __file__, " <spam-emails> <spam-emails> <output-path-for-the-built-model>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("logistic_regression_builder")\
        .getOrCreate()

    # Step-1: define input and output paths
    # define training data paths 
    training_emails_nospam_path = sys.argv[1]
    # "/.../training_emails_nospam.txt"
    training_emails_spam_path = sys.argv[2]
    # "/.../training_emails_spam.txt"
    # define output path for the built model
    saved_model_path = sys.argv[3]
    # "/.../model"    
    
    # check out the inputs
    print("training_emails_nospam_path: {}".format(training_emails_nospam_path))
    print("training_emails_spam_path: {}".format(training_emails_spam_path))
    print("saved_model_path: {}".format(saved_model_path))

    # Step-2: build RDDs for training data: spam and non-spam 
    #
    # spam_emails is an RDD[String]
    spam_emails = spark.sparkContext.textFile(training_emails_spam_path)
    print("spam_emails.collect()=", spam_emails.collect())
    print("spam_emails.count()=", spam_emails.count())
    #
    # nospam_emails is an RDD[String]
    nospam_emails = spark.sparkContext.textFile(training_emails_nospam_path)
    print("nospam_emails.collect()=", nospam_emails.collect())
    print("nospam_emails.count()=", nospam_emails.count())

    # Step-3: create an HTF
    # tf is an instance of HashingTF, which can hold up to 128 features
    FEATURES_HTF = HashingTF(numFeatures=128)

    
    # Step-4: create LabeledPoint per training email
    # spam emails gets a "1" classification, 
    # spam_labeled_points is an RDD[LabeledPoint]
    spam_labeled_points = spam_emails.map(lambda email : createLabeledPoint(1, email, FEATURES_HTF))
    print("spam_labeled_points.collect()=", spam_labeled_points.collect())
    print("spam_labeled_points.count()=", spam_labeled_points.count())
    #
    # nospam emails gets a "0" classification, 
    # nospam_labeled_points is an RDD[LabeledPoint]    
    nospam_labeled_points = nospam_emails.map(lambda email : createLabeledPoint(0, email, FEATURES_HTF))
    print("nospam_labeled_points.collect()=", nospam_labeled_points.collect())
    print("nospam_labeled_points.count()=", nospam_labeled_points.count())
    #

    # Step-5: Create a final training dataset
    # which includes spam and nonspam emails
    # Since all training data is classified, 
    # now we create a single RDD as training_data,
    # which is an RDD[LabeledPoint]  
    training_data = spam_labeled_points.union(nospam_labeled_points)
    print("training_data.count()=", training_data.count())
    print("training_data.collect()=", training_data.collect())
    training_data.cache()

    # Step-6: Use LogisticRegressionWithSGD to create an LR model.
    # Train a classification model for Binary Logistic Regression 
    # using Stochastic Gradient Descent (SGD). By default L2 
    # regularization is used, which can be changed via 
    # LogisticRegressionWithSGD.optimizer.
    # Labels used in Logistic Regression should be 
    # `{0, 1, ..., K - 1}` for K classes multi-label 
    # classification problem. So if we have `K = 2`
    # classes, then the labels will be `{0, 1}`.
    #
    # The following code snippet shows how to create an LR model:
    # You should note that the LR_model is an instance of 
    # pyspark.mllib.classification.LogisticRegressionModel 
    #   
    # build an LR model (`LogisticRegressionModel`) using  
    # `LogisticRegressionWithSGD`
    LR_model = LogisticRegressionWithSGD.train(training_data)  


    #Step-7:  Save The Built LR Model
    # To save the built model, we use `LogisticRegressionModel.save()` 
    # method as:
    LR_model.save(spark.sparkContext, saved_model_path)

    #====================================
    # Calculate the accuracy of the model. 
    #====================================
    # create test data for checking accuracy
    training_70_percent, test_30_percent = training_data.randomSplit((0.7, 0.3))
    #
    print("training_70_percent=", training_70_percent)
    print("training_70_percent.count()=", training_70_percent.count())
    print("training_70_percent.collect()=", training_70_percent.collect())
    #
    print("test_30_percent=", test_30_percent)
    print("test_30_percent.count()=", test_30_percent.count())
    print("test_30_percent.collect()=", test_30_percent.collect())

    #
    # Next, we need to test the model by creating a prediction label.
    prediction_label = test_30_percent.map(lambda x : (LR_model.predict(x.features), x.label))
    print("prediction_label.count()=", prediction_label.count())
    print("prediction_label.collect()=", prediction_label.collect())

    # Accuracy can be calculated by 
    # taking the matching terms from both the training and test data. This 
    # can be done as follows:
    accuracy = 1.0 * prediction_label.filter(lambda x : float(x[0]) == float(x[1])).count() / test_30_percent.count()
    print("accuracy=", accuracy)

    # done!
    spark.stop()

