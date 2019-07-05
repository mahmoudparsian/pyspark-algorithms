#!/usr/bin/python
#-----------------------------------------------------
# This program loads (reads) an LR model (built before 
# by the logistic_regression_builder.py program) and 
# then predicts new emails as "spam" or "non-spam".
#------------------------------------------------------
# Input Parameters:
#    argv[0]: String, is the name of the Python program
#    argv[1]: String, new emails to be classified [Query Data]
#    argv[2]: String, output path for the saved built model
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
#
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionModel
#
#===============================================
# Next, we build a simple Python function, `classify()` 
# to classify new emails with the built LR model:
# predict/classify email and return 
#    0 (for non-spam), and 
#    1 (for spam)
#
# Parameters:
#
#    email: a new email to be classified
#    tf: an instance of HashingTF
#    model: an LR model
#
def classify(email, tf, model):
    # tokenize an email into words
    tokens = email.split()
    # create features for a given email
    features = tf.transform(tokens)
    # classify email into "spam" (class 1) or "non-spam" (class 0)	
    return model.predict(features)
#end-def
#===============================================

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: ", __file__, "  <output-path-for-the-built-model> <new-emails-to-be-classified> ", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("logistic_regression_predictor")\
        .getOrCreate()

    # Step-1: define input paths
    saved_model_path =  sys.argv[1]
    # "/.../model"
    new_emails_path = sys.argv[2]
    # "/.../new_emails.txt"
    
    # check out the inputs
    print("saved_model_path: {}".format(saved_model_path))
    print("new_emails_path: {}".format(new_emails_path))

    # Next, we load the model from a saved location:
    LR_model = LogisticRegressionModel.load(spark.sparkContext, saved_model_path)

    # We still need to create a TF to create features 
    # for the query of new emails:
    FEATURES_HTF = HashingTF(numFeatures=128)

    # Finally, we can predict new emails:
    
    # build an RDD[String] for the new emails
    # for new_emails, create RDD[String], where each 
    # element is an email as a String
    new_emails = spark.sparkContext.textFile(new_emails_path)
    
    # next we classify every new email
    # classified is an RDD[classification, email], 
    # where classification will be either 0 (for non-spam email)  
    # or 1 (for spam email)
    classified = new_emails.map(\
        lambda email: (classify(email, FEATURES_HTF, LR_model), email)) 

    # Next, we debug/examine the classified emails:
    # use collect() if you are doing debugging/testing
    # of small number of emails
    predictions = classified.collect()
    spam_count = 0
    nospam_count = 0
    error_count = 0
    #
    # predications is a list of pair of (classification, email)
    # p denotes a pair of (classification, email); 
    # p[0] denotes predicted classification and 
    # p[1] denotes an email
    for p in predictions: 
        if p[0] == 0:
            nospam_count += 1
        elif p[0] == 1:
            spam_count += 1
        else:
            error_count += 1
        #
        print("prediction=" + str(p[0]) + "\t query email=" + str(p[1]))
    #end-for
    print("spam_count=" + str(spam_count))
    print("nospam_count=" + str(nospam_count))
    print("error_count=" + str(error_count))
    
    # done!
    spark.stop()

