#!/usr/bin/python
#-----------------------------------------------------
# This is a word count in Python programming language.
# The goal is to show how "word count" works.
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Mahmoud Parsian
#-------------------------------------------------------
import sys
import collections 
#
input_path = sys.argv[1] 
#
file = open(input_path, "r") 
wordcount = collections.Counter() 
#
for word in file.read().split(): 
    wordcount[word] += 1 
#for-done
print (wordcount) 
file.close() 
