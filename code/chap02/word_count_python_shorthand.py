#!/usr/bin/python
#-----------------------------------------------------
# This is a word count in Python programming language.
# The goal is to show how "word count" works.
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author a book reviewer (anonymous)
#-------------------------------------------------------
import sys
import collections
#
input_path = sys.argv[1]
#
with open(input_path) as input_file:
    word_count = collections.Counter(input_file.read().split())
#
print (word_count)


