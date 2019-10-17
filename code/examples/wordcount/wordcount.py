from __future__ import print_function
import sys
from pyspark.sql import SparkSession

#
print ("This is the name of the script: ", sys.argv[0])
print ("Number of arguments: ", len(sys.argv))
print ("The arguments are: " , str(sys.argv))
#
if len(sys.argv) != 3:
	print("Usage: wordcount.py <input-path>, <output-path>", file=sys.stderr)
	exit(-1)

# DEFINE your input path
input_path = sys.argv[1]
print("input_path: ", input_path)

# DEFINE your output path
output_path = sys.argv[2]
print("output_path: ", output_path)

# CREATE an instance of a SparkSession object
spark = SparkSession\
	.builder\
	.appName("PythonWordCount")\
	.getOrCreate()

# CREATE a new RDD[String]
lines = spark.sparkContext.textFile(input_path)
print("lines=", lines.collect())

#   APPLY a SET of TRANSFORMATIONS...
# counts: RDD[(String, Integer)]
counts = lines.flatMap(lambda x: x.split(' ')) \
			  .map(lambda x: (x, 1)) \
              .reduceByKey(lambda a,b : a+b)

#   output = [(word1, count1), (word2, count2), ...]                  
output = counts.collect()
for (word, count) in output:
	print("%s: %i" % (word, count))

# save output
counts.saveAsTextFile(output_path)

#   DONE!
spark.stop()
