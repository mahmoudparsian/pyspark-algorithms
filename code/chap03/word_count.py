import sys
from pyspark import SparkConf
from pyspark import SparkContext

def wordcount(sc, input_path):

    records_rdd = sc.textFile(input_path)
    print(records_rdd.collect())
    
    words_rdd = records_rdd.flatMap(lambda line: line.split(" "))
    print(words_rdd.collect())
    
    pairs_rdd =  words_rdd.map(lambda word: (word, 1))
    print(pairs_rdd.collect())
    
    frequencies_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b)
    print(frequencies_rdd.collect())


if __name__ == '__main__':
    
    conf = SparkConf()
    conf.setAppName("WordCount")
    conf.set('spark.executor.memory', '500M')
    conf.set('spark.cores.max', 4)
    try:
        sc = SparkContext(conf=conf)
        # hard coded input path, for DEMO only
        # never hard code
        input_path = "/tmp/sample.txt" 

    except: 
        print ("Failed to connect!")
        print(sys.exc_info()[0])
    
    # Execute word count
    wordcount(sc, input_path)
