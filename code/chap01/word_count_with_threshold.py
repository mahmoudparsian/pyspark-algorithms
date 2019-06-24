import sys
from pyspark import SparkConf
from pyspark import SparkContext

def wordcount(sc, input_path, threshold):

    records_rdd = sc.textFile(input_path)
    print(records_rdd.collect())
    
    words_rdd = records_rdd.flatMap(lambda line: line.split(" "))
    print(words_rdd.collect())
    
    pairs_rdd =  words_rdd.map(lambda word: (word, 1))
    print(pairs_rdd.collect())
    
    frequencies_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b)
    print(frequencies_rdd.collect())
    
    # filter out words with fewer than threshold occurrences
    filtered_rdd = frequencies_rdd.filter(lambda (word, count): count >= threshold)
    print(filtered_rdd.collect())

if __name__ == '__main__':
    
    conf = SparkConf()
    conf.setAppName("WordCount")
    conf.set('spark.executor.memory', '500M')
    conf.set('spark.cores.max', 4)
    try:
        sc = SparkContext(conf=conf)
    except: 
        print ("Failed to connect!")
        print(sys.exc_info()[0])
    
    #   sys.argv[0] is the name of the script.
    #   sys.argv[1] is the first parameter: filename
    #   sys.argv[2] is the second parameter: threshold
    input_path = sys.argv[1] # "file:///Users/mparsian/sample.txt"
    print("input_path: {}".format(input_path))
    
    # get threshold
    threshold = int(sys.argv[2])
    
    # Execute word count
    wordcount(sc, input_path, threshold)
