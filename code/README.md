# PySpark Algorithms: Source Code

## Introduction

* This is the directory for the source 
  code of **PySpark Algorithms** book by 
  [Mahmoud Parsian](https://www.linkedin.com/in/mahmoudparsian/).

* Each chapter contains 
	* Source code for PySpark programs
	* Sample shell scripts (how to invoke PySpark programs)
	* Sample data
	* Sample log files to show I/O
	

## PySpark

PySpark is the Python programming language 
binding for the Spark Platform and API and 
not much different from the Java or Scala 
versions. Python programming language is 
dynamically typed (no need to compile Python 
programs), so RDDs can hold objects of multiple 
types. PySpark does support the following 
Spark's Data Abstractions:

* RDDs
* DataFrames


### Spark

The following Spark version was used:

* [Spark-2.4.3](http://spark.apache.org/news/spark-2-4-3-released.html)

* Spark runs on Java 8+, Python 2.7+/3.4+ and R 3.1+.
  
* The PySpark programs presented here are tested 
  with the following:
	* Spark 2.4.3
	* Java8 (version "1.8.0_144")
	* Python 3.7.2

### Python

Which Python should we use? Python2 or Python3?
According to Spark's documentation, we may use
Python2 or Python3. But since Python 2 will retire
by 2020, I have decided to use Python 3. 

You may set up your own Python by setting the
`PYSPARK_PYTHON` environment variable.

* Let's assume that Python2 is installed 
  as `/usr/bin/python` 

* Let's assume that Python3 is installed 
  as `/usr/local/bin/python3`. 


To use Python3, then execute the following before 
invoking PySpark shell or `spark-submit` command:

````
# verify the version of Python
/usr/local/bin/python3 --version
Python 3.7.2
export PYSPARK_PYTHON="/usr/local/bin/python3"
````


To use Python2, then execute the following before 
invoking PySpark shell or `spark-submit` command:

````
# verify the version of Python
/usr/bin/python --version
Python 2.7.1
export PYSPARK_PYTHON="/usr/bin/python"
````


## Chapters

* There are 12 chapters and each chapter 
has a designated source code directory as:

	* [Chapter 1: Introduction to PySpark](./chap01) 
	* [Chapter 2: Hello World in PySpark](./chap02)	
	* [Chapter 3: Data Abstractions](./chap03)	
	* [Chapter 4: Getting Started](./chap04)	
	* [Chapter 5: Transformations in Spark](./chap05)	
	* [Chapter 6: Reductions in Spark](./chap06)	
	* [Chapter 7: DataFrames](./chap07)	 
	* [Chapter 8: Spark Data Sources](./chap08)	
	* [Chapter 9: Logistic Regression](./chap09)	
	* [Chapter 10: Movie Recommendations](./chap10)	
	* [Chapter 11: Graph Algorithms](./chap11)	
	* [Chapter 12: Design Patterns](./chap12)	
	* Appendix-A: Spark Installation
	* Appendix-B: Lambda Expressions and Functions
	* Appendix-C: Questions and Answers
	
## Programs 

Each program (most of them) has 3 associated files:

* PySpark program (`.py`) -- the actual program, which will run
* Shell script (`.sh`) -- how to run the program
* Sample log file (`.log`) -- what to expect from running this program


## Sample Data and Resources

* Per chapter, I have only provided some sample 
  data.  

* When the size of data is big, I have provided 
  a pointer to that data (when possible).

* Sample data files (as `.txt`, `.csv`, `.json`, ...) 
  are given for each chapter. 

* These files are located a long with the associated 
  programs (as `.py` and `.sh`).


## Required JARs 

* [Third Party JARs](./jars)	


## Spark Installation Directory

* The author assumes that you have installed Spark 
at the `/pyspark_book/spark-2.4.3/` (for `Spark-2.4.3` 
version).  However, you may install it in any other 
location and then update the shell scripts (`.sh` files) 
accordingly. The `SPARK_HOME` (as an environment variable) 
refers the Spark's installation directory.

### Spark's Home Directory Structure

On decompressing the Spark downloadable, you 
will see the following directory structure:

* bin    
* sbin
* conf
* examples
* licenses 
* ...

### Spark's PySpark Examples

Spark installation comes with a set of 
PySpark examples and they are located at 
`$SPARK_HOME/examples/src/main/python/`.




## Comments
If you have any comments or suggestions, 
please report them to the author at <mahmoud.parsian@yahoo.com>

Thank you!

