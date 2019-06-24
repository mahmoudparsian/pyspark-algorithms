# Chapter 1

# Problem Definition

Let's say that we have lots of data records 
measuring the URL visits by users in the following 
format defined in the Input section.


# Input

Record format:

````
<URL-address><,><frequency>
````

Sample input:

````
https://money.cnn.com,112300
https://money.cnn.com,157080
...
https://www.illumina.com,87000
https://www.illumina.com,58086
````

# Programs

We want to find out average, median, and the standard 
deviation of visiting numbers per key (as URL-address). 
Assume that another requirement is that to drop records, 
if the length of any record is less than 5 (may be as a 
malformed URL). It is easy to express an elegant solution 
for this in PySpark. The solutions are presented in this 
chapter as:

* `compute_stats`
	* `compute_stats.py` (PySpark program)
	* `compute_stats.sh` (shell script to call PySpark)
	* `compute_stats.log` (sample log for `compute_stats.py`)
	
* `compute_stats_detailed`
	* `compute_stats_detailed.py` (PySpark program)
	* `compute_stats_detailed.sh` (shell script to call PySpark)
	* `compute_stats_detailed.log` (sample log for `compute_stats_detailed.py`)

* `compute_stats_with_threshold`
	* `compute_stats_with_threshold.py` (PySpark program)
	* `compute_stats_with_threshold.sh` (shell script to call PySpark)
	* `compute_stats_with_threshold.log` (sample log for `compute_stats_with_threshold.py`)

* `compute_stats_with_threshold_and_filter`
	* `compute_stats_with_threshold_and_filter.py` (PySpark program)
	* `compute_stats_with_threshold_and_filter.sh` (shell script to call PySpark)
	* `compute_stats_with_threshold_and_filter.log` (sample log for `compute_stats_with_threshold_and_filter.py`)

* `dataframe_creation_from_csv`
	* `dataframe_creation_from_csv.py` (PySpark program)
	* `dataframe_creation_from_csv.sh` (shell script to call PySpark)
	* `dataframe_creation_from_csv.log` (sample log for `dataframe_creation_from_csv.py`)

* `rdd_creation_from_csv`
	* `rdd_creation_from_csv.py` (PySpark program)
	* `rdd_creation_from_csv.sh` (shell script to call PySpark)
	* `rdd_creation_from_csv.log` (sample log for `rdd_creation_from_csv.py`)

* `sort_numbers`
	* `sort_numbers.py` (PySpark program)
	* `sort_numbers.sh` (shell script to call PySpark)
	* `sort_numbers.log` (sample log for `sort_numbers.py`)
