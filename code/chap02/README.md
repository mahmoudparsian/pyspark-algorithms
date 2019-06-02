# Chapter 2 Programs

This chapter presents the "Hello World!" program
in PySpark. 

I have presented "word count" problem and provided 
several solutions to it using `reduceByKey()` and 
`groupByKey()` transformations. 

Note that the `reduceByKey()` transformation is 
efficient than  the `groupByKey()`. When possible, 
we should avoid using the `groupByKey()` transformation 
and replace it be `reduceByKey()`, `aggregateByKey()`, 
or `combineByKey()`.

Examples are provided to show how to use the 
`filter()` transformation.
