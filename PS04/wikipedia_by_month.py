#!/usr/bin/spark-submit
#
# Problem Set #4
# Implement wordcount on the shakespeare plays as a spark program that:
# a.Removes characters that are not letters, numbers or spaces from each input line.
# b.Converts the text to lowercase.
# c.Splits the text into words.
# d.Reports the 40 most common words, with the most common first.

# Note:
# You'll have better luck debugging this with ipyspark

import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##

    infile =  's3://gu-anly502/ps03/freebase-wex-2009-01-12-articles.tsv'

    ## 
    ## Run WordCount on Spark
    ##

    sc = SparkContext( appName="Wikipedia Count" )
    lines = sc.textFile(infile)
    counts = lines.map(lambda line: line.split('\t')[2]).map(lambda x:(x[0:7],1)).reduceByKey(add)
    yearmonth_sort = counts.sortBy(lambda x: x[0]).collect()

    ## YOUR CODE GOES HERE
    ## PUT YOUR RESULTS IN counts


    with open("wikipedia_by_month.txt","w") as fout:
        for (date, count) in yearmonth_sort:
            fout.write("{}\t{}\n".format(date,count))
    
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
