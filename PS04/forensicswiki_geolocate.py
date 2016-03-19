#
# You are on your own!
#
import sys
from operator import add
from pyspark import SparkContext


def isokay(ch):
    return ch in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '

if __name__ == "__main__":
 
    infile =  's3://gu-anly502/ps04/Shakespeare.txt'

    sqlContext = SQLContext(sc)
 
    myfile = sc.textFile("s3://gu-anly502/maxmind/GeoLite2-Country-Blocks-IPv4.csv")
    
    map1 = myfile.map(lambda line: line.split(','))
    map1 = myfile.map(lambda line: line.split(',')).map(lambda x: (x[1],x))
    
    myfile2 = sc.textFile("s3://gu-anly502/maxmind/GeoLite2-Country-Locations-en.csv")
    map2 = myfile2.map(lambda line: line.split(','))
    map2 = myfile2.map(lambda line: line.split(',')).map(lambda x: (x[0],x))
  
   
    top40counts = counts.sortBy(lambda x: x[1], ascending=False).take(40)

    with open("wordcount_shakespeare4.txt","w") as fout:
        for (word, count) in top40counts:
            fout.write("{}\t{}\n".format(word,count))
    
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
