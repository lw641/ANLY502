#
# You are on your own!
#
import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
 
    sc = SparkContext( appName="Geolocate" )
    infile =  's3://gu-anly502/ps04/Shakespeare.txt'
    myfile = sc.textFile("s3://gu-anly502/maxmind/GeoLite2-Country-Blocks-IPv4.csv")
    myfile2 = sc.textFile("s3://gu-anly502/maxmind/GeoLite2-Country-Locations-en.csv")
    
    map1 = myfile.zipWithIndex().filter(lambda x:x[1]>0).map(lambda x:x[0]).map(lambda line: line.split(',')).map(lambda x: (x[1],x))

    map2 = myfile2.zipWithIndex().filter(lambda x:x[1]>0).map(lambda x:x[0]).map(lambda line: line.split(',')).map(lambda x: (x[0],x))
  
   
    geo40 = counts.sortBy(lambda x: x[1], ascending=False).take(40)

    with open("forensicswiki_bycountry.txt","w") as fout:
        for (word, count) in geo40:
            fout.write("{}\t{}\n".format(word,count))
    
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
