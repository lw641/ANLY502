#!/usr/bin/env python2

# To get started with the join, 
# try creating a new directory in HDFS that has both the fwiki data AND the maxmind data.

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os
from mrjob.step import MRStep
import heapq

TOPN=10

class FwikiMaxmindJoin(MRJob):
    def mapper(self, _, line):
        # Is this a weblog file, or a MaxMind GeoLite2 file?
        filename = mrjob.compat.jobconf_from_env("map.input.file")
        if "top1000ips_to_country.txt" in filename:
            fields = line.split("\t")
            self.increment_counter("Info","top1000_ips_to_country Count",1)
            yield fields[0], ("Country", fields[1])
        else:
            log = Weblog(line)
            logfields = (log.ipaddr,log.datetime,log.url,log.wikipage())
            self.increment_counter("Info","weblog Count",1)
            yield logfields[0], ("Weblog",logfields)


    def reducer(self, key, values):
        country = None
        for v in values:
            if len(v)!=2:
                self.increment_counter("Warn","Invalid Join",1)
                continue
	    if v[0] == "Country":
		country = v[1]
            if v[0] == "Weblog":
                IP = v[1]
                if country:
                    #assert key == country[0]
                    #assert key == IP[0]
                    yield IP[0], (country,IP)
                else:
                    self.increment_counter("Warn","log without country")
                    yield IP[0],("N/A",IP)
    
    def mapper_count(self, _, value):
	item=value[0]
	yield item,1      

    def reducer_count(self, key, values):
        yield key, sum(values)

    def top10_mapper(self, word, count):
	if word!="N/A":
		yield "Top10", (word,count) 

    def top10_reducer(self, key, values):
	for i in heapq.nlargest(TOPN,values):
		yield "Top10",i

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),

            MRStep(mapper=self.mapper_count,
                   reducer=self.reducer_count),

	    MRStep(mapper=self.top10_mapper,
                   reducer=self.top10_reducer) ]



if __name__=="__main__":
    FwikiMaxmindJoin.run()
