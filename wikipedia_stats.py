import mrjob
from mrjob.job import MRJob
import os
from mrjob.step import MRStep
import heapq

class WikipediaStats(MRJob):
    def mapper(self, _, line):
        filename = mrjob.compat.jobconf_from_env("map.input.file")
        if "freebase-wex-2009-01-12-articles.tsv" in filename:
        fields = line.split("\t")
        time1 = field[2]
        time1 = time1.split()
        time2 = time1[0]
        time2 = time2.split("-")
        time = int(time2[0] + time2[1])
        yield time, 1

    def reducer(self, key, value):
    	yield key, sum(value)

    def mapper_sort(self, keys, values):
    	yield "", (keys, values)

    def reducer_sort(self, keys, values):
    	for i in sorted(values):
        yield keys, i

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),

            MRStep(mapper = self.mapper_sort,
                   reducer = self.reducer_count) ]



if __name__=="__main__":
    WikipediaStats.run()
