#Divided into each borough for counting Locations of crime
#<borough>  col[13]
#<latitude> col[21]
#<longitude>col[22]

#take Staten Island for example.

from __future__ import print_function

import sys
from csv import reader
from operator import add
from pyspark import SparkContext

borough = sys.argv[2]

def get_location(col):
    return col[13] + '\t' + col[21] + '\t' + col[22]

def find_borough(col):
    return borough.upper() in col[13] and col[21] != 'UNDEFINED' and col[22] != 'UNDEFINED'

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: borough_location_boroughname.py <clean_dataset> <borough_name>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    lines = lines.filter(find_borough).map(get_location)
   
    lines.saveAsTextFile("each_borough_locations_STATEN.out")
    sc.stop()
