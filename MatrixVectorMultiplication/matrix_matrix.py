import scipy.sparse as sp
import numpy as np
import sys
from operator import add
from pyspark.sql import SparkSession
import math

def Map1(r):
    r = r[0].split(",")
    
    if (r[0] == 'A'):
    	return [(int(r[2]), (r[0], int(r[1]), float(r[3])))]
    else:
    	return [(int(r[1]), (r[0], int(r[2]), float(r[3])))]
    
    
def Reduce1(r):
	key, matrix = r[0], list(r[1])
	
	print("k",key)
	print("m",matrix)
	
	A = []
	B = []
	
	for el in matrix:
		if el[0] == 'A':
			A.append(el)
		else:
			B.append(el)

	ret = []

	for a in A:
		for b in B:
			ret.append(((a[1],b[1]), a[2]*b[2]))
		
	print(ret)
	return ret
    	
	
def Map2(r):
    key, values = r[0], list(r[1])
    print((key, values))
    return [(key, values)]

def Reduce2(r):
    key, values = r[0], list(r[1])
    return [(key, sum(values))]
	
if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: matrixes <file>", file=sys.stderr)
		exit(-1)
		
	spark = SparkSession\
		.builder\
		.appName("PythonMatrix")\
		.getOrCreate()
		
	lines = spark.read.text(sys.argv[1]).rdd
	
	countsR = lines.flatMap(Map1)\
		.groupByKey()\
               .flatMap(Reduce1)\
               .groupByKey()\
               .flatMap(Map2)\
               .flatMap(Reduce2)
		
	for result in countsR.collect():
		print("Key = %s,  value = %d" % (result[0], result[1]))
		
	spark.stop()
