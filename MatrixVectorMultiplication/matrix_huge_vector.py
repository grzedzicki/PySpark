import scipy.sparse as sp
import numpy as np
import sys
from operator import add
from pyspark.sql import SparkSession
import math
from collections import defaultdict

def Map1(r, slice_len):
    r = r[0].split(",")
    r = [int(r[0]), int(r[1]), float(r[2])]
    
    k = math.floor(int(r[1])/slice_len)
    
    return (k,r)

def Map2(r, dir_name):
    key, values = r[0], r[1]

    this_vector = defaultdict(float)
   
    with open("%s/v%d" % (dir_name, key)) as sub_vector:
    	for line in sub_vector:
            pom = [float(x) for x in line.split(",")]
            this_vector[pom[0]] = pom[1]

    results = []
 
    for el in values:
    	result = this_vector[el[1]] * el[2]
    	print("matrix: ", el, "vector: ", this_vector[el[1]], "result: ",result)
    	
    	if result != 0:
    		results.append([el[0], result])
        
    return results

def Reduce(r):
    key, value = r[0], r[1]

    return key, sum(value)
	
if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: matrixes  <file>", file=sys.stderr)
		exit(-1)
		
	spark = SparkSession\
		.builder\
		.appName("PythonMatrix")\
		.getOrCreate()
		
	slice_lenght = 3
	vect_dir_name = sys.argv[2]
	matrix_dir_name = sys.argv[1]
	
	lines = spark.read.text(matrix_dir_name+'/m*').rdd
	
	countsR = lines.map(lambda x: Map1(x, slice_lenght))\
		       .groupByKey()\
               .flatMap(lambda x: Map2(x, vect_dir_name))\
               .groupByKey()\
               .map(Reduce)
		
	for result in countsR.collect():
		print("Key = %s,  value = %f" % (result[0], result[1]))
		
	spark.stop()
