import sys
from pyspark.sql import SparkSession

def Map(r, v):
    r = [float(x) for x in r[0].split(",")]
    vect = list(filter(lambda x: x[0] == r[1], v))
    if vect == []:
        return []

    vect = vect[0]
    return [(r[0],vect[1]*r[2])]

def Reduce(r):
    key, value = r[0], r[1]

    return (key, sum(value))
	
if __name__ == "__main__":

	if len(sys.argv) != 3:
		print("Usage: matrixes <file>", file=sys.stderr)
		exit(-1)
		
	spark = SparkSession\
		.builder\
		.appName("PythonMatrix")\
		.getOrCreate()
		
	lines = spark.read.text(sys.argv[1]).rdd
	
	v = []
	with open(sys.argv[2], "r") as v_file:
        	for line in v_file:
            		pom = [int(x) for x in line.split(",")]
            		v.append(pom)
	
	countsR = lines.flatMap(lambda x: Map(x, v))\
               .groupByKey()\
               .map(Reduce)
		
	for result in countsR.collect():
		print("Key = %d,  value = %f" % (result[0], result[1]))
		
	spark.stop()
