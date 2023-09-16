import sys
from operator import add
from pyspark.sql import SparkSession

def Map(r, N, M):
    r = r[0].split(',')
    r = [r[0], int(r[1]), int(r[2]), float(r[3])]
    results = []

    if r[0] == 'A':
        for i in range(0,N):
            results.append( ((r[1],i),('A', r[2], r[3])) )
    elif r[0] == 'B':
        for i in range(0,M):
            results.append( ((i,r[2]),('B', r[1], r[3])) )
    return results


def Reduce(r):
    key, values = r[0], r[1]
    
    A = []
    B = []
    
    for el in values:
        if el[0] == 'A':
            A.append(el)
        else:
            B.append(el)
    
    result = 0
    for el_A in A:
        for el_B in B:
            if el_B[1] == el_A[1]:
                result += el_A[2]*el_B[2]
                
    #print(result)
    if result == 0:
    	return []
    return [(key,result)]

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: matrixes <file>", file=sys.stderr)
        exit(-1)


    N = int(sys.argv[2])
    M = int(sys.argv[3])

    spark = SparkSession\
        .builder\
        .appName("PythonMatrix")\
        .getOrCreate()  
    
    matrices = spark.read.text(sys.argv[1]).rdd

    countsR = matrices.flatMap(lambda x: Map(x, N, M))\
                    .groupByKey()\
                    .flatMap(Reduce)\

    for result in countsR.collect():
        print("Key = %s,  value = %d" % (result[0], result[1]))
    spark.stop()
