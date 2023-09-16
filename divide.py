import sys
import math

if len(sys.argv) != 5:
	print("Nieprawidlowa ilosc argumentow", file=sys.stderr)
	exit(-1)

slice_length = 3
matrix = sys.argv[1]
vector = sys.argv[2]
matrix_dir_name = sys.argv[3]
vect_dir_name = sys.argv[4]
    
with open(matrix, "r") as m:
	for line in m:
		r = line.split(",")
		r[0], r[1] = int(r[0]), int(r[1])
		r[2] = float(r[2])
		k = int(math.floor(r[1]/slice_length))
		with open(matrix_dir_name+"/m"+str(k), "a+") as sub_matrix:
			sub_matrix.write(str(r[0])+","+str(r[1])+","+str(r[2])+"\n")
   
with open(vector, "r") as v:
	for line in v:
		r = line.split(",")
		r[0] = int(r[0])
		r[1] = float(r[1])
		k = int(math.floor(r[0]/slice_length))
		with open(vect_dir_name+"/v"+str(k), "a+") as sub_vector:
			sub_vector.write(str(r[0])+","+str(r[1])+"\n")
