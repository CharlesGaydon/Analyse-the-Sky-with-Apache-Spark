""" 

 AUTHOR : Charles GAYDON
 LAST EDITED : 30/10/2017
 
 SCRIPT : SparkApp2.py
 USAGE : spark-submit SparkApp2.py 
        OR 
		spark-submit SparkApp2.py 2> out  
 
 This work uses data from the PetaSky project : http://com.isima.fr/Petasky
 Tutorial followed can be found (in french...) at : 
    https://forge.univ-lyon1.fr/EMMANUEL.COQUERY/tp-spark-2017/blob/master/README.md
"""

print("Running.")
print("__ __ __ __ __")
#print("AIM : get the object that moved the most from the observations.") 
import os
import csv
from pyspark import SparkContext
import sys
from math import sqrt

identifiant = 'p1513939'

if len(sys.argv)<2:
    print("Usage : spark-submit SparkApp1 'name_folder/.../folder_where_to_partition' ")
else :
    name_partition = sys.argv[1]
print('Partition saved in path : ' + name_partition)

### Get sources names
SQL_obj = "Samples/Object.sql"  # Should be some file on your system
SQL_source = "Samples/Source.sql"
sc = SparkContext("local", "Simple App")
def get_attr(file_path) :
    logData = sc.textFile(file_path)
    first = logData.first()
    attr = logData.filter(lambda line : line!=first and  ';'not  in line)\
        .map(lambda line : line.split()[0]).collect()
    attr_dic = {}
    for i, a in enumerate(attr) :
        attr_dic[a] = i
    return(attr_dic)
#print(get_attr(SQL_obj))
dico_source = get_attr(SQL_source)
#print(dico_source)
print(
"Attribute index dictionnary was imported.")

### Count occurences of sample id in source-sample
SA_source = "Samples/source-sample"
attr = "objectId"
def get_count(file_path, my_attr, dico) :
    index = dico[my_attr]
    logData = sc.textFile(file_path)
    col = logData.map(lambda line : (line.split(',')[index],1)).filter(lambda tuple : tuple[0] != 'NULL').reduceByKey(lambda a,b : a+b).collect()
    return(col)
#print(get_count(SA_source, attr, dico_source))


### count occurences and get position dataprint('Import of min and max')

PATH_source = "/tp-data/Source/Source-*.csv"
attr = ["ra","decl"]

### Transform the ra attribute from [0,360] to [-180,180] degrees
def t_ra(ra):
	if ra>180:
		return(ra-360)
	else:
		return(ra)

### Get the min and max of ra and decl attributes, after t_ra transform.

def get_range(file_path, my_attr, dico) :
    ra = dico[my_attr[0]]
    decl = dico[my_attr[1]]
    logData = sc.textFile(file_path)
    col = logData.map(lambda line : line.split(','))\
        .map(lambda line : (t_ra(float(line[ra])),t_ra(float(line[ra])), float(line[decl]), float(line[decl]) ))\
        .filter(lambda t : t[0] != 'NULL' and t[2]!='NULL')\
        .reduce(lambda a,b : (min(a[0],b[0]), max(a[1],b[1]), min(a[2],b[2]), max(a[3],b[3]) ))
    
    return(col)

if not os.path.isfile('ra_decl_range.csv') : 
    with open('ra_decl_range.csv','wb') as file:
        minmaxs = get_range(PATH_source, attr, dico_source)
        print('Range of ra and decl for partitionning is :')
        print(minmaxs)
        wr = csv.writer(file, delimiter = ',')    
        wr.writerow(minmaxs)
else :
    print('Import of min and max: ')
    with open('ra_decl_range.csv', 'rb') as file:
        minmaxs = list(csv.reader(file, delimiter=','))[0]
        minmaxs = [float(x) for x in minmaxs] 
        print(minmaxs)


### Define the class for a smart partition of the data,
### suitable for further computations.
print('Partitionning #3 : Mapped Overlapping Partitionning.')

class Zone():
	def __init__(self, MM, Id):
		self.min_ra = MM[0]
		self.max_ra = MM[1]
		self.min_decl = MM[2]
		self.max_decl = MM[3]
	def ra_is_in(self, ra) : 
		if ra>self.max_ra:
			return(False)
		elif ra< self.min_ra : 
			return(False)
		else :
			return(True)
			
	def decl_is_in(self, decl) : 
		if decl>self.max_decl:
			return(False)
		elif decl< self.min_decl: 
			return(False)
		else : 
			return(True)
	def __str__(self):
		return('range ra : '+ str([self.min_ra,self.max_ra])+' ; range decl : ' + str([self.min_decl,self.max_decl]))

class Grid:
	def __init__(self, minmaxs, N, overlap = 0.05):
                self.N = N
		self.min_ra = minmaxs[0]
		self.max_ra = minmaxs[1]
		self.inc_ra = (self.max_ra-self.min_ra)/N
		self.min_decl = minmaxs[2]
		self.max_decl = minmaxs[3]
		self.inc_decl = (self.max_decl-self.min_decl)/N
		self.grid = [[[0] for x in range(N)] for x in range(N)] 
		Id = 0
		for i in range(N):
			for j in range(N):
				self.grid[i][j] = Zone([self.min_ra +self.inc_ra*(i-overlap) , self.min_ra+self.inc_ra*(i+1+overlap), self.min_decl+self.inc_decl*(j-overlap), self.min_decl+self.inc_decl*(j+1+overlap)], Id)
				#print(self.grid[i][j])
				Id+=1
 		
	def return_key_value_strings(self, a,b, line):
		I = []
		J = []
		for i in range(self.N):
			if self.grid[i][0].ra_is_in(a) :
				I.append(i)
				if (i+1)<self.N:
					if self.grid[i+1][0].ra_is_in(a) :
						I.append(i+1)
		
		for j in range(self.N): 
			if self.grid[0][j].decl_is_in(b) : #ok only because it is a square grid.
				J.append(j)
				if (j+1)<self.N:
					if self.grid[0][j+1].decl_is_in(b) :
						J.append(j+1)
		couples = [] 
		for i in I:
			for j in J:
				couples.append(str(int(i*self.N+j))+'#'+line)
		couples = '_'.join(couples)
		assert(len(I)!=0 and len(J)!=0)
		return(couples)

	def generate_repartition(self):
		nbr_line_csv = self.histo.coalesce(1).collect()
		columns = [['Id', 'min_ra', 'max_ra', 'min_decl', 'max_decl','nbr_line_csv','N','M']]
		lines = []
		Id = 0
		for i in range(self.N):
			for j in range(self.N):
				g = self.grid[i][j]
				lines.append([Id, g.min_ra, g.max_ra, g.min_decl, g.max_decl, nbr_line_csv[Id],self.N,self.N])
				Id += 1	
		lines = columns + lines
		lines = map(lambda x : str(x).replace('[','').replace(']','').replace("'",'') ,lines)
		return(lines)

### A function to count elements in each partition 
def count_in_a_partition(iterator):
	yield sum(1 for _ in iterator)

### to turn "15#line" in (15, "line")
def clean_couple(cou):
	C = cou.split("#")
	C[0] = int(C[0].encode('ascii','ignore'))
	#print(C)
	return(tuple(C))
	
### A function to create the partition in grid.
def fill(grid, logData):
	ra = dico_source["ra"]
	decl = dico_source["decl"]
	col = logData.map(lambda line : line.split(','))\
			.map(lambda line : (grid.return_key_value_strings(t_ra(float(line[ra])), float(line[decl]),','.join(line))))\
			.flatMap(lambda couples : couples.split('_'))\
			.map(clean_couple)\
			.partitionBy(grid.N*grid.N).map(lambda tu : tu[1])
	col.saveAsTextFile('hdfs:///user/'+identifiant + '/'+name_partition+'/')
	grid.histo = col.mapPartitions(count_in_a_partition)


overlap = 0.10
assert overlap>=0 and overlap<1
G = Grid(minmaxs,7, overlap)
fill(G, sc.textFile("/tp-data/Source/Source-*.csv"))
csv_file_name = 'hdfs:///user/'+identifiant+'/'+name_partition+'/Partition_metadata/'
lines = G.generate_repartition()
sc.parallelize(lines,1).saveAsTextFile(csv_file_name) #had to be outside!
print('The End.')

#tested : overlap=0 give same results as SparkApp2.py.
