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
import os
import csv
from pyspark import SparkContext
import sys
from math import sqrt
import numpy as np

identifiant = 'p1513939'

if len(sys.argv)<2:
    raise Exception("Usage : spark-submit SparkApp1 'name_folder/.../folder_where_to_partition' ")
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
print("Attribute index dictionnary was imported.")

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
print('Partitionning #4.2 : Mapped & Overlapping & median division of Big parts.')

class Zone():
	def __init__(self, MM, Id):
		self.min_ra = MM[0]
		self.max_ra = MM[1]
		self.min_decl = MM[2]
		self.max_decl = MM[3]
		self.Id = Id #never changed

	def ra_is_in(self, ra) : 
		if ra<=self.max_ra and ra>= self.min_ra:
			return(True)
		else:
			return(False)
			
	def decl_is_in(self, decl) : 
		if decl<=self.max_decl and decl>=self.min_decl:
			return(True)
		else: 
			return(False)

	def __str__(self):
		return('range ra : '+ str([self.min_ra,self.max_ra])+' ; range decl : ' + str([self.min_decl,self.max_decl]))

class Grid:
	def __init__(self, minmaxs, M,N, overlap = 0.05, max_lines_in_part = 175000):
                self.N = N
		self.M = M
		self.max_lines_in_part = max_lines_in_part
		self.min_ra = minmaxs[0]
		self.max_ra = minmaxs[1]
		self.min_decl = minmaxs[2]
		self.max_decl = minmaxs[3]
	        self.grid_1 = [[0] for x in range(M)]
		self.grid_2 = [[[0] for x in range(N)] for x in range(M)] 
		Id2 = 0
		for i in range(M):
	        	self.grid_1[i] = Zone([0,0,self.min_decl,self.max_decl],i)
			for j in range(N):
				self.grid_2[i][j] = Zone([0,0,0,0],Id2)
        		        Id2+=1
        print("Grids initialized.")
	def __str__(self):
		out = ''
		for i in range(self.M):
			out+= str(self.grid_2[i])+'\n'
		return(out)

	def update_ra(self, dec):
        	assert(len(dec)==(self.M+1))
        	for i in range(self.M):
	            	g = self.grid_1[i]
        	    	g.min_ra = dec[i]
            		g.max_ra = dec[i+1]
			for j in range(self.N):
				g = self.grid_2[i][j]
				g.min_ra = dec[i]
				g.max_ra = dec[i+1]
		for j in range(self.N):
			self.grid_2[0][j].min_ra = self.min_ra
			self.grid_2[-1][j].max_ra = self.max_ra
        	self.grid_1[0].min_ra = self.min_ra
		self.grid_1[-1].max_ra = self.max_ra
		print("Limits for ra updated.")
	def update_decl(self,dec):
		assert(len(dec)==(self.M) and len(dec[0])==(self.N+1))
		for i, d in enumerate(dec):
			for j in range(self.N):
				g = self.grid_2[i][j]
				g.min_decl = d[j]
				g.max_decl = d[j+1]
		for i in range(self.M):
			self.grid_2[i][0].min_decl = self.min_decl
			self.grid_2[i][-1].max_decl = self.max_decl

	def return_part_2(self, a,b, line):
		"""
        	Get couple key-value for ordered grid with possible overlapping, based on ra only.
        	"""
		I = []
		for i in range(self.M):
			if self.grid_1[i].ra_is_in(a) :
				I.append(i)
				if (i+1)<self.M:
					if self.grid_1[i+1].ra_is_in(a) :
						I.append(i+1)
                	        break
		couples = []
		for i in I:
			for j in range(self.N):
				if self.grid_2[i][j].decl_is_in(b):
        				couples.append(str(int(i*self.N+j))+"#"+line)
					if (j+1)<self.N:
						if self.grid[i][j+1].decl_is_in(b):
							couples.append(str(int(i*self.N+j))+"#"+line)	
				break
		if couples == []:
			raise Exception("void")
			print(a,b)
		couples = '_'.join(couples)
		return(couples)

	def return_part_1(self, a, line):
		I = []
		for i in range(self.M):
			if self.grid_1[i].ra_is_in(a):
				I.append(i)
				if (i+1)<self.N:
					if self.grid_1[i+1].ra_is_in(a):
						I.append(i+1)
		couples = []
		for i in I:
			couples.append(str(i)+'#'+line)
		couples = '_'.join(couples)
		assert(len(I)!=0)
		return(couples)
		

	def generate_metadata(self):
		nbr_line_csv = self.histo_2
		columns = [['Id', 'min_ra', 'max_ra', 'min_decl', 'max_decl','nbr_line_csv','M','N']]
		lines = []
		for i in range(self.N):
			for j in range(self.M):
				g = self.grid[i][j]
				Id = g.Id
				lines.append([Id, g.min_ra, g.max_ra, g.min_decl, g.max_decl, nbr_line_csv[Id],self.M,self.N])
		lines = columns + lines
		lines = map(lambda x : str(x).replace('[','').replace(']','').replace("'",'') ,lines)
		return(lines)

### A function to count elements in each partition 
def count_in_a_partition(iterator):
	yield sum(1 for _ in iterator)

def please_sample_ra(Data, index):
	d = Data.sample(False,0.005,seed=0).map(lambda l : t_ra(float(l[index])))
    	return(d)

def please_sample_decl(iterator):
	d = Data.map(lambda : line[decl])

### to turn "15#line" in (15, "line")
def clean_couple(cou):
	C = cou.split("#")
	if not C:
		print(cou)
	C[0] = int(C[0].encode('ascii','ignore'))
	return(tuple(C))

### A function to create the partition.
def fill(grid,Data,ra,decl):
    	first_part = Data.map(lambda line : (grid.return_part_1(t_ra(float(line[ra])),','.join(line))))\
			.flatMap(lambda couples : couples.split('_'))\
			.map(clean_couple)\
			.partitionBy(grid.M)
	grid.histo_1 = first_part.mapPartitions(count_in_a_partition).coalesce(1).collect()
	print("First partion in nb lines gives: ")
	print(grid.histo_1)
    	## sampling decl
	print(grid)
	sampled_decl = first_part.sample(False, 0.005, seed = 0).map(lambda t: float(t[1].split(',')[decl])).glom().collect()
	deciles_decl = []	
	for part in sampled_decl:
		deciles_decl.append([np.percentile(part, q) for q in np.array(list(range(grid.N+1)))*100/(grid.N+1.0)])		
	
	grid.update_decl(deciles_decl)
	for i in range(grid.M):
		print(str(grid.grid_2[i]))	
	second_part = Data.map(lambda line : (grid.return_part_2(t_ra(float(line[ra])),float(line[decl]),','.join(line))))\
			.flatMap(lambda couples : couples.split('_'))\
			.filter(lambda x : x!=[''])\
			.map(clean_couple)\
			.partitionBy(grid.N*grid.M)
	grid.histo_2 = second_part.mapPartitions(count_in_a_partition).coalesce(1).collect()
	print("After second step, in nb lines :")
	print(grid.histo_2)
	return(second_part)
## PARAMS
OVERLAP = 0.05 #0.05 is new value
MAX_LINES_IN_PART = 175000
M = 10 
N = 5
## COMPUTATIONS
G = Grid(minmaxs,M,N, OVERLAP, MAX_LINES_IN_PART)

Data = sc.textFile("/tp-data/Source/Source-001.csv").map(lambda line : line.split(','))
sampled_ra = please_sample_ra(Data, dico_source["ra"]).collect()

deciles_ra = [np.percentile(sampled_ra, q) for q in np.array(list(range(M+1)))*100/(M+1.0)]
G.update_ra(deciles_ra)
second_part = fill(G, Data,dico_source["ra"],dico_source["decl"])
part_repo = 'hdfs:///user/'+identifiant+'/'+name_partition+'/'
second_part.saveAsTextFile(part_repo)
"""
csv_file_name = 'hdfs:///user/'+identifiant+'/'+name_partition+'/Partition_metadata/'

lines = G.generate_metadata()
sc.parallelize(lines,1).saveAsTextFile(csv_file_name) #had to be outside!
print('The End.')
"""

