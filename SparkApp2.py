""" 
 SCRIPT : SimpleApp.py
 USAGE : spark-submit SimpleApp.py 
        OR 
    spark-submit SimpleApp1.py 2> out  
 AUTHOR : Charles GAYDON
 LAST EDITED : 30/10/2017
 
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

### 
def t_ra(ra):
	if ra>180:
		return(ra-360)
	else:
		return(ra)

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

print('Partitionning')


class Zone():
	def __init__(self, MM, Id):
		self.min_ra = MM[0]
		self.max_ra = MM[1]
		self.min_decl = MM[2]
		self.max_decl = MM[3]
	def ra_is_in(self, ra) : 
		if ra>=self.max_ra:
			return(False)
		else : 
			return(True)
			
	def decl_is_in(self, decl) : 
		if decl>=self.max_decl:
			return(False)
		else : 
			return(True)

class Grid:
	def __init__(self, minmaxs, N):
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
				self.grid[i][j] = Zone([self.min_ra +self.inc_ra*i, self.min_ra+self.inc_ra*(i+1), self.min_decl+self.inc_decl *j, self.min_decl+self.inc_decl*(j+1)], Id)
				Id+=1
 		
	def return_id(self, a,b):
		I = 0
		J = 0
		for i in range(self.N):
			if self.grid[i][0].ra_is_in(a) :
				I = i
				break
		for j in range(self.N):
			if self.grid[I][j].decl_is_in(b) :
				J = j
				break
		return(I*self.N+J)

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

# To count elements in each partition 
def count_in_a_partition(iterator):
	yield sum(1 for _ in iterator)

def fill(grid, logData):
	ra = dico_source["ra"]
	decl = dico_source["decl"]
	col = logData.map(lambda line : line.split(','))\
			.map(lambda line : (grid.return_id(t_ra(float(line[ra])), float(line[decl])), ','.join(line)))\
			.partitionBy(grid.N*grid.N).map(lambda tu : tu[1])
	col.saveAsTextFile('hdfs:///user/'+identifiant + '/'+name_partition+'/')
	grid.histo = col.mapPartitions(count_in_a_partition)


G = Grid(minmaxs,7)
fill(G, sc.textFile("/tp-data/Source/Source-*.csv"))
csv_file_name = 'hdfs:///user/'+identifiant+'/'+name_partition+'/Partition_metadata/'
lines = G.generate_repartition()
sc.parallelize(lines,1).saveAsTextFile(csv_file_name)
print('The End.')
