""" 
 AUTHOR : Charles GAYDON
 LAST EDITED : 30/10/2017
 
 SCRIPT : SparkApp1.py
 USAGE : spark-submit SparkApp1.py 
        OR 
    spark-submit SparkApp1.py 2> out  
    

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


### count occurences and get position data
print('Import of min and max')

PATH_source = "/tp-data/Source/Source-*.csv"
attr = ["ra","decl"]

def get_range(file_path, my_attr, dico) :
    ra = dico[my_attr[0]]
    decl = dico[my_attr[1]]
    logData = sc.textFile(file_path)
    col = logData.map(lambda line : line.split(','))\
        .map(lambda line : (float(line[ra]),float(line[ra]), float(line[decl]), float(line[decl]) ))\
        .filter(lambda t : t[0] != 'NULL' and t[2]!='NULL')\
        .reduce(lambda a,b : (min(a[0],b[0]), max(a[1],b[1]), min(a[2],b[2]), max(a[3],b[3]) ))
    return(col)

if not os.path.isfile('ra_decl_range.csv') : 
    with open('ra_decl_range.csv','wb') as file:
        minmaxs = get_range(PATH_source, attr, dico_source)
        print('here')
        print(minmaxs)
        wr = csv.writer(file, delimiter = ',')    
        wr.writerow(minmaxs)
else :
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
		self.min_ra = minmaxs[0]
		self.max_ra = minmaxs[1]
		self.inc_ra = (self.max_ra-self.min_ra)/N
		self.min_decl = minmaxs[2]
		self.max_decl = minmaxs[3]
		self.inc_decl = (self.max_decl-self.min_decl)/N
		self.grid = [[[0] for x in range(N)] for x in range(N)] 
		print(self.grid)
		Id = 0
		for i in range(N):
			for j in range(N):
				self.grid[i][j] = Zone([self.inc_ra*i, self.inc_ra*(i+1), self.inc_decl *j, self.inc_decl*(j+1)], Id)
				Id+=1
				
	def fill(self, file_path):
		ra = dico_source["ra"]
		decl = dico_source["decl"]
		logData = sc.textFile(file_path)
		col = logData.map(lambda line : line.split(','))\
				.map(lambda line : (self.return_id(float(line[ra]), float(line[decl])), line))\
				.toDF(["Zone","Line"])\
				.saveAsTextFile('hdfs:///user/p1513939/Partion1/').partitionBy("Zone")#.text("Line")
		
	def return_id(self, a,b):
		I = 0
		J = 0
		for i in range(N):
			if ra_is_in(a,self.grid[i][0]) :
				I = i
				break
		for j in range(N):
			if decl_is_in(b,self.grid[I][j]) :
				J = j
				break
		return("Zone_"+str(I)+'_'+str(J))
	
	
G = Grid(minmaxs,7)
G.fill("/tp-data/Source/Source-001.csv")

