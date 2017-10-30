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
print("AIM : get the object that moved the most from the observations.") 

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

PATH_source = "Samples/Sources/Source/Source-*.csv"
attr = ["objectId", "sourceId","ra","decl"]

def get_obj_summary(file_path, my_attr, dico) :
        x = dico[my_attr[0]]
	y = dico[my_attr[1]]
	ra = dico[my_attr[2]]
	decl = dico[my_attr[3]]

        logData = sc.textFile(file_path)
        col = logData.map(lambda line : line.split(','))\
		.map(lambda line : (line[x], (1,int(line[y]), float(line[ra]),float(line[decl]))))\
		.filter(lambda tuple : tuple[0] != 'NULL')\
		.reduceByKey(lambda a,b : (a[0]+b[1], max(a[1],b[1]), min(a[2],b[2]), max(a[2],b[2]), min(a[3],b[3]), max(a[3],b[3]) ) )
        return(col)
objects_move = get_obj_summary(PATH_source, attr, dico_source)
print(objects_move.first())

### Object that moved the most

best = objects_move.filter(lambda l : len(l[1])==6)\
	.map(lambda mo : (mo[0],(mo[1][2]-mo[1][3])**2+(mo[1][4]-mo[1][5])**2   ))
print(best.first())
best = best.reduce(max)

print("Object that moved the most has id %s with distance %f "%(best[0],sqrt(best[1])))
print("__  __  __  __  __")
