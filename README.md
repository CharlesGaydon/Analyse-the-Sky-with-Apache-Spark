# Spark - Dealing With The Sky
Author : Charles Gaydon
Last edited : 30/10/2017

## Outline

I used Apache Spark to deal with massive astronomical data from the PetaSKy project. 
This work is intended as an example of what simple Spark syntax can achieve on massive data, and is based on [this](https://forge.univ-lyon1.fr/EMMANUEL.COQUERY/tp-spark-2017/blob/master/README.md) exercice (in french).

This repository contains :

- a python script designed for Apache Spark, using Map/Reduce ;
- two csv files containing samples of the sources and object we deal with ;
- two sql files containing the description of the  attributes of the aforementionned source and object data ;
- a Source-subset folder, containing a small subset of the Source files from the PetaSky project. 

 We first extract the attribute index from the corresponding SQL description, and then determine the object that has the most moved along the observations. 
 The distance travelled is roughly estimated as the euclidian distance between the max and min values from two positionnal attribute : *ra* and *decl*.
 More comments are in the python script.
 
## USAGE : 

- To run in a Spark verbose mode : *spark-submit SimpleApp1.py*;
- To show only meaningfull results : *spark-submit SimpleApp1.py 2> out*.
