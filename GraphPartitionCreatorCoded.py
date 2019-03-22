#from mpi4py import MPI
import numpy as np
import sys
import random
import threading
import time
#import networkx as nx
import copy
import itertools
import _pickle as pickle
#import gmpy2
import math
from scipy.misc import comb

# To create the dictionaries for the workers in the coded case
# N is a multiple of K-choose-r

with open("Dict_0.txt", "rb") as myFile:
	Gdict = pickle.load(myFile)
K=int(sys.argv[1])
N=int(sys.argv[2])
r=int(sys.argv[3])

kcr=int(comb(K,r))
print("Number of workers K", K)
print("Number of nodes N",N)
print("Computational Load R",r)
g=N/kcr
#print("Number of multiple of total combinations (Batch Size)",g)
fid=[x for x in range(1,kcr+1)]
#print("Batch Ids",fid)
ltemp1=[x for x in range(1,K+1)]
#print("Machine Sets",ltemp1)
ltemp2=itertools.combinations(ltemp1, r)
WSubsetR={}
fidWSet={}
fidNodes={}
Gdist= [None] * K
for j in range(K):
	Gdist[j]={}

i1=1
for x in ltemp2:
	WSubsetR[i1]=x
	fidWSet[x]=i1
	i1+=1

# fidNodes
i2=0
for i1 in range(1,kcr+1):
	fidNodes[i1]=tuple([x for x in range(i2*int(g),(i2+1)*int(g))])
	i2+=1
	#print("File id",i1," => Nodes",fidNodes[i1])

for i1 in fid: 
	listKeys=[]
	listNeighboursList=[]
	listNeighboursSize=[]

	ltemp1=fidNodes[i1]

	for i2 in ltemp1:
		listKeys.append(i2)
		listNeighboursList.append(Gdict[i2])
		listNeighboursSize.append(len(Gdict[i2]))

	ltemp2=WSubsetR[i1]
	for i2 in ltemp2:
		Gdist[i2-1][i1]={}
		Gdist[i2-1][i1][1]=listKeys
		Gdist[i2-1][i1][2]=listNeighboursList
		Gdist[i2-1][i1][3]=listNeighboursSize

#print("Machine\t\tNodes[List1]\t\tNeighbours[List2]")
for machine in range(K):
	#print("Machine",machine)
	for batch in Gdist[machine].keys():
		count_node = len(Gdist[machine][batch][1])
		for i in range(count_node):
			node = Gdist[machine][batch][1][i]
			neighbors = Gdist[machine][batch][2][i]
			#print("\tBatch No",batch,"Node",node,"Neighbors",neighbors)
	print()
for j in range(K):
	#print(j)
	i=j+1

	# change 3 to suitable r as required
	with open("DictC3_%s.txt"%i, "wb") as myFile:
		pickle.dump(Gdist[j], myFile)
	myFile.close()
	

