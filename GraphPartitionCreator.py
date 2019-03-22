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
import math

#################create a subgraph and then partition and then create worker dictionaries#############

K=int(sys.argv[1]) # Number of Workers
N=int(sys.argv[2]) # Number of Nodes
p=0.3 # Initial probability

Dict={}
print("Number of workers",K)
print("Number of nodes",N)
print("Initializing dictionary of size",N)
for i in range(N):
	Dict[i]=[]

for i in range(N):
	for j in range(i+1,N):
		sample=np.random.binomial(1, p, 1) #(n,p,number of samples)
		if sample[0]==1:
			Dict[i].append(j)
			Dict[j].append(i)

#for i in range(N):
#	print("Node",i,"has following neighbours",Dict[i])

# creating separate files

#print("Mapping",N,"nodes to",K,"machine using",K,"files")
Gdist= [None] * K
for j in range(K):
	Gdist[j]={}
	Gdist[j][1]=[]
	Gdist[j][2]=[]

#print("Machine\t\tNodes(List1)\t\tNode(List2)")
for j in range(K):
	for i in range(j,N,K):
		Gdist[j][1].append(i)
		Gdist[j][2].append(Dict[i])
		#print(j,"\t\t",i,"\t\t\t",Dict[i])


for j in range(K):
	i=j+1
	with open("Dict_%s.txt"%i, "wb") as myFile:
		pickle.dump(Gdist[j], myFile)
	myFile.close()

# to keep the dictionary for further proccessing for Coded case
i=0
with open("Dict_%s.txt"%i, "wb") as myFile:
	pickle.dump(Dict, myFile)
myFile.close()	

