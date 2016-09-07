import pyspark
from pyspark.sql import Row
import os
import shutil

#=============CLEAN FILES================
print "Beginning data formatting..."
pyspark.SparkContext.setSystemProperty('spark.default.parallelism','24')
spark = pyspark.SparkContext("local[*]")

from pyspark.sql import SQLContext
sqlc = SQLContext(spark)
jsonIn = sqlc.read.json("trawlResults.json")


#===========RDD helper funcs==========
graphRDD = jsonIn.rdd.zipWithUniqueId().map(lambda x: (x[0][0],x[0][1],x[1]))
vertsRDD = graphRDD.map(lambda x: (x[2],x[0]))
vertsDF  = sqlc.createDataFrame(vertsRDD)
vertsDF.write.parquet("trawlVerts.parquet")

#=========DICTIONARY CREATION++++++++++
vertDict = {}
print "Creating graph nodes..."
for i in vertsDF.collect():
	nID = i[1].encode("utf8")
	vertDict["nID {}".format(nID)]=i[0]
def getNode(nID):
    return vertDict.get("nID {}".format(nID.encode("utf8")))

#=========EDGE CREATION=====================
print "Creating graph edges"
def makeEdges(pointA,listB):
	edges = []
	for pointB in listB:
		edges.append((getNode(pointA),getNode(pointB),"related"))
	return edges

edgesRDD = graphRDD.flatMap(lambda x: makeEdges(x[0],x[1]))
edgesDF  = sqlc.createDataFrame(edgesRDD)
edgesDF.write.parquet("trawlEdges.parquet")
print "Finished writing graph data!"
