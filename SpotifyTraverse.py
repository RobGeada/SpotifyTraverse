from __future__ import print_function
import pyspark
from pyspark.sql import Row
import inquirer
import os,sys
#import APIRequests as API

spark = pyspark.SparkContext("local[*]")
spark.setLogLevel("OFF")
from pyspark.sql import SQLContext
sqlc = SQLContext(spark)
from pyspark.sql import SQLContext
from pyspark.sql.functions import avg

#===========PROGRAM FLOW HELPERS==========
def wait(label):
    raw_input("waiting for user input at {}".format(label))

def printTitle():
    os.system('clear')
    print("==========================================================================")
    print("Welcome to")   
    print("   _____             _   _  __         _______                 _           ")
    print("  / ____|           | | (_)/ _|       |__   __|               | |          ")
    print(" | (___  _ __   ___ | |_ _| |_ _   _     | |_ __ __ ___      _| | ___ _ __ ")
    print("  \___ \| '_ \ / _ \| __| |  _| | | |    | | '__/ _` \ \ /\ / / |/ _ \ '__|")
    print("  ____) | |_) | (_) | |_| | | | |_| |    | | | | (_| |\ V  V /| |  __/ |   ")
    print(" |_____/| .__/ \___/ \__|_|_|  \__, |    |_|_|  \__,_| \_/\_/ |_|\___|_|   ")
    print("        | |                     __/ |                                      ")
    print("        |_|                    |___/                                       ")
    print("==============Version 1.3=========================9/7/2016================\n")

#===========DATA READING=========
printTitle()
cwd = os.getcwd()
availableSizes = os.listdir(cwd+"/Datasets")
availableSizes = [int(x.split("_")[1]) for x in availableSizes]
availableSizes.sort()
availableSizes = ["{} artists".format(x) for x in availableSizes]

chooseSize= [
            inquirer.List('chooseSize',
                 message="Select graph size to analyze",
                 choices=availableSizes
                ),
            ]
dataSetSize = inquirer.prompt(chooseSize)['chooseSize'].split(" artists")[0]

print("Importing data from top {} artists...".format(dataSetSize))
edges = sqlc.read.parquet("{}/Datasets/BackupTrawl_{}/trawlEdges.parquet".format(cwd,dataSetSize))
verts = sqlc.read.parquet("{}/Datasets/BackupTrawl_{}/trawlVerts.parquet".format(cwd,dataSetSize))

#======CREATE VERT DICTIONARY===========
vIDDict = {}
nIDDict = {}
encoding = "utf8"
print("Creating reference dictionary...")
artistList = verts.collect()
for i in artistList:
    nID = i[1].encode(encoding)
    vertID = i[0]
    vIDDict["vertID {}".format(vertID)]={'nID':nID}
    nIDDict["nID {}".format(nID)] = {'rel':[],'nID':nID,'distance':"infinity",'degree':0,'parent':[]}

#======DICTIONARY HELPER FUNCTIONS=========
def getNode(nID):
    return nIDDict.get("nID {}".format(nID))

def getNIDByKey(key):
    return nIDDict.get("{}".format(key))['nID']   

def getnID(vertID):
    return vIDDict.get("vertID {}".format(vertID))['nID']

def setRel(nID,B):
    nIDDict.get("nID {}".format(nID))['rel'] = B   

def getRel(nID):
    return nIDDict.get("nID {}".format(nID))['rel']   

def printNode(nID):
    print("{} is related to {}".format(nID,getRel(nID)))

def setParent(nID,parent):
    nIDDict.get("nID {}".format(nID))['parent'] = parent 

def setDist(nID,distance):
    nIDDict.get("nID {}".format(nID))['distance'] = distance 

def getParent(nID):
    return nIDDict.get("nID {}".format(nID))['parent'] 

def getDist(nID):
    return nIDDict.get("nID {}".format(nID))['distance'] 

def getName(nID):
    return nID.split(" _ _ ")[0]

#======DATA FORMATTING 1==============
print("Formatting data, round 1...")
edges = edges.rdd.map(lambda x: (getnID(x[0]),getnID(x[1])))
edges = edges.reduceByKey(lambda x,y: x + "_as_well_as_" + y)
edges = edges.map(lambda x: (x[0],x[1].split("_as_well_as_")))

#======CREATE GRAPH==================
print("Creating graph...(this'll take a bit)")
for artist in edges.collect():    
    setRel(artist[0],artist[1])

#======DATA FORMATTING 1==============
print("Formatting data, round 2...")
artRef = verts.rdd.map(lambda x: (getName(x[1]),x[1]))
artRef = sqlc.createDataFrame(artRef,['name','nID'])

#======TRAVERSAL=====================
def clean():
    for key,value in nIDDict.items():
        keyNID = getNIDByKey(key)
        setParent(keyNID,[])
        setDist(keyNID,"infinity")

def traverse(orig):
    clean()
    Q =[orig]
    getNode(Q[0])['distance']=0
    runningSum = 0.0
    maxDist = 0
    maxArtist=""
    numNodes = 0.0
    while Q:
        current = getNode(Q[0])
        Q = Q[1:len(Q)]
        for i in current['rel']:
            if getDist(i) == "infinity":
                distTo = current['distance']+1
                setDist(i,distTo)
                setParent(i,current)
                numNodes+=1
                runningSum+=distTo
                if distTo > maxDist:
                    maxArtist = current['nID']
                    maxDist = distTo
                Q.append(i)
    if numNodes>0:
        return ({"radialDist":int(maxDist),
                 "furthestArtist":maxArtist,
                 "aveDist":float(runningSum/numNodes),
                 "numNodes":numNodes})
    else:
        return ({"radialDist":None,
                 "furthestArtist":None,
                 "aveDist":int(dataSetSize),
                 "numNodes":0})

def parse(dest,mode="Reader"):
    path = getNode(dest)
    result = [path['nID']]
    next = getParent(path['nID'])
    while (next != []):
        result.append(next['nID'])
        next = next['parent']
    result.reverse()

    if len(result)==1:
        print("No connection!")
    else:
        if mode=="Reader":
            if (len(result)-1 == 1):
                print("\nThe shortest path (1 click) is:")
            else:    
                print("\nThe shortest path ({} clicks) is:".format(len(result)-1)) 
            for i in result:
                if i != result[-1]:
                    #name = i.encode("utf8")
                    name = getName(i)
                    print("{} -> ".format(name),end="")
                else:
                    #name = i.encode("utf8")
                    name = getName(i)
                    print("{}".format(name))
        else:
            playlistName = getName(result[0]) + " to " + getName(result[-1]) +\
            " ({}) : ".format(dataSetSize)
            path = []
            for i in result:
                path.append(i)
            print(path)
            API.makePlaylist(playlistName,path)

#=========UI helpers=============

def distinguishDups(names):
    outNames,i = [],0
    while i<len(names):
        j=i+1
        while j<len(names) and getName(names[j])==getName(names[i]):
            j+=1
        nameSplice = names[i:j]
        if len(nameSplice)==1:
            outNames.append(getName(nameSplice[0]))
        else:
            for index,name in enumerate(nameSplice):
                legible = getName(name)
                relations = getRel(name)
                if len(relations)>2:
                    relationA,relationB = getName(relations[0]),getName(relations[1])
                    outNames.append("{} (Related to {}, {})".format(legible,relationA,relationB))
                elif len(relations)==1:
                    outNames.append("{} (Related to {})".format(legible,getName(relations[0])))
                else:
                    outNames.append("{} (No relations)".format(legible))
        i=j
    return outNames

def selectArtist(searchResults):
    searchResults=distinguishDups(searchResults)
    questions = [
        inquirer.List('searched',
                    message="Search complete! Our best guesses",
                    choices=searchResults+["Not here? Search again."]
                ),
    ]
    answers = inquirer.prompt(questions)
    print(answers['searched'])
    return answers['searched']

def flowDirection(orig,options="ALL"):
    if options=="ALL":
        userChoices = ['Find a route from {} to someone else'.format(orig),
                       'Display origin artist statistics',
                       'Generate a playlist from the path',
                       'Start over',
                       'Quit']
    elif options=="NO_STAT":
        userChoices = ['Find a route from {} to someone else'.format(orig),
                       'Generate a playlist from the path',
                       'Start over',
                       'Quit']
    else:
        userChoices = ['Find a route from {} to someone else'.format(orig),
                       'Display origin artist statistics',
                       'Start over',
                       'Quit']
    questions = [
        inquirer.List('direction',
                    message="Would you like to",
                    choices=userChoices
                ),
    ]
    answers = inquirer.prompt(questions)
    return answers['direction']

def lookupOrig():
    content = False
    while not content:
        printTitle()

        print("Welcome to the routing interface! \n")
        sqlc.registerDataFrameAsTable(artRef,"artistTable")
        searchTerm = raw_input("Selecting Origin: Who would you like to search for? ")
        
        print("Searching...")
        searched = sqlc.sql("SELECT nID FROM artistTable WHERE name LIKE '%{}%'".format(searchTerm))
        results = searched.orderBy("nID").take(100)
        nIDResults = []
        searchResults=[]
        for i in results:
            searchResults.append(i[0])
            nIDResults.append(i[0])
        searchResults = distinguishDups(searchResults)
        queryResult = selectArtist(searchResults)

        if queryResult == "Not here? Search again.":
            continue
        else:
            print(searchResults)
            orig = nIDResults[searchResults.index(queryResult)]
            content = True
    return orig

def lookupDest(origName):
    content=False
    while not content:
        printTitle()
        
        print("Routing from {}...".format(origName))
        sqlc.registerDataFrameAsTable(artRef,"artistTable")
        searchTerm = raw_input("\nSelecting Destination: Who would you like to search for? ")
        print("Searching...")
        searched = sqlc.sql("SELECT nID FROM artistTable WHERE name LIKE '%{}%'".format(searchTerm))
        results = searched.orderBy("nID").take(100)
        nIDResults = []
        searchResults=[]
        for i in results:
            searchResults.append(i[0])
            nIDResults.append(i[0])
        searchResults = distinguishDups(searchResults)
        queryResult = selectArtist(searchResults)
        if queryResult == "Not here? Search again.":
            continue
        else:
            dest = nIDResults[searchResults.index(queryResult)]
            content = True
    return dest


#========UI FUNCTIONS===========================

def initializeOrig():
    orig = lookupOrig().encode("utf8")
    origName = getName(orig)
    printTitle()
    print("Finding all routes from {}...".format(origName))
    stats = traverse(orig)
    return (orig,origName,stats)

def getDest(origName):
    dest = lookupDest(origName).encode("utf8")
    destName = getName(dest)
    printTitle()
    print("Routing from {} to {}...".format(origName,destName))
    parse(dest,mode="Reader")
    print("\n")
    return (dest,destName)

def printStats(stats):
    furthest = getName(stats['furthestArtist'])
    print("GRAPH STATISTICS FOR {}".format(origName.upper()))
    print("================================================")
    print("Most distant artist is {}, at {} clicks".format(furthest,stats["radialDist"]))
    print("Average distance to rest of Spotify: {} clicks".format(round(stats['aveDist'],2)))
    print("Number of reachable artists: {}".format(int(stats["numNodes"])))
    print("================================================\n")

#=========BEGIN MAIN UI LOOP=====================
while 1:
    #initialize results
    orig,origName,stats = initializeOrig()
    startingOver,userOptions = False,"NO_PATH"

    #single origin main loop
    while not startingOver:
        content = flowDirection(origName,options=userOptions).split()[0]
        
        #find route
        if content == "Find":
            dest,destName=getDest(origName)
            options = "ALL"
        
        #generate 9d playlist
        elif content == "Generate":
            print("Playlist generation is WIP, sorry!")
            raw_input("Press enter to return to main interface. ")
            #parse(dest,mode="Playlist")
        
        #display origin stats
        elif content=="Display":
            printStats(stats)
        
        #find new origin
        elif content=="Start":
            startingOver = True
        
        #quit
        else:
            os.system('clear')
            sys.exit()