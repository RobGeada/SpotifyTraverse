import spotipy
import spotipy.util as util
import os,sys
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from collections import OrderedDict

#========SETUP AUTHORIZATIONS===========
os.environ['SPOTIPY_CLIENT_ID']='1083196f93df4b68a898154b061d695b'
os.environ['SPOTIPY_CLIENT_SECRET']='d9fd53774b2546fb931a7bba31d208f4'
os.environ['SPOTIPY_REDIRECT_URI']='http://localhost:8888/callback/'

#========GET USER INFO==================s
scope    = "playlist-modify-public"
username = "robfrosty"
os.system("echo $SPOTIPY_REDIRECT_URI")
token = util.prompt_for_user_token(username,scope=scope)
sp = spotipy.Spotify(auth = token)
emphasized = None

#retrieve top songs from every artist in path
def parsePath(path):
	print "Parsing path..."
	pathTracks = []
	for artist in path:
		artist = artist.split(" _ _ ")[1]
		topTracks =  sp.artist_top_tracks(artist,country='US')["tracks"]
		artistSongs = []
		for song in topTracks:
			song = song["uri"].split(":")[2]
			songFeat = sp.audio_features(tracks=[song])
			artistSongs.append((song,songFeat))
		pathTracks.append(artistSongs)
	return pathTracks

#========Distance metric helpers===============
#get song qualities
def extractMetrics(song):
	songMetrics = []
	for key,value in song[1][0].items():
		if key in ["energy","liveness","tempo","speechiness","acousticness","instrumentalness","danceability","loudness","valence"]:
			if key == "loudness":
				value = (value+60)/60
			if key == "tempo":
				value = value/60
			value-=.5
			songMetrics.append(value)
	return songMetrics

#find the 9-dimensional Cartesian distance between songs
def songDistance(songA,songB):
	songAMetrics=extractMetrics(songA)
	songBMetrics=extractMetrics(songB)
	songAMetrics,songBMetrics = np.array(songAMetrics),np.array(songBMetrics)

	dot = np.dot(songAMetrics,songBMetrics)
	ALen= np.linalg.norm(songAMetrics)
	BLen= np.linalg.norm(songBMetrics)

	cosine = dot/(ALen*BLen)

	diff = songBMetrics-songAMetrics
	sqDiff=np.power(diff,2)
	dist = np.sum(sqDiff)
	return dist

#turn id into human-readable info
def getTrackInfo(id):
	song = sp.track(id)
	return (song['name'],song['artists'][0]['name'],song['id'])

#=========ALIGN WHEELS=====================
#align the most similar songs of two artist "wheels"
def alignWheels(wheelA,wheelB):
	alignment = []
	for a,songA in enumerate(wheelA):
		for b,songB in enumerate(wheelB):
			alignment.append((songDistance(songA,songB),a,b))
	alignment.sort(key=lambda x: x[0])
	songA = alignment[0][1]
	songB = alignment[0][2]
	songA = getTrackInfo(wheelA[songA][0])
	songB = getTrackInfo(wheelB[songB][0])
	return (songA,songB)

#=======GET SONG PAIRS=====================
#align all the wheels in the path
def alignAll(pathTracks):
	path=[]
	print "Aligning wheels..."
	for i,artist in enumerate(pathTracks):
		if i < len(pathTracks)-1:
			path.append(alignWheels(pathTracks[i],pathTracks[i+1]))

	pairPath = [path[0][0]]
	for i,pair in enumerate(path):
		if i < len(path)-1:
			pairPath.append((path[i][1],path[i+1][0]))
	pairPath.append(path[-1][1])
	return pairPath

#=======CREATE ARTIST GRAPHS===============
#get the shortest paths to traverse the intrawheel space
def shortestPath(pairPath,pathTracks):
	songs=[]
	for i,artist in enumerate(pathTracks):
		if artist == pathTracks[0]:
			songs.append(pairPath[0])
		elif artist==pathTracks[-1]:
			songs.append(pairPath[-1])
		else:
			G = nx.Graph()
			for songA in artist:
				for songB in artist:
					G.add_edge(songA[0],songB[0],distance=songDistance(songA,songB))
			path = nx.shortest_path(G,source=pairPath[i][0][2],target=pairPath[i][1][2],weight="distance")
			songPath = []
			for song in path:
				songPath.append(getTrackInfo(song))
			songs.append(songPath)
	return songs

def flattenPath(path):
	flatPathMid = [item for sublist in path[1:-1] for item in sublist]
	flatPath = [path[0]]
	flatPath.extend(flatPathMid)
	flatPath.append(path[-1])
	flatPath = list(OrderedDict.fromkeys(flatPath))
	return flatPath

def makePlaylist(name,path):
	global emphasized
	pathTracks = parsePath(path)
	content = "y"
	while content=="y":
		nameCopy = name
		emphasized = ""#raw_input("Focus on: ")
		nameCopy += emphasized
		aligned = alignAll(pathTracks)
		
		flatPath = flattenPath(wheelSongList)
		playlistID = sp.user_playlist_create(username,nameCopy)
		playlist = []
		for song in flatPath:
			playlist.append(song[2])
			print "Adding {}...".format(getTrackInfo(song[2]))
		sp.user_playlist_add_tracks(username, playlistID["id"], tracks=playlist)
		
		content = raw_input("Adjust dimensional focus? (y/n) ")

if __name__ == "__main__":
	testPathA = ['Johann Sebastian Bach _ _ 5aIqB5nVVvmFsvSdExz408', 'Wolfgang Amadeus Mozart _ _ 4NJhFmfw43RLBLjQvxDuRS', 'Claude Debussy _ _ 1Uff91EOsvd99rtAupatMP', 'Erik Satie _ _ 459INk8vcC0ebEef82WjIK', 'Steve Reich _ _ 1aVONoJ0EM97BB26etc1vo', 'Jonny Greenwood _ _ 0z9s3P5vCzKcUBSxgBDyLU', 'Thom Yorke _ _ 4CvTDPKA6W06DRfBnZKrau', 'Patrick Wolf _ _ 6s92YZUPkTK1HL1WIGrPKE', 'The Irrepressibles _ _ 1v5bOzXbhrQ57qSvRwGA6s', 'Woodkid _ _ 44TGR1CzjKBxSHsSEy7bi9']
	testPathB = ['Chief Keef _ _ 15iVAtD3s3FsQR4w1v6M0P', 'Future _ _ 1RyvyyTE3xzB2ZywiAwp0i', 'Drake _ _ 3TVXtAsR1Inumwj472S9r4', 'Frank Ocean _ _ 2h93pZq0e7k5yf4dywlkpM', 'Azealia Banks _ _ 7gRhy3MIPHQo5CXYfWaw9I', 'Lana Del Rey _ _ 00FQb4jTyendYWaN8pK0wa', 'Florence + The Machine _ _ 1moxjboGR7GNWYIMWsRjgG', 'Mumford & Sons _ _ 3gd8FJtBJtkRxdfbTu19U2', 'Iron & Wine _ _ 4M5nCE77Qaxayuhp3fVn4V']
	makePlaylist("test",testPathA)