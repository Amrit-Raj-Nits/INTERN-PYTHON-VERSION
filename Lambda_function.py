import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')

#reference to the table to be modified
table = dynamodb.Table('user-playlist-info')
trigger_source_table = dynamodb.Table('playlist-tracks')

#Log marking the beginning of the code
print('Loading function..')

def lambda_handler(event, context):
	print('------------------------')
	print(event)
	#1. Iterate over each record
	try:
		for record in event['Records']:
			#2. Handle event by type
			if record['eventName'] == 'INSERT':
				handle_insert(record)
			elif record['eventName'] == 'REMOVE':
				handle_remove(record)
		print('------------------------')
		print('Success!')
		return "Success!"
	except Exception as e: 
		print(e)
		print('------------------------')
		return "Error"


#Function defination to handle insert event
def handle_insert(record):
	#Notify beginning of the function in the log
	print("Handling INSERT Event")
	
	#Get newImage content
	newImage = record['dynamodb']['NewImage']
	
	#Parse values for the item to enter
	new_asin = newImage['asin']['S']
	new_pasin = newImage['pasin']['S']
	new_uid = newImage['uid']['N']
	new_album = newImage['album']['S']
	new_artist = newImage['artist']['S']
	new_duration = newImage['duration']['S']
	new_name = newImage['name']['S']
	new_popularity = newImage['popularity']['N']
	new_release = newImage['release']['S']
	new_genre = newImage['genre']['S']
	

	#Print the item details before entering
	print ('New row added with asin=' + new_asin)
	print ('New row added with pasin=' + new_pasin)
	print ('New row added with uid=' + new_uid)
	print ('New row added with album=' + new_album)
	print ('New row added with artist=' + new_artist)
	print ('New row added with duration=' + new_duration)
	print ('New row added with name=' + new_name)
	print ('New row added with popularity=' + new_popularity)
	print ('New row added with release=' + new_release)
	print ('New row added with genre=' + new_genre)
	
	#Getting all the tracks which belong to the playlist to which the new track has been added
	#Based on this data, we will compute the derived features to be updated in the playlist
	other_tracks = trigger_source_table.query(KeyConditionExpression = Key('pasin').eq(new_pasin))
	#Printing the list obtained into the logs
	print(other_tracks['Items'])
	
	#.................................
	artist_dir = {}
	album_dir = {}
	genre_dir = {}
	era_dir = {}
	era_dir[0] = 0
	era_dir[1] = 0
	era_dir[2] = 0
	era_dir[3] = 0
	era_dir[4] = 0
	no_of_tracks = 0
	track_duration_sum = 0
	track_popularity_sum = 0
	avg_popularity = 0
	for items in other_tracks['Items']:
		no_of_tracks = no_of_tracks + 1
		track_duration_sum = track_duration_sum + float(items['duration'])
		track_popularity_sum = track_popularity_sum + items['popularity']
		era_temp = items['release'].split(" ")
		era_year = int(era_temp[2])
		if era_year >=1920 and era_year <=1940:
			era_dir[0] = era_dir[0] + 1
		elif era_year >1940 and era_year <=1960:
			era_dir[1] = era_dir[1] + 1
		elif era_year >1960 and era_year <=1980:
			era_dir[2] = era_dir[2] + 1
		elif era_year >1980 and era_year <=2000:
			era_dir[3] = era_dir[3] + 1
		else:
			era_dir[4] = era_dir[4] + 1
			
		if items['artist'] in artist_dir.keys():
			artist_dir[items['artist']] = artist_dir[items['artist']] + 1
		else:
			artist_dir[items['artist']] = 1
		if items['album'] in album_dir.keys():
			album_dir[items['album']] = album_dir[items['album']] + 1
		else:
			album_dir[items['album']] = 1
		if items['genre'] in genre_dir.keys():
			genre_dir[items['genre']] = genre_dir[items['genre']] + 1
		else:
			genre_dir[items['genre']] = 1
	
	avg_popularity = track_popularity_sum/no_of_tracks
	#Print the log of the frequency of the three track features in the playlist
	print ("artist_dir = ")
	print (artist_dir)
	print ("album_dir = ")
	print (album_dir)
	print ("genre_dir = ")
	print (genre_dir)
	
	#List variables to store the artists, album, and genre which satisfy the threshold condition of 20%
	artist_list = []
	album_list = []
	genre_list = []
	
	#Finding the artists based on the threshold of 20% each
	for key, value in artist_dir.items():
		temp = ((value)/(no_of_tracks)) * 100
		if temp >= 20:
			artist_list.append(key)
	#Finding the albums based on the threshold of 20% each
	for key, value in album_dir.items():
		temp = ((value)/(no_of_tracks)) * 100
		if temp >= 20:
			album_list.append(key)
	#Finding the albums based on the threshold of 20% each
	for key, value in genre_dir.items():
		temp = ((value)/(no_of_tracks)) * 100
		if temp >= 20:
			genre_list.append(key)
	
	#Printing the final list to be updated in the user-playlist-info table
	print ("Top Artists:")
	print (artist_list)
	print ("Top Albums:")
	print (album_list)
	print ("Top Genres:")
	print (genre_list)
	
	#Making the lists each of length 5 by adding default values to them
	while len(artist_list) < 5:
		artist_list.append("")
	while len(album_list) < 5:
		album_list.append("")
	while len(genre_list) < 5:
		genre_list.append("")
	
	#Logging that no issue till line 144
	print("No issues till line - 144")
	
	#updating the user-playlist info...
	response = table.update_item(
		Key = {
			'uid' : int(new_uid),
			'pasin' : new_pasin
		},
		UpdateExpression = "set #p1 = :n, #p2 = :d, #p3 = :al1, #p4 = :al2, #p5 = :al3, #p6 = :al4, #p7 = :al5, #p8 = :ar1, #p9 = :ar2, #p10 = :ar3, #p11 = :ar4, #p12 = :ar5, #p13 = :g1, #p14 = :g2, #p15 = :g3, #p16 = :g4, #p17 = :g5, #p18 = :pop, #p19 = :er1, #p20 = :er2, #p21 = :er3, #p22 = :er4, #p23 = :er5",
		ExpressionAttributeValues = {
			':n' : no_of_tracks,
			':d' : int(track_duration_sum),
			':al1' :album_list[0],
			':al2' :album_list[1],
			':al3' :album_list[2],
			':al4' :album_list[3],
			':al5' :album_list[4],
			':ar1' :artist_list[0],
			':ar2' :artist_list[1],
			':ar3' :artist_list[2],
			':ar4' :artist_list[3],
			':ar5' :artist_list[4],
			':g1' :genre_list[0],
			':g2' :genre_list[1],
			':g3' :genre_list[2],
			':g4' :genre_list[3],
			':g5' :genre_list[4],
			':er1' :era_dir[0],
			':er2' :era_dir[1],
			':er3' :era_dir[2],
			':er4' :era_dir[3],
			':er5' :era_dir[4],
			':pop' :int(avg_popularity)
		},
		ExpressionAttributeNames = {
			'#p1' :"number-of-tracks",
			'#p2' :"playlist-duration",
			'#p3' :"album-rank1",
			'#p4' :"album-rank2",
			'#p5' :"album-rank3",
			'#p6' :"album-rank4",
			'#p7' :"album-rank5",
			'#p8' :"artist-rank1",
			'#p9' :"artist-rank2",
			'#p10' :"artist-rank3",
			'#p11' :"artist-rank4",
			'#p12' :"artist-rank5",
			'#p13' :"genre-rank1",
			'#p14' :"genre-rank2",
			'#p15' :"genre-rank3",
			'#p16' :"genre-rank4",
			'#p17' :"genre-rank5",
			'#p18' :"popularity",
			'#p19' :"era1",
			'#p20' :"era2",
			'#p21' :"era3",
			'#p22' :"era4",
			'#p23' :"era5"
		},
		ReturnValues = "UPDATED_NEW"
	)
	
	#Printing the success message
	print("Done handling INSERT Event - new row added to employee-copy table")


#Function defination to handle remove event
def handle_remove(record):
	#Notify beginning of the function in the log
	print("Handling REMOVE Event")

	#Parse oldImage
	oldImage = record['dynamodb']['OldImage']
	
	#Parse values for the item deleted
	removed_asin = oldImage['asin']['S']
	removed_pasin = oldImage['pasin']['S']
	removed_uid = oldImage['uid']['N']
	removed_album = oldImage['album']['S']
	removed_artist = oldImage['artist']['S']
	removed_duration = oldImage['duration']['S']
	removed_name = oldImage['name']['S']
	removed_popularity = oldImage['popularity']['N']
	removed_release = oldImage['release']['S']
	removed_genre = oldImage['genre']['S']
	

	#Print the item details before entering
	print ('New row added with asin=' + removed_asin)
	print ('New row added with pasin=' + removed_pasin)
	print ('New row added with uid=' + removed_uid)
	print ('New row added with album=' + removed_album)
	print ('New row added with artist=' + removed_artist)
	print ('New row added with duration=' + removed_duration)
	print ('New row added with name=' + removed_name)
	print ('New row added with popularity=' + removed_popularity)
	print ('New row added with release=' + removed_release)
	print ('New row added with genre=' + removed_genre)
	
	
	
	#Getting all the tracks which belong to the playlist to which the new track has been added
	#Based on this data, we will compute the derived features to be updated in the playlist
	other_tracks = trigger_source_table.query(KeyConditionExpression = Key('pasin').eq(removed_pasin))
	#Printing the list obtained into the logs
	print(other_tracks['Items'])
	
	#.................................
	artist_dir = {}
	album_dir = {}
	genre_dir = {}
	era_dir = {}
	era_dir[0] = 0
	era_dir[1] = 0
	era_dir[2] = 0
	era_dir[3] = 0
	era_dir[4] = 0
	no_of_tracks = 0
	track_duration_sum = 0
	track_popularity_sum = 0
	avg_popularity = 0
	for items in other_tracks['Items']:
		no_of_tracks = no_of_tracks + 1
		track_duration_sum = track_duration_sum + float(items['duration'])
		track_popularity_sum = track_popularity_sum + items['popularity']
		era_temp = items['release'].split(" ")
		era_year = int(era_temp[2])
		if era_year >=1920 and era_year <=1940:
			era_dir[0] = era_dir[0] + 1
		elif era_year >1940 and era_year <=1960:
			era_dir[1] = era_dir[1] + 1
		elif era_year >1960 and era_year <=1980:
			era_dir[2] = era_dir[2] + 1
		elif era_year >1980 and era_year <=2000:
			era_dir[3] = era_dir[3] + 1
		else:
			era_dir[4] = era_dir[4] + 1
			
		if items['artist'] in artist_dir.keys():
			artist_dir[items['artist']] = artist_dir[items['artist']] + 1
		else:
			artist_dir[items['artist']] = 1
		if items['album'] in album_dir.keys():
			album_dir[items['album']] = album_dir[items['album']] + 1
		else:
			album_dir[items['album']] = 1
		if items['genre'] in genre_dir.keys():
			genre_dir[items['genre']] = genre_dir[items['genre']] + 1
		else:
			genre_dir[items['genre']] = 1
	
	avg_popularity = 0
	if no_of_tracks > 0:
		avg_popularity = track_popularity_sum/no_of_tracks
	else:
		avg_popularity = 0
	#Print the log of the frequency of the three track features in the playlist
	print ("artist_dir = ")
	print (artist_dir)
	print ("album_dir = ")
	print (album_dir)
	print ("genre_dir = ")
	print (genre_dir)
	
	#List variables to store the artists, album, and genre which satisfy the threshold condition of 20%
	artist_list = []
	album_list = []
	genre_list = []
	
	#Finding the artists based on the threshold of 20% each
	for key, value in artist_dir.items():
		temp = ((value)/(no_of_tracks)) * 100
		if temp >= 20:
			artist_list.append(key)
	#Finding the albums based on the threshold of 20% each
	for key, value in album_dir.items():
		temp = ((value)/(no_of_tracks)) * 100
		if temp >= 20:
			album_list.append(key)
	#Finding the albums based on the threshold of 20% each
	for key, value in genre_dir.items():
		temp = ((value)/(no_of_tracks)) * 100
		if temp >= 20:
			genre_list.append(key)
	
	#Printing the final list to be updated in the user-playlist-info table
	print ("Top Artists:")
	print (artist_list)
	print ("Top Albums:")
	print (album_list)
	print ("Top Genres:")
	print (genre_list)
	
	#Making the lists each of length 5 by adding default values to them
	while len(artist_list) < 5:
		artist_list.append("")
	while len(album_list) < 5:
		album_list.append("")
	while len(genre_list) < 5:
		genre_list.append("")
	
	#Logging that no issue till line 144
	print("No issues till line - 144")
	
	#updating the user-playlist info...
	response = table.update_item(
		Key = {
			'uid' : int(removed_uid),
			'pasin' : removed_pasin
		},
		UpdateExpression = "set #p1 = :n, #p2 = :d, #p3 = :al1, #p4 = :al2, #p5 = :al3, #p6 = :al4, #p7 = :al5, #p8 = :ar1, #p9 = :ar2, #p10 = :ar3, #p11 = :ar4, #p12 = :ar5, #p13 = :g1, #p14 = :g2, #p15 = :g3, #p16 = :g4, #p17 = :g5, #p18 = :pop, #p19 = :er1, #p20 = :er2, #p21 = :er3, #p22 = :er4, #p23 = :er5",
		ExpressionAttributeValues = {
			':n' : no_of_tracks,
			':d' : int(track_duration_sum),
			':al1' :album_list[0],
			':al2' :album_list[1],
			':al3' :album_list[2],
			':al4' :album_list[3],
			':al5' :album_list[4],
			':ar1' :artist_list[0],
			':ar2' :artist_list[1],
			':ar3' :artist_list[2],
			':ar4' :artist_list[3],
			':ar5' :artist_list[4],
			':g1' :genre_list[0],
			':g2' :genre_list[1],
			':g3' :genre_list[2],
			':g4' :genre_list[3],
			':g5' :genre_list[4],
			':er1' :era_dir[0],
			':er2' :era_dir[1],
			':er3' :era_dir[2],
			':er4' :era_dir[3],
			':er5' :era_dir[4],
			':pop' :int(avg_popularity)
		},
		ExpressionAttributeNames = {
			'#p1' :"number-of-tracks",
			'#p2' :"playlist-duration",
			'#p3' :"album-rank1",
			'#p4' :"album-rank2",
			'#p5' :"album-rank3",
			'#p6' :"album-rank4",
			'#p7' :"album-rank5",
			'#p8' :"artist-rank1",
			'#p9' :"artist-rank2",
			'#p10' :"artist-rank3",
			'#p11' :"artist-rank4",
			'#p12' :"artist-rank5",
			'#p13' :"genre-rank1",
			'#p14' :"genre-rank2",
			'#p15' :"genre-rank3",
			'#p16' :"genre-rank4",
			'#p17' :"genre-rank5",
			'#p18' :"popularity",
			'#p19' :"era1",
			'#p20' :"era2",
			'#p21' :"era3",
			'#p22' :"era4",
			'#p23' :"era5"
		},
		ReturnValues = "UPDATED_NEW"
	)
	
	"""
	
	#Deleting the content from the copy table
	table.delete_item(
		Key = {
			"id" : int(del_id),
			"name" : del_name
		}
	)
	"""
	
	#Printing the success message
	print("Done handling REMOVE Event")
	
"""
As of now, our code does not need an update code as we do not allow manipulation of individual track data from here.
This might be useful later.
def handle_modify(record):
	print("Handling MODIFY Event")
	#Parse oldImage and score
	oldImage = record['dynamodb']['OldImage']
	oldScore = oldImage['attribute_name']['attribute_type']
	
	#Parse oldImage and score
	newImage = record['dynamodb']['NewImage']
	newScore = newImage['attribute_name']['attribute_type']
	#Check for change
	if oldScore != newScore:
		print('Scores changed - oldScore=' + str(oldScore) + ', newScore=' + str(newScore))
	print("Done handling MODIFY Event")
"""
	

"""
Sample data to test: 
Case 1: 
{
  "album": "Prime Prine: The Best of John Prine",
  "artist": "John Prine",
  "asin": "B001OGLRT2",
  "duration": "2.1",
  "genre": "Country",
  "name": "Grandpa Was a Carpenter",
  "pasin": "P100000001",
  "popularity": 8,
  "release": "September 30 1988",
  "uid": 1000000001
}
{
  "album": "Prime Prine: The Best of John Prine",
  "artist": "John Prine",
  "asin": "B001OGNR9U",
  "duration": "4.3",
  "genre": "Country",
  "name": "Donald and Lydia",
  "pasin": "P100000001",
  "popularity": 7,
  "release": "September 30 1988",
  "uid": 1000000001
}
{
  "album": "Flower Boy",
  "artist": "Tyler The Creator",
  "asin": "B073K8XRKJ",
  "duration": "3.43",
  "genre": "Rap & Hip Hop",
  "name": "Garden Shed [Explicit]",
  "pasin": "P100000001",
  "popularity": 1,
  "release": "December 1 2017",
  "uid": 1000000001
}
"""
