import sys
import string
import re
import json
import MapReduce

scores = {}
tweetlist = []
count = 0
tweetcountlist = {}
tweetwordcount = 0
result = {}


mr = MapReduce.MapReduce()


def mapper(record):
	global count
	global result
	global tweetwordcount
	
	remstring = re.sub("(@[a-z|A-Z|0-9]+)|(#[a-z|A-Z|0-9]+)|(RT)|(\w+:\/\/\S+)|(retweet)|([0-9]+)"," ",record["text"])
	
	newstring = remstring.encode('utf-8').translate(None, string.punctuation)
	
	

	#print newstring
	
	lowernew = newstring.lower()
	#print lowernew
	tweetlist = lowernew.split()
	#print tweetlist
	count += 1
	s =set(tweetlist)

	
	
	
	for key in s:
		tweetcountlist.setdefault(key, [])
		tweetcountlist[key].append([count,tweetlist.count(key)])
		mr.emit_intermediate(key,tweetcountlist[key])
		

def reducer(key,list_of_values):
	total = 0.0
	
	mr.emit((key,len(list_of_values),list_of_values[0]))

			




tweet_data = open(sys.argv[1])
mr.execute(tweet_data, mapper, reducer)


