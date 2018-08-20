import sys
import string
import re
import json
import MapReduce

scores = {}
tweetlist = []
tweetcount = 0 


afinnfile = open(sys.argv[1])
mr = MapReduce.MapReduce()


def mapper(record):
	global tweetcount
	newstring = record["text"].encode('utf-8').translate(None, string.punctuation)
	lowernew = newstring.lower()
	tweetlist = lowernew.split()
	tweetcount +=1
	for key in tweetlist:
		if key in scores:
			mr.emit_intermediate(tweetcount,scores[key])
		else:
			mr.emit_intermediate(tweetcount,0)

def reducer(key,list_of_values):
	total = 0.0
	for v in list_of_values:
		total += float(v)
	mr.emit((key, total))

			



for line in afinnfile:
    term, score  = line.split("\t")  # The file is tab-delimited. #\t means the tab character.
    scores[term] = int(score)  # Convert the score to an integer.
tweet_data = open(sys.argv[2])
mr.execute(tweet_data, mapper, reducer)


