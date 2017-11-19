#
#	Aaron Enright
#	Exercise 2
#
#	finalresults.py - print the final results of the twitterstream
#

import sys
import psycopg2

def ShowResultsForWordList(wordList):
	query = "SELECT word, count FROM tweetwordcount "
	if len(wordList) > 0:
		query += "WHERE word in ("
		for wordcount in range(len(wordList)):
			query += "'" + wordList[wordcount] + "'"
			if wordcount < (len(wordList) - 1):
				query += ","
		query += ") "
	query += "ORDER BY word;"
	conn = psycopg2.connect(database="tcount", user="w205", password="", host="localhost", port="5432")
	cur = conn.cursor()
	
	cur.execute(query)
	records = cur.fetchall()
	for record in records:
		print 'total number of occurences of "' + record[0] + '": ' + str(record[1])
	conn.commit()
	conn.close()

ShowResultsForWordList(sys.argv[1:])

