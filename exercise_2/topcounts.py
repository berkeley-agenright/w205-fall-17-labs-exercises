#
#	Aaron Enright
#	Exercise 2
#
#	topcounts.py - print the top words in the twitterstream
#
import sys
import psycopg2

def ShowTopWordCounts(limitNumber):
	conn = psycopg2.connect(database="tcount", user="w205", password="", host="localhost", port="5432")
	cur = conn.cursor()
	
	cur.execute('SELECT word, count FROM tweetwordcount ORDER BY COUNT DESC LIMIT %s;', (limitNumber,))
	records = cur.fetchall()
	for record in records:
		print record[0] + ': ' + str(record[1])
	conn.commit()
	conn.close()

if len(sys.argv) < 2:
	print 'usage: topcounts num'
	exit()
limitString = sys.argv[1]
if not str.isdigit(limitString):
	print 'error:  limit count must be a number'
	exit()
ShowTopWordCounts(int(limitString))

