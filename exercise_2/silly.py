#
#	Aaron Enright
#	Exercise 2
#
#	finalresults.py - print the final results of the twitterstream
#
import psycopg2

def ShowResultsForWordList():
	query = 'SELECT word, count FROM tweetwordcount ORDER BY word;'
	conn = psycopg2.connect(database="tcount", user="w205", password="", host="localhost", port="5432")
	cur = conn.cursor()
	
	cur.execute('''SELECT * FROM tweetwordcount;''')
	conn.commit()
	conn.close()

ShowResultsForWordList()
