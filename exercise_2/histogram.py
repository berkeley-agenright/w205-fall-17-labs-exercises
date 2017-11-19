#
#	Aaron Enright
#	Exercise 2
#
#	histogram.py - print the final word counts within a range
#
import sys
import psycopg2

def ShowBoundedWordCounts(lowerBound, upperBound):
	conn = psycopg2.connect(database="tcount", user="w205", password="", host="localhost", port="5432")
	cur = conn.cursor()
	
	cur.execute('SELECT word, count FROM tweetwordcount WHERE count BETWEEN %s AND %s ORDER BY COUNT DESC;', (lowerBound, upperBound))
	records = cur.fetchall()
	for record in records:
		print record[0] + ': ' + str(record[1])
	conn.commit()
	conn.close()

if len(sys.argv) < 3:
	print 'usage: histogram lower upper'
	exit()
lowerString = sys.argv[1]
upperString = sys.argv[2]
if not (str.isdigit(lowerString) and str.isdigit(upperString)):
	print 'error:  lower and upper bounds must be numbers'
	exit()
ShowBoundedWordCounts(int(lowerString), int(upperString))

