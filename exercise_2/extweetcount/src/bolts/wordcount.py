#
#	Aaron Enright
#	Exercise 2
#
#	Twitter word counter - added psycopg2 code to write to database.
#
from __future__ import absolute_import, print_function, unicode_literals
import psycopg2

from collections import Counter
from streamparse.bolt import Bolt



class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()

    def process(self, tup):
        word = tup.values[0]

        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount
        # Table name: Tweetwordcount
        # you need to create both the database and the table in advance.

        conn = psycopg2.connect(database="tcount", user="w205", password="", host="localhost", port="5432")
        cur = conn.cursor()
	#
	#	Algorithm:
	#		1) Lock the table.  Because we nay need to insert or update, we need
	#		   atomicity across the two statements.  Otherwise, one thread might try
	#		   and update the table while another sets ie back to zero.
	#		2) Update the wordcount for the selected word.  If the word does not exist
	#		   in the table, this becomes a NOP (well, essentially - we still need to
	#		   read the table to find out).
	#		3) Do an existence check for the word.   It may seem counterintuitive to do
	#		   this AFTER the update, but we actually save a write by doing it this way.
	#		4) Commit the transaction and free the table lock.
	#
	#	Comment:
	#		The MERGE keyword does not seem to exist in this version of Postgres.
	#		It can simplify the statement, but some on-line commentators also seem to
	#		think that the concurrency semantics around MERGE are not well defined.
	#
        cur.execute("""
			LOCK TABLE tweetwordcount IN EXCLUSIVE MODE;

			UPDATE tweetwordcount SET
				count = count + 1
			WHERE
				word = %s;

			INSERT INTO tweetwordcount (word, count)
				SELECT %s, 1
			WHERE
				NOT EXISTS (SELECT 1 FROM tweetwordcount WHERE word = %s);

			COMMIT;
        """, (word, word, word));
        conn.commit()
        conn.close()

        # Increment the local count
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))

