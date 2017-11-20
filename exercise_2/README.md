# MIDS W205 : Exercise 2
Aaron Garth Enright
W205 Section 1 (Monday 4pm)

## Introduction

Exercise 2 analyzes streaming tweet data using tweepy, Postgres, and Apache Storm.  We analyze the wordcounts coming from the running stream of tweets in the Twitter-verse, storing counts in Postgres
and analyzing them using various counts.

## Platform

We performed this exercise using Postgres 8.4.2 and Apache Storm on linux (Centos 6.6).  The VM configuration was based on the UCB W205 EX2-FULL image.  Our particular instance was configured as:

| Family          | Type     | VCPUs | Memory (GiB) | Instance Storage | EBS Optimized Available | Network Performance | IPv6 Support |
| --------------- | -------- | ----- | ------------ | ---------------- | ----------------------- | ------------------- | ------------ |
| General Purpose | m1.large | 2     | 15           | 2x420            | Yes                     | Moderate            | No           |

## Architecture

The full architecture is detailed here: [Architecture](Architecture.pdf) 

## Installation

Prior to running the application, it is necessary to configure Postgres and create a database called tcount with one table called tweetwordcount.  Steps within Postgres would be:

	CREATE DATABASE tcount;
	
	CREATE TABLE tweetwordcount (word TEXT PRIMARY KEY NOT NULL, count INT NOT NULL);
	
Once Postgres has been configured and the table installed, install tweepy and psycopg2 as follows:

	pip install tweepy
	pip install pycopg2
	
Now, clone this repository and do the following:

	cd w205-fall-17-labs-exercises/exercise_2/extweetcount
	run sparse
	
This will begin gathering tweet data.   Analysis can be done usiong the following tools (all found in the w205-fall-17-labs-exercises/exercise_2 folder):

	python finalresults.py [word1 word2 ...]		-- get the counts for words alphabetically;  if no word arguments are supplied, the counts for all words in the tweet-stream are returned
	python histogram.py lower upper					-- produce the word counts between lower and upper counts ordered by count
	python topcounts.py n							-- produce the top n word counts in descending order

	