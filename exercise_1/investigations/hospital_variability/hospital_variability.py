#
#	Exercise 1 - Question 3 - hospital variability
#	Aaron Garth Enright
#	W205 section 1 (Monday 4pm)
#
#	Environment:	 PYSPARK 1.5 (linux)
#
#
from pyspark import SparkContext
from pyspark import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

#
#	ExecSQL - convenience to display results from a single query
#
def ExecSQL(command):
	results = sqlContext.sql(command)
	results.show()
	return

#
#	Calculate the normalized scores for the condition measures
#
def NormalizeConditionScores():
	#
	#	Calculation:
	#		1)	Normalize the score against the min/max scores recorded in the metrics table
	#
	normalized_score_data = sqlContext.sql("""
		SELECT
			e.ProviderID,
			e.Condition,
			e.MeasureID,
			e.Score,
			e.Sample,
			e.Footnote,
			(coalesce(e.Score, 0.0) - m.MinScore) / (m.MaxScore - m.MinScore) as NormalizedScore
		FROM
			effective_care e
			JOIN measures m on m.MeasureID = e.MeasureID
	""")
	normalized_score_data.registerTempTable('condition_score_tmp')
	return
	
#
#	Calculate Mean and STDEV for the normalized effective_care measures
#
def CalculateMuSigma():
	#
	#	Calculation:
	#		1)	Calculate the mean and standard deviation of all normalized effective care measures
	#
	normalized_measures = sqlContext.sql("""
		SELECT
			m.MeasureID,
			m.Name,
			m.StartQuarter,
			m.StartDate,
			m.EndQuarter,
			m.EndDate,
			m.MinScore,
			m.MaxScore,
			AVG(NormalizedScore) as ScoreAverage,
			STDDEV(NormalizedScore) as ScoreStandardDeviation
		FROM
			condition_score_tmp e
			JOIN measures m on m.MeasureID = e.MeasureID
		GROUP BY
			m.MeasureID,
			m.Name,
			m.StartQuarter,
			m.StartDate,
			m.EndQuarter,
			m.EndDate,
			m.MinScore,
			m.MaxScore
	""")
	normalized_measures.registerTempTable('normalized_measures_tmp')
	return
	
#
#	Calculate how many STDEV away from the mean each effective care measure is
#
def FindStdDevAway():
	#
	#	Calculation:
	#		1)	Calculate how many standard deviations away from the mean a measure is
	#
	normalized_score_data = sqlContext.sql("""
		SELECT
			e.*,
			ABS(NormalizedScore - m.ScoreAverage) / m.ScoreStandardDeviation as StdDevAway
		FROM
			condition_score_tmp e
			JOIN normalized_measures_tmp m on m.MeasureID = e.MeasureID
	""")
	normalized_score_data.registerTempTable('condition_stddev_tmp')
	return
	
#
#	Show the measures with the greatest variability
#
def ShowVariability():
	#
	#	Calculation:
	#		1)	Find the measures that have the largest average standard deviation - those are the ones with the greatest variability
	#
	NormalizeConditionScores()
	CalculateMuSigma()
	FindStdDevAway()
	normalized_score_data = sqlContext.sql("""
		SELECT
			m.Name,
			m.ScoreAverage,
			m.ScoreStandardDeviation,
			AVG(e.StdDevAway) as Variability
		FROM
			normalized_measures_tmp m
			JOIN condition_stddev_tmp e on e.MeasureID = m.MeasureID
		WHERE
			m.MinScore IS NOT NULL
			AND m.MaxScore IS NOT NULL
			AND m.ScoreAverage IS NOT NULL
			AND m.ScoreStandardDeviation IS NOT NULL
		GROUP BY
			m.Name,
			m.ScoreAverage,
			m.ScoreStandardDeviation
		ORDER BY
			Variability DESC
	""")
	normalized_score_data.show(100, False)
	return

#
#	Main body
#

#	Set up spark environment, requesting a little more memory than the default
SparkContext.setSystemProperty('spark.executor.memory', '4g')
sc = SparkContext("local", "transformer app")
sqlContext = HiveContext(sc)

ShowVariability()
