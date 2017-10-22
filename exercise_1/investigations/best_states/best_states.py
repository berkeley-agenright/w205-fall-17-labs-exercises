#
#	Exercise 1 - Question 1 - best states
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

def GatherStates():
	states = sqlContext.sql("""
		SELECT DISTINCT
			State
		FROM
			hospitals
	""")
	states.registerTempTable('states_tmp')
#
#	Calculate the normalized average of all the condition scores for a particular provider
#
def CalculateConditionScores():
	#
	#	Calculation:
	#		1)	Normalize the score against the min/max scores recorded in the metrics table
	#		2)	Now that the scores are normalized, take the mean of all the scores together
	#
	normalized_score_data = sqlContext.sql("""
		SELECT
			h.State,
			AVG((coalesce(e.Score, 0.0) - m.MinScore) / (m.MaxScore - m.MinScore)) as ConditionScoreAverage
		FROM
			effective_care e
			JOIN measures m on m.MeasureID = e.MeasureID
			JOIN hospitals h on e.ProviderID = h.ProviderID
		GROUP BY
			h.State
	""")
	normalized_score_data.registerTempTable('condition_score_tmp')
	return
	
#
#	Calculate the readmission and death score
#
def CalculateReadmissionAndDeathScore():
	#
	#	Calculation:
	#		1)	Sum all the compared to national indicators (we get a +1 for better than national average, -1 for worse, 0 for anything else)
	#
	normalized_score_data = sqlContext.sql("""
		SELECT
			h.State,
			SUM(coalesce(r.ComparedToNationalIndicator, 0.0)) / COUNT(coalesce(r.ComparedToNationalIndicator, 0.0)) as ReAdmissionAndDeathScore
		FROM
			readmissions r
			JOIN hospitals h on r.ProviderID = h.ProviderID
		GROUP BY
			h.State
	""")
	normalized_score_data.registerTempTable('readmission_and_death_score_data_tmp')
	return
	
#
#	Calculate the final score for a provider
#
def CalculateFinalScores():
	#
	#	Calculation:
	#		1)	Weigh the conditions score by the readmission and death score
	#
	normalized_score_data = sqlContext.sql("""
		SELECT
			s.State,
			coalesce(r.ReAdmissionAndDeathScore, 0.0) as ReAdmissionAndDeathScore,
			coalesce(c.ConditionScoreAverage, 0.0) as ConditionScoreAverage
		FROM
			states_tmp s
			LEFT OUTER JOIN condition_score_tmp c on c.State = s.State
			LEFT OUTER JOIN readmission_and_death_score_data_tmp r on r.State = s.State
	""")
	normalized_score_data.registerTempTable('score_data_tmp')
	return

#
#	Print the results of our intermediate calculations to ensure nothing is out of line
#
def SanityCheckScores():
	normalized_score_data = sqlContext.sql("""
		SELECT
			MIN(ReAdmissionAndDeathScore) as MinReAdmissionAndDeathScore,
			MAX(ReAdmissionAndDeathScore) as MaxReAdmissionAndDeathScore,
			MIN(ConditionScoreAverage) as MinConditionScoreAverage,
			MAX(ConditionScoreAverage) as MaxConditionScoreAverage
		FROM
			score_data_tmp
	""")
	normalized_score_data.show()
	return
	

#
#	Print the top x states requested by the caller
#
def ShowTopStates(numberToShow):
	GatherStates()
	CalculateConditionScores()
	CalculateReadmissionAndDeathScore()
	CalculateFinalScores()
	SanityCheckScores()
	finalScores = sqlContext.sql("""
		SELECT
			st.State,
			s.ConditionScoreAverage,
			s.ReAdmissionAndDeathScore,
			(s.ConditionScoreAverage + s.ReAdmissionAndDeathScore) / 2 as FinalScore
		FROM
			score_data_tmp s
			JOIN states_tmp st on st.State = s.State
		ORDER BY
			FinalScore DESC
	""")
	finalScores.show(numberToShow, False)
	return

#
#	Main body
#

#	Set up spark environment, requesting a little more memory than the default
SparkContext.setSystemProperty('spark.executor.memory', '4g')
sc = SparkContext("local", "transformer app")
sqlContext = HiveContext(sc)

ShowTopStates(10)
