#
#	Exercise 1 - data transforms in pyspark
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
#	Load an individual temp table from a CSV file
#
def CreateTempTableFromCSV(tableName, schemaString):
	#
	#	Algortihm:
	#		1) load the file from the hdfs store (tables are named after their CSV filename ala Hadoop)
	#		2) split the lines;  delimeters are ",";  however, this leaves one double-quote at the beginning and end of the line, so we ignore the first and last character on the line
	#		3) create a nullable string field for each entry in the schema and add it to the resulting temp table schema
	#		4) create a data frame from the raw data and the simple string schema, then register it as a temp table
	#
	fileName = 'hdfs:///user/w205/hospital_compare/' + tableName + '/' + tableName + '.csv'
	lines = sc.textFile(fileName)
	parts = lines.map(lambda l: l[1:-1].split('\",\"'))
	tableData = parts.map(lambda p: tuple(p))
	fields = [StructField(fieldName, StringType(), True) for fieldName in schemaString.split()]
	schema = StructType(fields)
	schemaWebData = sqlContext.createDataFrame(tableData, schema)
	schemaWebData.registerTempTable(tableName + '_tmp')
	return

#
#	Load the CSV files that we want to transform
#
def LoadAllCSVs():
	print 'Loading CSV files'
	CreateTempTableFromCSV('hospitals', 'ProviderID HospitalName Address City State ZIPCode CountyName PhoneNumber HospitalType HospitalOwnership EmergencyServices')
	CreateTempTableFromCSV('effective_care', 'ProviderID HospitalName Address City State ZIPCode CountyName PhoneNumber Condition MeasureID MeasureName Score Sample Footnote MeasureStartDate MeasureEndDate')
	CreateTempTableFromCSV('readmissions', 'ProviderID HospitalName Address City State ZIPCode CountyName PhoneNumber MeasureName MeasureID ComparedToNational Denominator Score LowerEstimate HigherEstimate Footnote MeasureStartDate MeasureEndDate')
	CreateTempTableFromCSV('Measures', 'MeasureName MeasureID MeasureStartQuarter MeasureStartDate MeasureEndQuarter MeasureEndDate')
	CreateTempTableFromCSV('surveys_responses', 'ProviderID HospitalName Address City State ZIPCode CountyName CommunicationWithNursesAchievementPoints CommunicationWithNursesImprovementPoints CommunicationWithNursesDimensionScore CommunicationWithDoctorsAchievementPoints CommunicationWithDoctorsImprovementPoints CommunicationWithDoctorsDimensionScore ResponsivenessOfHospitalStaffAchievementPoints ResponsivenessOfHospitalStaffImprovementPoints ResponsivenessOfHospitalStaffDimensionScore PainManagementAchievementPoints PainManagementImprovementPoints PainManagementDimensionScore CommunicationAboutMedicinesAchievementPoints CommunicationAboutMedicinesImprovementPoints CommunicationAboutMedicinesDimensionScore CleanlinessandQuietnessOfHospitalEnvironmentAchievementPoints CleanlinessandQuietnessOfHospitalEnvironmentImprovementPoints CleanlinessandQuietnessOfHospitalEnvironmentDimensionScore DischargeInformationAchievementPoints DischargeInformationImprovementPoints DischargeInformationDimensionScore OverallRatingOfHospitalAchievementPoints OverallRatingOfHospitalImprovementPoints OverallRatingOfHospitalDimensionScore HCAHPSBaseScore HCAHPSConsistencyScore')
	return

#
#	To re-run this script, we need to drop existing tables if they already exist
#
def DropExistingTables():
	print 'Dropping any existing tables'
	sqlContext.sql("DROP TABLE IF EXISTS hospitals");
	sqlContext.sql("DROP TABLE IF EXISTS effective_care");
	sqlContext.sql("DROP TABLE IF EXISTS readmissions");
	sqlContext.sql("DROP TABLE IF EXISTS Measures");
	sqlContext.sql("DROP TABLE IF EXISTS surveys_responses");
	return;

#
#	Normalize hospitals - this is a base table, so there's not much to be done;  only conversion is the emergency services field to a boolean
#
def TransformHospitals():
	hospitals_revised = sqlContext.sql("""
		SELECT
			ProviderID,
			HospitalName,
			Address,
			City,
			State,
			ZIPCode,
			CountyName,
			PhoneNumber,
			HospitalType,
			HospitalOwnership,
			case when EmergencyServices = 'Yes' then 1 else 0 end as HasEmergencyServices
		FROM
			hospitals_tmp
	""")
	hospitals_revised = hospitals_revised.withColumn('HasEmergencyServices', (when(hospitals_revised.HasEmergencyServices == 'Yes', True).otherwise(False)).cast(BooleanType()))
	hospitals_revised.saveAsTable("hospitals")
	return

#
#	Normalize Effective Care table - use uppercase measure ID and change the one bad foreign key to match the measures table (this was found by hand-searching the data)
#
def TransformEffectiveCare():
	#
	#	Transforms:
	#		1) From our initial table analysis, we found that the key for measures table called "IMM3" is given as "IMM_3_FAC_ADHPCT" in this table;  also there is mixed casing.  Because this is
	#		   is a foreign key back to Measures, we transform is using a UDF.
	#		2) We transform all the nuanced Score values to something we can use in comparisons and calculations.  This includes:
	#			a. store "Not Available" as null in the table
	#			b. convert "Low", "Medium", "High" "Very High" (for the emergency department volume metric) to a numeric
	#			c. otherwise, we'll assume it's castable to a double
	#		3) Transform sample-size to a double, again interpreting "Not Available" as null
	#
	effective_care_revised = sqlContext.sql("""
		SELECT
			ProviderID,
			Condition,
			MeasureID,
			Score,
			Sample,
			Footnote
		FROM
			effective_care_tmp
	""")
	measure_id_udf = udf(lambda orig_id: 'IMM_3' if orig_id == 'IMM_3_FAC_ADHPCT' else orig_id.upper(), StringType())
	effective_care_revised = effective_care_revised.withColumn('MeasureID', measure_id_udf(effective_care_revised.MeasureID))
	effective_care_revised = effective_care_revised.withColumn('Score', (
		when(effective_care_revised.Score == 'Not Available', lit(None)).otherwise(
		when(effective_care_revised.Score == 'Low (0 - 19,999 patients annually)', '1.0').otherwise(
		when(effective_care_revised.Score == 'Medium (20,000 - 39,999 patients annually)', '2.0').otherwise(
		when(effective_care_revised.Score == 'High (40,000 - 59,999 patients annually)', '3.0').otherwise(
		when(effective_care_revised.Score == 'Very High (60,000+ patients annually)', '4.0').otherwise(
		effective_care_revised.Score)))))).cast(DoubleType()))
	effective_care_revised = effective_care_revised.withColumn('Sample', (when(effective_care_revised.Sample == 'Not Available', lit(None)).otherwise(effective_care_revised.Sample)).cast(DoubleType()))
	effective_care_revised.saveAsTable('effective_care')
	return

#
#	Normalize Readmissions table - no bad keys detected in the previous queries, so simply remove denormalized data from the hospitals and measures tables, and change data types
#
def TransformReadmissions():
	#
	#	Transforms:
	#		1) Transform Denominator (sample-size), Score, LowerEstimate, and HigherEstimate to doubles, interpreting "Not Available" as null
	#
	readmissions_revised = sqlContext.sql("""
		SELECT
			ProviderID,
			UPPER(MeasureID) as MeasureID,
			ComparedToNational,
			Denominator,
			Score,
			LowerEstimate,
			HigherEstimate,
			Footnote
		FROM
			readmissions_tmp
	""")
	readmissions_revised = readmissions_revised.withColumn('Denominator', (when(readmissions_revised.Denominator == 'Not Available', lit(None)).otherwise(readmissions_revised.Denominator)).cast(IntegerType()))
	readmissions_revised = readmissions_revised.withColumn('Score', (when(readmissions_revised.Score == 'Not Available', lit(None)).otherwise(readmissions_revised.Score)).cast(DoubleType()))
	readmissions_revised = readmissions_revised.withColumn('LowerEstimate', (when(readmissions_revised.LowerEstimate == 'Not Available', lit(None)).otherwise(readmissions_revised.LowerEstimate)).cast(DoubleType()))
	readmissions_revised = readmissions_revised.withColumn('HigherEstimate', (when(readmissions_revised.HigherEstimate == 'Not Available', lit(None)).otherwise(readmissions_revised.HigherEstimate)).cast(DoubleType()))
	readmissions_revised = readmissions_revised.withColumn('ComparedToNationalIndicator', (
			when(readmissions_revised.ComparedToNational == 'Not Available', lit(None)).otherwise(
			when(readmissions_revised.ComparedToNational == 'Number of Cases Too Small', lit(None)).otherwise(
			when(readmissions_revised.ComparedToNational == 'Better than the National Rate', lit(1.0)).otherwise(
			when(readmissions_revised.ComparedToNational == 'Worse than the National Rate', lit(-1.0)).otherwise(lit(0.0)))))).cast(DoubleType()))
	readmissions_revised.saveAsTable('readmissions')
	return
	
#
#	Normalize Measures table - ensure that primary key is uppercase, calculate the minimum and maximum known scores from the sample, and change dates from strings to dates
#		*** NOTE this function uses the transformed effective_care and readmissions tables
#
def TransformMeasures():
	#
	#	Transforms:
	#		1) Find the minimum and maximum Scores in the effective_care and readmissions tables (we know that these two tables are mutually exclusive - no duplicate MeasureID columns from manual
	#		   inspection) and union the results together (using distinct to eliminate duplicates)
	#		2) Add the aggregated MinScore and MaxScore values to the raw CSV data
	#		3) Convert the Date columns from strings to a date using a UDF
	#
	min_maxes = sqlContext.sql("""
		SELECT
			UPPER(MeasureID) as MeasureID,
			min(coalesce(Score, 0.0)) as MinScore,
			max(coalesce(Score, 0.0)) as MaxScore
		FROM
			effective_care
		GROUP BY
			MeasureID
		UNION
		SELECT
			UPPER(MeasureID) as MeasureID,
			min(coalesce(Score, 0.0)) as MinScore,
			max(coalesce(Score, 0.0)) as MaxScore
		FROM
			readmissions
		GROUP BY
			MeasureID
	""")
	min_maxes.registerTempTable('min_maxes')
	date_convert_udf = udf(lambda str: datetime.strptime(str, '%Y-%m-%d %H:%M:%S').date(), DateType())
	measures_revised = sqlContext.sql("""
		SELECT
			UPPER(m.MeasureID) as MeasureID,
			m.MeasureName as Name,
			m.MeasureStartQuarter as StartQuarter,
			m.MeasureStartDate as StartDate,
			m.MeasureEndQuarter as EndQuarter,
			m.MeasureEndDate as EndDate,
			x.MinScore as MinScore,
			x.MaxScore as MaxScore
		FROM
			Measures_tmp m 
			LEFT OUTER JOIN min_maxes x on UPPER(x.MeasureID) = UPPER(m.MeasureID)
	""")
	measures_revised = measures_revised.withColumn('StartDate', date_convert_udf(measures_revised.StartDate))
	measures_revised = measures_revised.withColumn('EndDate', date_convert_udf(measures_revised.EndDate))
	measures_revised.saveAsTable("measures")
	return

#
#	Normalize surveys_responses - one bad foreign key back to the hospitals was detected (through inspection), by using the denormalized name, address, etc fields, we were able to map it from 330249 to 331316
#
def TransformSurveysResponses():
	#
	#	Transforms:
	#		NOTE:  On the target architecture, this transform exhausted the Java thread pool, even with system/environment tweaking.  To fix this, we break the transform into three parts
	#		   (stored in temp tables) and join them back together at the end
	#		1) Create a UDF to turn columns of the form "x out of y" into a numeric value.  So, for example, the string "4 out of 10" turns into a double of 0.4.  To do this:
	#			a. split the string on blanks
	#			b. ensure that we get four values
	#			c. check that the first and fourth values are numeric and the fourth is not 0
	#			d. if the above conditions are met, convert the first value to a double, then divide it by the fourth value , also converted to a double, otherwise return 0.0
	#		2) Create a temp table with all the achievement point scores, converting all the values
	#		3) Create a temp table with all the improvement point scores, converting all the values
	#		4) Create a temp table with all the dimension scores, converting all the values
	#		5) Join the temp tables back to the data frame
	#		6) Convert the HCAHPS scores to doubles
	#
	convert_response_udf = udf(lambda str: (float(str.split()[0]) / float(str.split()[3])) if len(str.split()) == 4 and str.split()[0].isnumeric() and str.split()[3].isnumeric() and float(str.split()[3]) <> 0.0 else 0.0, DoubleType())
	achievement_points = sqlContext.sql("""
		SELECT
			ProviderID,
			CommunicationWithNursesAchievementPoints,
			CommunicationWithDoctorsAchievementPoints,
			ResponsivenessOfHospitalStaffAchievementPoints,
			PainManagementAchievementPoints,
			CommunicationAboutMedicinesAchievementPoints,
			CleanlinessandQuietnessOfHospitalEnvironmentAchievementPoints,
			DischargeInformationAchievementPoints,
			OverallRatingOfHospitalAchievementPoints
		FROM
			surveys_responses_tmp
	""")
	achievement_points = achievement_points.withColumn('CommunicationWithNursesAchievementPoints', convert_response_udf(achievement_points.CommunicationWithNursesAchievementPoints))
	achievement_points = achievement_points.withColumn('CommunicationWithDoctorsAchievementPoints', convert_response_udf(achievement_points.CommunicationWithDoctorsAchievementPoints))
	achievement_points = achievement_points.withColumn('ResponsivenessOfHospitalStaffAchievementPoints', convert_response_udf(achievement_points.ResponsivenessOfHospitalStaffAchievementPoints))
	achievement_points = achievement_points.withColumn('PainManagementAchievementPoints', convert_response_udf(achievement_points.PainManagementAchievementPoints))
	achievement_points = achievement_points.withColumn('CommunicationAboutMedicinesAchievementPoints', convert_response_udf(achievement_points.CommunicationAboutMedicinesAchievementPoints))
	achievement_points = achievement_points.withColumn('CleanlinessandQuietnessOfHospitalEnvironmentAchievementPoints', convert_response_udf(achievement_points.CleanlinessandQuietnessOfHospitalEnvironmentAchievementPoints))
	achievement_points = achievement_points.withColumn('DischargeInformationAchievementPoints', convert_response_udf(achievement_points.DischargeInformationAchievementPoints))
	achievement_points = achievement_points.withColumn('OverallRatingOfHospitalAchievementPoints', convert_response_udf(achievement_points.OverallRatingOfHospitalAchievementPoints))
	achievement_points.registerTempTable('achievement_points')

	improvement_points = sqlContext.sql("""
		SELECT
			ProviderID,
			CommunicationWithNursesImprovementPoints,
			CommunicationWithDoctorsImprovementPoints,
			ResponsivenessOfHospitalStaffImprovementPoints,
			PainManagementImprovementPoints,
			CommunicationAboutMedicinesImprovementPoints,
			CleanlinessandQuietnessOfHospitalEnvironmentImprovementPoints,
			DischargeInformationImprovementPoints,
			OverallRatingOfHospitalImprovementPoints
		FROM
			surveys_responses_tmp
	""")
	improvement_points = improvement_points.withColumn('CommunicationWithNursesImprovementPoints', convert_response_udf(improvement_points.CommunicationWithNursesImprovementPoints))
	improvement_points = improvement_points.withColumn('CommunicationWithDoctorsImprovementPoints', convert_response_udf(improvement_points.CommunicationWithDoctorsImprovementPoints))
	improvement_points = improvement_points.withColumn('ResponsivenessOfHospitalStaffImprovementPoints', convert_response_udf(improvement_points.ResponsivenessOfHospitalStaffImprovementPoints))
	improvement_points = improvement_points.withColumn('PainManagementImprovementPoints', convert_response_udf(improvement_points.PainManagementImprovementPoints))
	improvement_points = improvement_points.withColumn('CommunicationAboutMedicinesImprovementPoints', convert_response_udf(improvement_points.CommunicationAboutMedicinesImprovementPoints))
	improvement_points = improvement_points.withColumn('CleanlinessandQuietnessOfHospitalEnvironmentImprovementPoints', convert_response_udf(improvement_points.CleanlinessandQuietnessOfHospitalEnvironmentImprovementPoints))
	improvement_points = improvement_points.withColumn('DischargeInformationImprovementPoints', convert_response_udf(improvement_points.DischargeInformationImprovementPoints))
	improvement_points = improvement_points.withColumn('OverallRatingOfHospitalImprovementPoints', convert_response_udf(improvement_points.OverallRatingOfHospitalImprovementPoints))
	improvement_points.registerTempTable('improvement_points')

	dimension_score = sqlContext.sql("""
		SELECT
			ProviderID,
			CommunicationWithNursesDimensionScore,
			CommunicationWithDoctorsDimensionScore,
			ResponsivenessOfHospitalStaffDimensionScore,
			PainManagementDimensionScore,
			CommunicationAboutMedicinesDimensionScore,
			CleanlinessandQuietnessOfHospitalEnvironmentDimensionScore,
			DischargeInformationDimensionScore,
			OverallRatingOfHospitalDimensionScore
		FROM
			surveys_responses_tmp
	""")
	dimension_score = dimension_score.withColumn('CommunicationWithNursesDimensionScore', convert_response_udf(dimension_score.CommunicationWithNursesDimensionScore))
	dimension_score = dimension_score.withColumn('CommunicationWithDoctorsDimensionScore', convert_response_udf(dimension_score.CommunicationWithDoctorsDimensionScore))
	dimension_score = dimension_score.withColumn('ResponsivenessOfHospitalStaffDimensionScore', convert_response_udf(dimension_score.ResponsivenessOfHospitalStaffDimensionScore))
	dimension_score = dimension_score.withColumn('PainManagementDimensionScore', convert_response_udf(dimension_score.PainManagementDimensionScore))
	dimension_score = dimension_score.withColumn('CommunicationAboutMedicinesDimensionScore', convert_response_udf(dimension_score.CommunicationAboutMedicinesDimensionScore))
	dimension_score = dimension_score.withColumn('CleanlinessandQuietnessOfHospitalEnvironmentDimensionScore', convert_response_udf(dimension_score.CleanlinessandQuietnessOfHospitalEnvironmentDimensionScore))
	dimension_score = dimension_score.withColumn('DischargeInformationDimensionScore', convert_response_udf(dimension_score.DischargeInformationDimensionScore))
	dimension_score = dimension_score.withColumn('OverallRatingOfHospitalDimensionScore', convert_response_udf(dimension_score.OverallRatingOfHospitalDimensionScore))
	dimension_score.registerTempTable('dimension_score')

	surveys_responses_revised = sqlContext.sql("""
		SELECT
			CASE WHEN s.ProviderID = "330249" THEN "331316" ELSE s.ProviderID END as ProviderID,
			a.CommunicationWithNursesAchievementPoints,
			i.CommunicationWithNursesImprovementPoints,
			d.CommunicationWithNursesDimensionScore,
			a.CommunicationWithDoctorsAchievementPoints,
			i.CommunicationWithDoctorsImprovementPoints,
			d.CommunicationWithDoctorsDimensionScore,
			a.ResponsivenessOfHospitalStaffAchievementPoints,
			i.ResponsivenessOfHospitalStaffImprovementPoints,
			d.ResponsivenessOfHospitalStaffDimensionScore,
			a.PainManagementAchievementPoints,
			i.PainManagementImprovementPoints,
			d.PainManagementDimensionScore,
			a.CommunicationAboutMedicinesAchievementPoints,
			i.CommunicationAboutMedicinesImprovementPoints,
			d.CommunicationAboutMedicinesDimensionScore,
			a.CleanlinessandQuietnessOfHospitalEnvironmentAchievementPoints,
			i.CleanlinessandQuietnessOfHospitalEnvironmentImprovementPoints,
			d.CleanlinessandQuietnessOfHospitalEnvironmentDimensionScore,
			a.DischargeInformationAchievementPoints,
			i.DischargeInformationImprovementPoints,
			d.DischargeInformationDimensionScore,
			a.OverallRatingOfHospitalAchievementPoints,
			i.OverallRatingOfHospitalImprovementPoints,
			d.OverallRatingOfHospitalDimensionScore,
			s.HCAHPSBaseScore,
			s.HCAHPSConsistencyScore
		FROM
			surveys_responses_tmp s
			JOIN achievement_points a on a.ProviderID = s.ProviderID
			JOIN improvement_points i on i.ProviderID = s.ProviderID
			JOIN dimension_score d on d.ProviderID = s.ProviderID
	""")
	surveys_responses_revised = surveys_responses_revised.withColumn('HCAHPSBaseScore', (when(surveys_responses_revised.HCAHPSBaseScore == 'Not Available', lit(None)).otherwise(surveys_responses_revised.HCAHPSBaseScore)).cast(DoubleType()))
	surveys_responses_revised = surveys_responses_revised.withColumn('HCAHPSConsistencyScore', (when(surveys_responses_revised.HCAHPSConsistencyScore == 'Not Available', lit(None)).otherwise(surveys_responses_revised.HCAHPSConsistencyScore)).cast(DoubleType()))
	surveys_responses_revised.saveAsTable('surveys_responses')
	return

#
#	Check the row count of the original (temp) table against the transformed (permanent) table
#
def CheckRowCounts(tableName):
	orig_rowcount = sqlContext.sql('select "' + tableName + '" as TableName, COUNT(*) as RowCount_tmp from ' + tableName + '_tmp')
	tranform_rowcount = sqlContext.sql('select "' + tableName + '" as TableName, COUNT(*) as RowCount from ' + tableName)
	compare_rowcounts = orig_rowcount.join(tranform_rowcount, 'TableName')
	compare_rowcounts.show()
	return 

#
#	Check the row counts of all tables to ensure we didn't lose any rows in our transformations
#
def CheckAllRowCounts():
	CheckRowCounts('hospitals')
	CheckRowCounts('measures')
	CheckRowCounts('effective_care')
	CheckRowCounts('readmissions')
	CheckRowCounts('surveys_responses')
	return

#
#	In the design of our schema, we rely on foreign keys that we create from what we define as key fields in the original CSV. They are the MeasureID back to the measures table and ProviderID
#	back to the hospitals table.  Several of our transforms had logic to fix up any problems in these keys.  Now we check to make sure that nothing was missed.
#	
def CheckForeignKeys():
	print 'Checking foreign keys'
	ExecSQL("""
		SELECT DISTINCT
			m.MeasureID as Measure_MeasureID,
			e.MeasureID as effective_care_MeasureID
		FROM
			effective_care e
			LEFT OUTER JOIN measures m on m.MeasureID = e.MeasureID
		WHERE
			m.MeasureID IS NULL
	""")
	ExecSQL("""
		SELECT DISTINCT
			m.MeasureID as Measure_MeasureID,
			r.MeasureID as readmissions_MeasureID
		FROM
			readmissions r
			LEFT OUTER JOIN measures m on m.MeasureID = r.MeasureID
		WHERE
			m.MeasureID IS NULL
	""")
	ExecSQL("""
		SELECT DISTINCT
			h.ProviderID as hospitals_ProviderID,
			e.ProviderID as effective_care_ProviderID
		FROM
			effective_care e
			LEFT OUTER JOIN hospitals h on h.ProviderID = e.ProviderID
		WHERE
			h.ProviderID IS NULL
	""")
	ExecSQL("""
		SELECT DISTINCT
			h.ProviderID as hospitals_ProviderID,
			r.ProviderID as readmissions_ProviderID
		FROM
			readmissions r
			LEFT OUTER JOIN hospitals h on h.ProviderID = r.ProviderID
		WHERE
			h.ProviderID IS NULL
	""")
	ExecSQL("""
		SELECT DISTINCT
			h.ProviderID as hospitals_ProviderID,
			s.ProviderID as surveys_responses_ProviderID
		FROM
			surveys_responses s
			LEFT OUTER JOIN hospitals h on h.ProviderID = s.ProviderID
		WHERE
			h.ProviderID IS NULL
	""")
	return

#
#	Main body
#

#	Set up spark environment, requesting a little more memory than the default
SparkContext.setSystemProperty('spark.executor.memory', '4g')
sc = SparkContext("local", "transformer app")
sqlContext = HiveContext(sc)

# Create the baseline temp tables from CSV files
DropExistingTables()
LoadAllCSVs()

# Transform all the data - this is "hand logic" based on the unique requirements of each table
TransformHospitals()
TransformEffectiveCare()
TransformReadmissions()
TransformMeasures()
TransformSurveysResponses()

# Finally, perform some sanity checks on the data
CheckAllRowCounts()
CheckForeignKeys()

