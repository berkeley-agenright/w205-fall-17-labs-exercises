# MIDS W205 : Exercise 1
Aaron Garth Enright
W205 Section 1 (Monday 4pm)

## Introduction

Exercise 1 take a large CDC dataset and loads it into Hadoop to answer some questions about the data.  The dataset is related to hospital quality care and was taken in 2015.  The questions that we
were asked were:

	*	What hospitals are models of high-quality care? That is, which hospitals have the most consistently high scores for a variety of procedures.
	*	What states are models of high-quality care?
	*	Which procedures have the greatest variability between hospitals?
	*	Are average scores for hospital quality or procedural variability correlated with patient survey responses?

## Platform

We performed this exercise using Hadoop 2.6 and Apache Spark 1.5 on linux (Centos 6.6).  The VM configuration was based on the UCB W205 Spring 2016 image.  Our particular instance was configured as:

|Family         |Type    |VCPUs|Memory (GiB)|Instance Storage|EBS Optimized Available|Network Performance|IPv6 Support|
-----------------------------------------------------------------------------------------------------------------------
|General Purpose|m1.large|2    |7.5         |2x420           |Yes                    |Moderate           |No          |
-----------------------------------------------------------------------------------------------------------------------

## Methodology

### Week 1

On this week, we loaded the CSV files from the CDC directly into Hadoop for general purpose viewing and data exploration.  It was here that we identified the following issues that would need to be
corrected/transformed in the data:

	*	Several tables had "pseudo" foreign keys into other tables.  What we mean here is that they had fields labelled with the same name as another table, but no entry in that other table.  In all
cases, we were able to locate the correct data using denormalized fields in each CSV.
	*	Many data were of a type not useful for comparison/manipulation.  For example, survey responses were noted as "0 out of 9" for example.
	*	Data was highly denormalized.
	
So, exploring this data and looking at the questions asked, we derived the following ERD:

[HospitalComparison](loading_and_modelling/HospitalCompare.png)

Our loading and modelling files are included in the sub-folder [loading_and_modelling](loading_and_modelling)

### Week 2

On this week, we transformed the data using pyspark.  Our transformations included the following:

	*	Normalizing the data.  Much of the data was denormalized in the original files, adding confusion and to possible errors.  We determined the correct table for each field and ensured that there
was only one copy of each one.
	*	Fixing foreign key references.  Especially important with the step above was ensuring that all foreign key references were correct.  Our findings from Week 1 (above) was that the measures table -
the table that included the metrics being used to compare tables had inconsistent capitlization with the effective_care and readmissions tables.  The effective_care table also referenced a measure called
IMM_3_FAC_ADHPCT which occurred nowhere in the measures table;  we found that there was a key called IMM_3 which had no reference in the effective_care table, and seemed to correspond data-wise, so we
changed all references in effective_care to IMM_3.  There was also a ProviderID in the surveys_responses table that did not exist in the hospitals table;  however, we matched HopitalName, Address, City, State,
ZipCode, and CountyName in the denormalized data back to another ProviderID and substituted it.
	*	Converting to native types.  There were several cases of data that was obviously intended to be a type other than string.  This included dates in the measures table, a flag to indicate whether a
hospital had emergency services, Scores and sample-sizes in the effective_care and readmissions tables, and HCAHPS scores in the surveys_responses table.  In the cases where data was marked "Not Available",
we generally inserted null into the final tables.
	*	Tranforming the data to enable comparisons.  Here we needed to convert values such as "0 out of 9" into usable numerics for comparison.  This required a complex UDF, which exhausted our system
resources.  The only workaround we found was to break the surveys_results table into three temporary tables and tranform each in turn then re-join the table.  Within the effective care table were also
Scores that were both categorical and numeric, so we changed all scores to numerics for final processing.
	*	Creating ranges on the measures.  Many of the measures had different ranges in the effective_care and readmissions tables, so in hopes fo being able to normalize these data, we enriched the
metrics table with a MinScore and MaxScore for each category that appeared in one of the other tables.

Our loading and modelling file is in the sub-folder [transforming](transforming)
