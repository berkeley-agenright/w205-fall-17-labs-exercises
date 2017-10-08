!/bin/bash

# must be run as w205 - "su - w205" prior to running this script

unzip Hospital_Revised_Flatfiles.zip
for name in *\ *.csv; do tail -n +2 "$name" > "${name// /}"; mv -v "$name" "$name".orig; done

#
#rename the needed files
#
mv -v HospitalGeneralInformation.csv hospitals.csv
mv -v TimelyandEffectiveCare-Hospital.csv effective_care.csv
mv -v ReadmissionsandDeaths-Hospital.csv readmissions.csv
mv -v MeasureDates.csv Measures.csv
mv -v hvbp_hcahps_05_28_2015.csv surveys_responses.csv

#
# Create the hdfs folder and move files there
#

hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare/hospitals
hdfs dfs -mkdir /user/w205/hospital_compare/effective_care
hdfs dfs -mkdir /user/w205/hospital_compare/readmissions
hdfs dfs -mkdir /user/w205/hospital_compare/Measures
hdfs dfs -mkdir /user/w205/hospital_compare/surveys_responses
hdfs dfs -put hospitals.csv /user/w205/hospital_compare/hospitals
hdfs dfs -put effective_care.csv /user/w205/hospital_compare/effective_care
hdfs dfs -put readmissions.csv /user/w205/hospital_compare/readmissions
hdfs dfs -put Measures.csv /user/w205/hospital_compare/Measures
hdfs dfs -put surveys_responses.csv /user/w205/hospital_compare/surveys_responses
