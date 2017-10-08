--
--	Create hospitals table
--
DROP TABLE hospitals;
CREATE EXTERNAL TABLE hospitals
(
	ProviderID		string,
	HospitalName		string,
	Address			string,
	City			string,
	State			string,
	ZIPCode			string,
	CountyName		string,
	PhoneNumber		string,
	HospitalType		string,
	HospitalOwnership	string,
	EmergencyServices	string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
	"separatorChar" = ",",
	"quoteChar" = '"',
	"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/hospitals';

--
--	Create effective_care table
--
DROP TABLE effective_care;
CREATE EXTERNAL TABLE effective_care
(
	ProviderID		string,
	HospitalName		string,
	Address			string,
	City			string,
	State			string,
	ZIPCode			string,
	CountyName		string,
	PhoneNumber		string,
	Condition		string,
	MeasureID		string,
	MeasureName		string,
	Score			string,
	Sample			string,
	Footnote		string,
	MeasureStartDate	string,
	MeasureEndDate		string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
        "separatorChar" = ",",
        "quoteChar" = '"',
        "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/effective_care';

--
--	Create readmissions table
--
DROP TABLE readmissions;
CREATE EXTERNAL TABLE readmissions
(
	ProviderID		string,
	HospitalName		string,
	Address			string,
	City			string,
	State			string,
	ZIPCode			string,
	CountyName		string,
	PhoneNumber		string,
	MeasureName		string,
	MeasureID		string,
	ComparedtoNational	string,
	Denominator		string,
	Score			string,
	LowerEstimate		string,
	HigherEstimate		string,
	Footnote		string,
	MeasureStartDate	string,
	MeasureEndDate		string

)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
        "separatorChar" = ",",
        "quoteChar" = '"',
        "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/readmissions';

--
--	Create measures table
--
DROP TABLE measures;
CREATE EXTERNAL TABLE measures
(
	Measure_Name		string,
	Measure_ID		string,
	MeasureStartQuarter	string,
	MeasureStartDate	string,
	MeasureEndQuarter	string,
	MeasureEndDate		string

)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
        "separatorChar" = ",",
        "quoteChar" = '"',
        "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/Measures';

--
--	Create surveys_responses table
--
DROP TABLE surveys_responses;
CREATE EXTERNAL TABLE surveys_responses
(
	ProviderNumber							string,
	HospitalName							string,
	Address								string,
	City								string,
	State								string,
	ZIPCode								string,
	CountyName							string,
	CommunicationwithNursesAchievementPoints			string,
	CommunicationwithNursesImprovementPoints			string,
	CommunicationwithNursesDimensionScore				string,
	CommunicationwithDoctorsAchievementPoints			string,
	CommunicationwithDoctorsImprovementPoints			string,
	CommunicationwithDoctorsDimensionScore				string,
	ResponsivenessofHospitalStaffAchievementPoints			string,
	ResponsivenessofHospitalStaffImprovementPoints			string,
	ResponsivenessofHospitalStaffDimensionScore			string,
	PainManagementAchievementPoints					string,
	PainManagementImprovementPoints					string,
	PainManagementDimensionScore					string,
	CommunicationaboutMedicinesAchievementPoints			string,
	CommunicationaboutMedicinesImprovementPoints			string,
	CommunicationaboutMedicinesDimensionScore			string,
	CleanlinessandQuietnessofHospitalEnvironmentAchievementPoints	string,
	CleanlinessandQuietnessofHospitalEnvironmentImprovementPoints	string,
	CleanlinessandQuietnessofHospitalEnvironmentDimensionScore	string,
	DischargeInformationAchievementPoints				string,
	DischargeInformationImprovementPoints				string,
	DischargeInformationDimensionScore				string,
	OverallRatingofHospitalAchievementPoints			string,
	OverallRatingofHospitalImprovementPoints			string,
	OverallRatingofHospitalDimensionScore				string,
	HCAHPSBaseScore							string,
	HCAHPSConsistencyScore						string

)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
        "separatorChar" = ",",
        "quoteChar" = '"',
        "escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/surveys_responses';

