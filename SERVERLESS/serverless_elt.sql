----create a database if one does not already exist
--CREATE DATABASE [nycserverless]
--GO

-- create master key that will protect the credentials, if you haven't already done so
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<enter very strong password here>'

-- create credentials for containers in our demo storage account
CREATE DATABASE SCOPED CREDENTIAL [sqlondemand]
WITH IDENTITY='Managed Identity'  --alternatively you can use the Storage Account Key (SAS) instead.
GO


--if you don't already have any external data sources defined, create them! For this scenario, there are 2. The first external data source points to the raw storage container, and the 2nd assumes that data is to be moved to a refined container in either the same or different storage account. I split this out to look/feel more "real-world"
IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE [name] = 'taxi_data_raw') 
DROP EXTERNAL DATA SOURCE [taxi_data_raw]
GO

CREATE EXTERNAL DATA SOURCE [taxi_data_raw] 
WITH 
	(
		LOCATION = N'https://chrschmcompdeveastus2.dfs.core.windows.net/raw',
		CREDENTIAL = [sqlondemand]) --the credential you created earlier
GO

--create the 2nd external data source described above
IF EXISTS (SELECT 1 FROM sys.external_data_sources WHERE [name] = 'taxi_data_refined') 
DROP EXTERNAL DATA SOURCE [taxi_data_refined]
GO

CREATE EXTERNAL DATA SOURCE [taxi_data_refined] 
WITH 
	(
		LOCATION = N'https://chrschmcompdeveastus2.dfs.core.windows.net/refined',
		CREDENTIAL = [sqlondemand])
GO

--create the external file formats we need
IF EXISTS (SELECT 1 FROM sys.external_file_formats WHERE [name] = 'nyc_csv_file_format')
DROP EXTERNAL FILE FORMAT [nyc_csv_file_format]
GO


--the first external file format it for CSV files that match our NYC import pattern
CREATE EXTERNAL FILE FORMAT [nyc_csv_file_format] 
WITH (
	FORMAT_TYPE = DELIMITEDTEXT,
	FORMAT_OPTIONS (FIELD_TERMINATOR = N',', 
	STRING_DELIMITER = N'"', 
	USE_TYPE_DEFAULT = False)
	)
GO

--the second file format is the underlying storage mechanism for our sink tables. We'll be using Parquet here.
IF EXISTS (SELECT 1 FROM sys.external_file_formats WHERE [name] = 'ParquetFF')
DROP EXTERNAL FILE FORMAT [ParquetFF]
GO


CREATE EXTERNAL FILE FORMAT [ParquetFF] 
WITH (
	FORMAT_TYPE = PARQUET, 
	DATA_COMPRESSION = N'org.apache.hadoop.io.compress.SnappyCodec')
GO

--create the external table
CREATE EXTERNAL TABLE [yellow_vehicles_refined_cetas_201901] WITH 
	(DATA_SOURCE = [taxi_data_refined], LOCATION = N'nyctaxi/yellow/201901/',FILE_FORMAT = [ParquetFF])
AS
SELECT *
FROM OPENROWSET(
	BULK 's3.amazonaws.com/trip+data/yellow_tripdata_2019-01.csv'
	,DATA_SOURCE = 'taxi_data_raw'
	,HEADER_ROW = true
	,FORMAT = 'CSV'
	, PARSER_VERSION = '2.0'
	) as [r]


