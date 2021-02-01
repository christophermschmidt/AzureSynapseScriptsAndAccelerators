USE [nycserverless]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO


--now, creating a regular old external table is boring! whatwe want to do is dynamically generate this based on an input parameter to handle the latest arriving data
CREATE OR ALTER PROCEDURE dbo.uspExternalTblCreate
(
	@sinkTableName NVARCHAR(500) 
	,@sinkDataSource NVARCHAR(100)
	,@sourceDataSource NVARCHAR(100)
	,@location NVARCHAR(100)
	,@sinkFileFormat NVARCHAR(100)
	,@inputTimeStamp NVARCHAR(25)
	,@sourceDataSourceFilePath NVARCHAR(500)
	,@sourceDataSourceFileNamePattern NVARCHAR(100)
)
AS


/*
Date: 2/1/2021
Author: Chris Schmidt
Desc: This procedure is designed to take a single file at a time with a list of inputs and dynamically create an external table in Synapse Serverless.

Parameters: 

@sinkTableName
DataType: nvarchar(500)
Description: the name of the sink (destination) table that the table will be created as. Will be joined with the NUMERIC only portion of the inputTimeStamp parameter to create the full table name
Example: yellow_vehicles_refined_cetas

@sinkDataSource
DataType: nvarchar(100)
Description: the name of the sink data source you wish to use. Should already be defined
Example: taxi_data_refined

@sourceDataSource
DataType: nvarchar(100)
Description: the name of the source data source where the files live that will be loaded/moved/read from. Should already be defined
Example: taxi_data_raw

@location
DataType: nvarchar(100)
Description: the folder path where the files live. Should INCLUDE the trailing slash: / It is the folder where you want the files to go TO
Example: nyctaxi/yellow/

@sinkFileFormat
DataType NVARCHAR(100)
Description: the file format previously defined which describes the external data being written
Example: ParquetFF

@inputTimeStamp
DataType: NVARCHAR(25)
Description: THe input time stamp for the files. Should be the lowest level of granularity for a specific file type and is what you plan on partitioning on. Whatever you decide, please take a look at https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/best-practices-sql-on-demand to ensure that your folder structure adheres to best practices
Example: 2019-01

@sourceDataSourceFilePath
DataType: nvarchar(500)
Description: the path to the folder where the data lives in the source. Can be nested as much as needed, be sure to include the trailing slash: /. It is the folder where the files are coming from
Example: s3.amazonaws.com/trip+data/

@sourceDataSourceFileNamePattern
DataType: nvarchar(100)
Description: the pattern of the name of the file (excluding the time stamp) where the files are coming from
Example: yellow_tripdata_
*/

BEGIN

--if you're data doesn't have headers, be sure to update it in the OPENROWSET function call to change it to false

	--declare some additional parameters to clean up the inputs for the dynamic sql statement
	DECLARE @fullsinkTableName NVARCHAR(525)
	DECLARE @fullSourceFilePath NVARCHAR(600)
	DECLARE @fulllocation NVARCHAR(125)
	DECLARE @cleanedTimeStamp NVARCHAR(25)

	--final sql variable.
	DECLARE @sql NVARCHAR(max)


	--combine the tableName and the input time stamps to create the new output file name
	SET @cleanedTimeStamp = REPLACE(@inputTimeStamp, SUBSTRING(@inputTimeStamp, PATINDEX('%[^0-9]%', @inputTimeStamp),1), '')
	--combine the sink table name and the new cleaned time stamp to create the external table in Synapse
	SET @fullsinkTableName = @sinkTableName + '_' + @cleanedTimeStamp
	--create the full source file path by combing the source file path, file name pattern, and the RAW input time stamp along with the file extension (hard coded to csv) to create the final file. for example: s3.amazonaws.com/trip+data/yellow_tripdata_2019-01.csv
	SET @fullSourceFilePath = @sourceDataSourceFilePath + @sourceDataSourceFileNamePattern + @inputTimeStamp + '.csv'
	--combine the location and the new cleaned TimeStamp parameter to create the folder path that will exist in the new sink container. for example: nyctaxi/yellow/201901/
	SET @fullLocation = @location + @cleanedTimeStamp + '/'

	--create the parameterized sql statement
	SET @sql = '
		IF EXISTS(SELECT 1 FROM sys.external_tables WHERE [name] = '''+ @fullsinkTableName +''')
			DROP EXTERNAL TABLE ' + QUOTENAME(@fullsinkTableName) + '


		CREATE EXTERNAL TABLE ' + QUOTENAME(@fullsinkTableName) + ' WITH 
		(DATA_SOURCE = ' + QUOTENAME(@sinkdataSource) + ', LOCATION = N''' + @fulllocation + ''',FILE_FORMAT = ' + QUOTENAME(@sinkFileFormat) + ')
		AS
		SELECT *
		FROM OPENROWSET(
			BULK ''' + @fullSourceFilePath + '''
			,DATA_SOURCE = ''' + @sourceDataSource + '''
			,HEADER_ROW = true
			,FORMAT = ''CSV''
			, PARSER_VERSION = ''2.0''
			) as [r]'
	
	--if you want to view the statement, use the print command to view it. Outside of the scope of this tutorial but possible would be to log this to a control table somewhere
	PRINT @sql

--execute the sql to create your external table!
	EXEC sp_executesql @sql

END;



----sample script execution

EXEC	[dbo].[uspExternalTblCreate]
		@sinkTableName = N'yellow_vehicles_refined_cetas',
		@sinkDataSource = N'taxi_data_refined',
		@sourceDataSource = N'taxi_data_raw',
		@location = N'nyctaxi/yellow/',
		@sinkFileFormat = N'ParquetFF',
		@inputTimeStamp = N'2019*',
		@sourceDataSourceFilePath = N's3.amazonaws.com/trip+data/',
		@sourceDataSourceFileNamePattern = N'yellow_tripdata_'
GO