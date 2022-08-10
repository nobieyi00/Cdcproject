This project is to provide junior data engineers a hands on experience to learn basic data engineering principles.

Context

Imagine, you are working for a Health system, where they want to process data from a source Database like Oracle Database server however, you are not permitted to access the underlying 
table directly. You are expected to ingest the data from an output of a real time Change Data Capture Tool like Qlik Replicate. The business request here it to process
the data outputed in a data lake in near real time. Performing both full and incremental load

Prerequiste tools needed

1. Azure Data lake Gen 2
2. Data factory
3. Azure Synapase SQL pool


The goal is to build an ETL pipeline that can process both full and incremental load from the Data lake folder source table and load to the target SQL table


Project assignment
1. Build an ETL pipeline using Data factory that reads first the full load loads it into the Target SQL db table
2. Read the incremental file and process and load into the Target SQL table.
3. Final Target table should look like the below

![image](https://user-images.githubusercontent.com/20451211/175101501-12abfbd2-34f1-4872-b5b1-be5e5418a2c4.png)

![image](https://user-images.githubusercontent.com/20451211/183940348-d5762b6b-b45d-446e-9357-dd7527b7f78d.png)

