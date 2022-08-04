MERGE dbo.TargetTable AS TARGET
USING (SELECT * FROM (
SELECT *, ROW_NUMBER() OVER (Partition by ID Order by MODIFIEDDATE desc ) as rn
FROM [dbo].[StageTable] 
) SUB
WHERE SUB.RN =1) AS SOURCE 
ON (TARGET.ID = SOURCE.ID) 
--When records are matched
WHEN MATCHED 
    THEN UPDATE SET TARGET.NAME = SOURCE.NAME, TARGET.WEIGHT = SOURCE.WEIGHT, TARGET.MODIFIEDDATE =  SOURCE.MODIFIEDDATE ,  
                    TARGET.TRANSACTIONTYPE =  SOURCE.TRANSACTIONTYPE
--When no records are matched, insert the incoming records from source table to target table
WHEN NOT MATCHED 
            THEN INSERT (ID, NAME, WEIGHT, MODIFIEDDATE,TRANSACTIONTYPE) 
            VALUES (SOURCE.ID, SOURCE.NAME, SOURCE.WEIGHT,SOURCE.MODIFIEDDATE, SOURCE.TRANSACTIONTYPE);
