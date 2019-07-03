# JTC Data Engineering Assignment
### Software Setup
Ubuntu 16.04  
Microsoft SQL Server 2017 (Development License)  
Anaconda Python 3.7.3
## Part 1: Data transformation & database loading
1. Create db database:  
sqlcmd -S localhost -U SA -P 'passW@rd' -Q "create database db"  
2. Run DDL-DML.sql script:  
sqlcmd -S localhost -U SA -P 'passW@rd' -d db -i DDL-DML.sql  
3. Execute run.py in the working directory where the raw data directory is:  
python run.py  
### Results:
Total number of files processed = 15,770  
Total number of rows in FactStore table created = 1,547,897  
## Part 2: Database queries
### Part 2A:
### Query:
SELECT DimProducts.ProductCategoryName,AVG(FactStore.OnHandQty) AS OnHandQty FROM FactStore  
LEFT JOIN DimProducts ON FactStore.ProductID = DimProducts.ProductID  
GROUP BY ProductCategoryName;
### Results:
ProductCategoryName            OnHandQty  
------------------------------ -----------  
Audio                                   21  
Cameras and camcorders                  20  
Cell phones                             62  
Computers                               21  
Games and Toys                          75  
Home Appliances                         20  
Music, Movies and Audio Books           18  
TV and Video                            20  
### Part 2B:
### Query: 
