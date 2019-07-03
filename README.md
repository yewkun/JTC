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
## Part 2: Database queries
