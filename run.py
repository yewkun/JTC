# Filename: run.py
# Version: 1.0
# Description: [JTC Assignment] Transform GZIP text data to pipe delimited files to be stored in MS SQL Server
# Author: Alan Chee
# Date created: 3 Jul 2019

# Run Info: 
# Put & run script in the working directory where the raw data directory (src_base_dir) is found.
# The extracted data directory (tgt_base_dir) will be created in the same working directory.
# Data from PSV files will be stored in db table, which is created beforehand.	
 
import os
import glob
import gzip
import pandas as pd
import pyodbc

base_path = os.getcwd()
src_base_dir ='RawData'
tgt_base_dir = 'dataextract'

# Database password
# WARNING: To protect password, set and limit access right for working directory.
db_password = 'passW@rd'


nfile = 0


# Generate a pipe delimited file from a gzipped tab delimited raw data file 
def gen_psv(source, target):
	with gzip.open(source) as f:
		df = pd.read_csv(f, sep='\t', usecols=['Date', 'StoreID', 'ProductID', 'OnHandQuantity', 'OnOrderQuantity', 'DaysInStock', 'MinDayInStock', 'MaxDayInStock'])
		df.rename(columns={'Date':'DateID', 'OnHandQuantity':'OnHandQty', 'OnOrderQuantity':'OnOrderQty'}, inplace=True)
		target_name = source.split('.')[0]
		df.to_csv(target, sep='|', index=False)

def db_connect(pwd):
	con = pyodbc.connect('DSN=MySQLServer;UID=SA;PWD={}'.format(pwd))
	return con
	

def db_load_psv(con, source):
	full_path = os.path.join(base_path, source)
	sqlcmd = "SET DATEFORMAT YMD; BULK INSERT db.dbo.FactStore FROM '{}' WITH ( FIELDTERMINATOR='|', ROWTERMINATOR = '0x0A', FIRSTROW=2);".format(full_path)
	cursor = con.cursor()
	ret = cursor.execute(sqlcmd)
	ret.commit()

# Create target base dir (dataextract), if it doesn't exist
if not os.path.isdir(tgt_base_dir):
	os.mkdir(tgt_base_dir)

db_con = db_connect(db_password)
print('Database connection open. Starting process...')

try:
	# Find all gzip files under source dir & process 1 at a time
	for src in glob.glob(src_base_dir+'/**/*.gz', recursive=True):
		parts=src.split('/')
		tgt_path = tgt_base_dir
		fname = parts[-1]

		# Work thru the dir levels & create new dirs for non-existing
		for i in range(1, len(parts)-1):
			tgt_path = os.path.join(tgt_path, parts[i])
			if not os.path.isdir(tgt_path):
				os.mkdir(tgt_path)
		tgt = os.path.join(tgt_path, fname.split('.')[0]+'.psv')
		gen_psv(src, tgt)
		db_load_psv(db_con, tgt)

		nfile = nfile + 1
		if (nfile%1000)==0:
			print('{} files processed'.format(nfile))
except:
	print('An exception has occurred.')
finally:
	db_con.close()
	print('Database connection closed.')
	print('--- {} files processed in total ---'.format(nfile))
		


