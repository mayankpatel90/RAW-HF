import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.opencsv.CSVWriter;

public class DBInsert2 {
	//public static long globalData = 0;
	public static Connection con;
	public static boolean scale=false;
	public static void main(String[] args) {
		try
		{
			 
		//DBInsert.con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");
		//	Connection con = DriverManager.getConnection(DBInsert.DBCon1, DBInsert.U1, DBInsert.P1);
		//	Connection con = DriverManager.getConnection(DBInsert.DBCon1, DBInsert.U1, DBInsert.P1);

			//main.co
		//set the LODTriples RecordsTotal, as 504178(100MB, ~5lakh record),800937 (200MB,~8Lakh records), 1GB, ...
		int RecordsTotal = 504178;
		RecordsTotal = 800937;
		//RecordsTotal = 101484389; //26GB  //Insert done in CSV but after that PC hang
		RecordsTotal = 10000000; //2.3GB//10M
		RecordsTotal = 50000000; //13.3GB//50M
		RecordsTotal = 10800000; //2.3GB//K_10M
		
		//Datasets
		String fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriples.csv";
		String SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriples2.sql";
		
		//Queries
		//RAW LODTriples
		String SQLQueryPath = "/home/mayank/workspace/DBRAW1/queries/LODTriplesQueries.csv";
		//DBMS LODTriples2
		String SQLQueryPath2 = "/home/mayank/workspace/DBRAW1/queries/LODTriples2Queries.csv";
		
		//CSV file used as NoDB, database in PostgresRAW    
		// Snoof.conf  (path: /usr/local/pgraw_db3/snoop.conf)
		//setting for Snoof.conf , NODB, PgRAW
		//filename-1 = '/home/mayank/workspace/DBRAW1/NODB/LODTriples.csv'
		//relation-1 = 'lodtriples'
		//delimiter-1 = ','
		
		//Now RAW DB file table is changed to  LODTriples2. from 31-oct-2018
		String NODBfileOutPath = "/home/mayank/workspace/DBRAW1/NODB/LODTriples.csv";
		//DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		//Date date = new Date();
		//System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss");
		LocalDateTime now = LocalDateTime.now();
		System.out.println(dtf.format(now)); //2016/11/16 12:08:43
		
		String ResultOutPath = "/home/mayank/workspace/DBRAW1/results/result"+dtf.format(now)+".csv";
		//String QueryResultOutPath = "/home/mayank/workspace/DBRAW1/results/r1.csv";
		//String QueryResultOutPath2 = "/home/mayank/workspace/DBRAW1/results/r2.csv";
	
		if(RecordsTotal == 504178)
		{
			fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriples.csv";

	
		
			//#CSV File ready......File size : 102.4 MB (10,23,93,813 bytes)
			SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriples2.sql";
			//#So only insert SQL statements for LODTriples2 table is ready.
			//#File size: 132.6 MB (13,26,44,493 bytes)
			
			SQLQueryPath = "/home/mayank/workspace/DBRAW1/queries/LODTriplesQueries.csv";
			SQLQueryPath2 = "/home/mayank/workspace/DBRAW1/queries/LODTriples2Queries.csv";
			
			System.out.println("100MB  File size LODTriples, Dataset used, 504178 records");
			writeCSV(ResultOutPath, "100MB  File size LODTriples, Dataset used, 504178 records", true);
		}
		else if(RecordsTotal == 800937)
		{
			//8lakh records LODTriplesFile
			fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlain8lakh.csv";
			//#CSV File ready......File size : 155.1 MB (15,50,75,951 bytes)
			SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlain8lakh2.sql";
			//#So only insert SQL statements for LODTriples2 table is ready.
			//#File size: 202.3 MB (20,23,31,234 bytes)
			
			SQLQueryPath = "/home/mayank/workspace/DBRAW1/queries/LODTriplesQueries.csv";
			SQLQueryPath2 = "/home/mayank/workspace/DBRAW1/queries/LODTriples2Queries.csv";
			
			System.out.println("200MB File size LODTriples, Dataset used, 800937 records");
			writeCSV(ResultOutPath, "200MB  File size LODTriples, Dataset used, 800937 records", true);
		}
		else if(RecordsTotal == 101484389)
		{
			//Now RAW DB file table is changed to  LODTriples2. from 31-oct-2018
			//Both SQL and CSV data-sets are taken from a single file
			//1rec.txt
			//boolean isSQLDataset = true; 
			fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlain26GB.txt";
			//#CSV File ready......File size : 155.1 MB (15,50,75,951 bytes)
			SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlain26GB.txt";
			
			//1rec.txt
		/*	fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/1rec.txt";
			SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/1rec.txt";*/
			
			//Now RAW DB file table is changed to  LODTriples2. from 31-oct-2018
			SQLQueryPath2 = "/home/mayank/workspace/DBRAW1/queries/LODTriplesQueries.csv";
			SQLQueryPath = "/home/mayank/workspace/DBRAW1/queries/LODTriples2Queries.csv";
			
			System.out.println("26GB File size LODTriples, Dataset used, 101484389 records");
			writeCSV(ResultOutPath, "26GB  File size LODTriples, Dataset used, 101484389 records", true);
		}
		else if (RecordsTotal == 10000000)
		{
			//LODTriplesPlain10M.txt //2.3GB 
			fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlain10M.txt";
			SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlain10M.txt";
			//boolean isSQLDataset = true;

			//Now RAW DB file table is changed to  LODTriples2. from 31-oct-2018
			SQLQueryPath2 = "/home/mayank/workspace/DBRAW1/queries/LODTriplesQueries.csv";
			SQLQueryPath = "/home/mayank/workspace/DBRAW1/queries/LODTriples2Queries.csv";
			
			System.out.println("2.3GB + K File size LODTriples, Dataset used, 10000000 10M records");
			writeCSV(ResultOutPath, "2.3GB + K  File size LODTriples, Dataset used, 10000000 10M records", true);
		}
		else if (RecordsTotal == 50000000)
		{
			//LODTriplesPlain13GB.txt //13GB
			
			fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlain13GB.txt";
			SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlain13GB.txt";
			
		//	fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlainK.sql";
		//	SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlainK.sql";
			//boolean isSQLDataset = true;
			
			//LODTriplesPlainK.sql
			
			//Now RAW DB file table is changed to  LODTriples2. from 31-oct-2018
			SQLQueryPath2 = "/home/mayank/workspace/DBRAW1/queries/LODTriplesQueries.csv";
			SQLQueryPath = "/home/mayank/workspace/DBRAW1/queries/LODTriples2Queries.csv";
			
			System.out.println("13GB File size LODTriples, Dataset used, 50000000 50M records");
			writeCSV(ResultOutPath, "13GB File size LODTriples, Dataset used, 50000000 50M records", true);
		}
		else if(RecordsTotal == 10800000)
		{
			//LODTriplesPlain10M.txt //2.3GB + Kalgi data
			fileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlainK_10M.txt";
			SQLfileInpath = "/home/mayank/workspace/DBRAW1/Datasets/LODTriplesPlainK_10M.txt";
			//boolean isSQLDataset = true;

			//Now RAW DB file table is changed to  LODTriples2. from 31-oct-2018, queirs _All for all queries
			SQLQueryPath2 = "/home/mayank/workspace/DBRAW1/queries/LODTriplesQueries.csv";
			SQLQueryPath = "/home/mayank/workspace/DBRAW1/queries/LODTriples2Queries.csv";
			
			//Now RAW DB file table is changed to  LODTriples2. from 31-oct-2018, queirs _All for all queries
			SQLQueryPath2 = "/home/mayank/workspace/DBRAW1/queries/LODTriplesQueries_K.csv";
			SQLQueryPath = "/home/mayank/workspace/DBRAW1/queries/LODTriples2Queries_K.csv";
			
			System.out.println("2.3GB + K File size LODTriples, Dataset used, 10000000 10M records");
			writeCSV(ResultOutPath, "2.3GB + K  File size LODTriples, Dataset used, 10000000 10M records", true);
		}		
		
		
		int lines=0;
		boolean append=true; 
		int bulkNo=0; 
		boolean runQueries = false; //Run Queries in between of bulk save
		boolean isSQLDataset = false;
	
		String DBCon1="jdbc:postgresql://localhost:5432/mayank";
		String U1="mayank";
		String P1="postgres";
		String DBCon2="jdbc:postgresql://localhost:5432/mayank"; 
		String U2="mayank";
		String P2="postgres";
		
		//Delete the data in CSV file
		delCSVFile(NODBfileOutPath);
		 
		//one by one save data to DBMS & CSV, DBMS connection is made only once
		//exp1(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2, U2, P2);
		
		//bulk save data to CSV 7 DBMS
		//exp2(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2, U2, P2);
		
		//bulk save data to CSV 7 DBMS + SQL Queries RUN + Connection made once and all queries run one after another...
		//exp2plus(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2);
				
		//Insert Specific No. of Data in bulk save(1,100,1000,10000,100000,1000000(1M)) to CSV 7 DBMS + SQL Queries RUN + Connection made once and all queries run one after another...
		
		writeCSV(ResultOutPath, "insert 2nd run", true);

	/*	bulkNo=100;
		runQueries=false;
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
		
		bulkNo=1000;
		runQueries=false;
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
	
		bulkNo=10000;
		runQueries=false;
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
	
		bulkNo=50000;
		runQueries=false;
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
	
		bulkNo=100000;
		runQueries=false;
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
	
		bulkNo=300000;
		runQueries=false;
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
	
		bulkNo=500000;
		runQueries=false;
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
	
		bulkNo=700000;
		runQueries=false;
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
			*/
		bulkNo=800937;
		scale=false;
		bulkNo=1000000; //when scale is True, bulk becomes the scaling factor, and increase in ZipF fashion, 1,2,4,8...
		runQueries=true;
		isSQLDataset=true;  //True when using only one SQL file for CSV as well. 
		exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries, isSQLDataset);
	
		//bulkNo=100;
		//runQueries=true;
		//exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1,  DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
		}
		catch(Exception e){
			return;
		}
	}
	
	public static String exp1(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2) {
		
		String resultscount= "";
		
		writeCSV(ResultOutPath, "Exp START ", append);
		String exp = "Exp1.1, Basic GET Data from CSV check";
		Instant start = Instant.now();		
		List<String> rdata = getLstfromCSV(fileInpath);		
		Instant end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		System.out.println("Got data from CSV");
		
		exp = "Exp1.2, Basic Insert Data To CSV check";
		append = false; //Save data to find, read from CSV, no extra data so False.
		start = Instant.now();		
		writeCSV(NODBfileOutPath, rdata, append);		
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		append  = true;
		
		exp = "Exp1.3, Basic GET Data from CSV check";
		start = Instant.now();
		List<String> SQLqrydata = getLstfromCSV(SQLfileInpath);		
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		exp = "Exp1.4, Basic Insert Data To PostgresSQL check";
		start = Instant.now();		
		SQLInsert(SQLqrydata, "0");
		end = Instant.now();
		//System.out.println("SQLqrydata:" + SQLqrydata[0]);
		//String s3 = SQLInsert(SQLqrydata, "0");
		//System.out.println(s3);		
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		 
		start = Instant.now();		
		resultscount = SQLQuery("Select count(*) from LODTriples", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 1.5, Count Check PostgresRAW", resultscount, "Select count(*) from LODTriples", ResultOutPath);
		
		start = Instant.now();		
		resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 1.6, Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples2", ResultOutPath);
		
		//delete csvfile data & create new file of same name
		start = Instant.now();	
		resultscount = delCSVFile(NODBfileOutPath);
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 1.7, Delete file CSV", resultscount, "delCSVFile(NODBfileOutPath) = " + NODBfileOutPath, ResultOutPath);
		
		start = Instant.now();	
		resultscount = SQLQuery("TRUNCATE TABLE LODTriples2", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp, 1.8, Delete table data PGSQL", resultscount, "TRUNCATE TABLE LODTriples2", ResultOutPath);
		
		
		resultscount = SQLQuery("Select count(*) from LODTriples", "0");
		logTimetoCSV(start, end, "Exp 1.9, After Delete Count Check PostgreRAW", resultscount, "Select count(*) from LODTriples", ResultOutPath);
		
		resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
		logTimetoCSV(start, end, "Exp 1.10, After Delete Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples2", ResultOutPath);
				
		writeCSV(ResultOutPath, "Exp END ", append);		
		System.out.println("Exp Ended....");
		
		return "";				
	}
	
	public static String exp2(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2) {
		String resultscount= "";
		
		append = true;
		writeCSV(ResultOutPath, "Exp START ", append);
		String exp = "Exp2.1, Basic GET Data from CSV check";
		Instant start = Instant.now();		
		List<String[]> rdata = getStrfromCSV2(fileInpath, 0);		
		Instant end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		System.out.println("Got data from CSV");
		
		exp = "Exp2.2, Bulk Insert Data To CSV check";
		append = false;
		start = Instant.now();
		writeCSVB3(NODBfileOutPath, rdata, append);		
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		append = true;
		
		exp = "Exp2.3, Basic GET Data from CSV check";
		start = Instant.now();
		List<String> SQLqrydata = getLstfromCSV(SQLfileInpath);
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		exp = "Exp2.4, Bulk Insert Data To PostgresSQL check";
		start = Instant.now();
		SQLBatchInsert(SQLqrydata, "0");
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		 
		start = Instant.now();
		resultscount = SQLQuery("Select count(*) from LODTriples", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 2.5, Count Check PostgresRAW", resultscount, "Select count(*) from LODTriples", ResultOutPath);
		
		start = Instant.now();		
		resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 2.6, Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples2", ResultOutPath);
		/*
		start = Instant.now();	
		resultscount = delCSVFile(NODBfileOutPath);
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 2.7, Delete file CSV & Create New", resultscount, "delCSVFile(NODBfileOutPath) = " + NODBfileOutPath, ResultOutPath);
		
		start = Instant.now();	
		resultscount = SQLQuery("TRUNCATE TABLE LODTriples2", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 2.8, Delete table data PGSQL", resultscount, "TRUNCATE TABLE LODTriples2", ResultOutPath);
		
		
		resultscount = SQLQuery("Select count(*) from LODTriples", "0");
		logTimetoCSV(start, end, "Exp 2.9, After Delete Count Check PostgreRAW", resultscount, "Select count(*) from LODTriples", ResultOutPath);
		
		resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
		logTimetoCSV(start, end, "Exp 2.10, After Delete Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples2", ResultOutPath);
				*/
		writeCSV(ResultOutPath, "Exp END ", append);		
		System.out.println("Exp Ended....");
		
		return "";
	}
	
	//exp2+
	public static String exp2plus(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2, String SQLQueryPath, String SQLQueryPath2) {
		String resultscount= "";
		
		
		//CSV(insert) RAW Start, NODB, PostgresRAW (Queries only)
		append = true;
		writeCSV(ResultOutPath, "RAW + SQL Exp2+ START with Queries RAW ", append);
		String exp = "Exp2.1, Basic GET Data from CSV check";
		Instant start = Instant.now();		
		List<String[]> rdata = getStrfromCSV2(fileInpath, 0);		
		Instant end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		System.out.println("Got data from CSV");
		
		exp = "Exp2.2, Bulk Insert Data To CSV check";
		append = false;
		start = Instant.now();
		writeCSVB3(NODBfileOutPath, rdata, append);		
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		append = true;		 
		
		start = Instant.now();		
		resultscount = SQLQuery("Select count(*) from LODTriples", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 2.3, Count Check PostgresRAW", resultscount, "Select count(*) from LODTriples", ResultOutPath);
		
		//SQL Query GET 
		exp = "Exp2.4, GET SQL Queries List from CSV Time";
		start = Instant.now();
		List<String[]> Qdata = getQryStrfromCSV2(SQLQueryPath, 0);	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		//SQL Query RUN
		exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
		start = Instant.now();
		List<String[]> r2data = SQLQuery(Qdata, "All", ResultOutPath, DBCon1, U1, P1);	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		//SQL Query Results Save to CSV
		//exp = "Exp2.5.1, Bulk Insert Query Results Data To CSV check [Copy of the Data but with Instant start & Instant end data]";
		exp = "Exp 2.5.1, List of Queries used in above exp.";
		start = Instant.now();
		writeCSVB3(ResultOutPath, r2data, append);	 //Append	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		
		//Delete Data
		start = Instant.now();	
		resultscount = delCSVFile(NODBfileOutPath);
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 2.6, Delete file CSV & Create New", resultscount, "delCSVFile(NODBfileOutPath) = " + NODBfileOutPath, ResultOutPath);
	
		resultscount = SQLQuery("Select count(*) from LODTriples", "0");
		logTimetoCSV(start, end, "Exp 2.7, After Delete Count Check PostgreRAW", resultscount, "Select count(*) from LODTriples", ResultOutPath);
	
		writeCSV(ResultOutPath, "RAW + SQL 2+ EXP END. ", append);	
		
		
		//DBMS start, PostgresSQL **********************************************
		writeCSV(ResultOutPath, "DBMS + SQL 2.8+ EXP Start... ", append);	
		
		exp = "Exp2.8, Basic GET SQL insert statements Data from CSV check ";
		start = Instant.now();
		List<String> SQLqrydata = getLstfromCSV(SQLfileInpath);		
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		exp = "Exp2.9, Bulk Insert Data To PostgresSQL check";
		start = Instant.now();
		SQLBatchInsert(SQLqrydata, "0");
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		start = Instant.now();		
		resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 2.10, Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples2", ResultOutPath);
		
		//SQL Query GET 
		exp = "Exp2.11, GET SQL Queries List from CSV Time";
		start = Instant.now();
		List<String[]> Q2data = getQryStrfromCSV2(SQLQueryPath2, 0);	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		//SQL Query RUN
		exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
		start = Instant.now();
		List<String[]> r3data = SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1);	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		//SQL Query Results Save to CSV
		exp = "Exp2.12.1, Bulk Insert Query Results Data To CSV check [Copy of the Data but with Instant start & Instant end data]";
		start = Instant.now();
		writeCSVB3(ResultOutPath, r3data, append);	 //Append	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		//Delete Data
		start = Instant.now();	
		resultscount = SQLQuery("TRUNCATE TABLE LODTriples2", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp 2.13, Delete table data PGSQL", resultscount, "TRUNCATE TABLE LODTriples2", ResultOutPath);
					
		resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
		logTimetoCSV(start, end, "Exp 2.14, After Delete Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples2", ResultOutPath);
				
		writeCSV(ResultOutPath, "DBMS + SQL 2.8+ Exp END ", append);		
		System.out.println("Exp Ended....");
		
		return "";
	}
	
	public static String exp3(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2, String SQLQueryPath, String SQLQueryPath2, int bulkNo, boolean runQueries, boolean isSQLDataset) {
		String resultscount= "";
		
		
		//CSV(insert) RAW Start, NODB, PostgresRAW (Queries only)
		append = true;
		writeCSV(ResultOutPath, "RAW + SQL Exp3+ START with Queries RAW ", append);
		String exp = "Exp3.1, Basic GET Data from CSV check";
		Instant start = Instant.now();		
		List<String[]> rdata = getInsertStrfromCSV3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries, isSQLDataset);		
		Instant end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		System.out.println("Got data from CSV");
		System.gc();
		/*Not needed in this
		 * 
		 * exp = "Exp3.2, -Save-x- -x-Errors "BUT It inserts data again" during Bulk Insert Data To CSV check";
		start = Instant.now();
		writeCSVB3(ResultOutPath, rdata, append);
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		*
		*/
		
		/*Not needed in this
		 * 
		 * exp = "Exp3.2, Bulk Insert Data To CSV check";
		append = false;
		start = Instant.now();
		writeCSVB3(NODBfileOutPath, rdata, append);		
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		append = true;		 
		*
		*
		*/
		
		start = Instant.now();		
		resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp3.3, Count Check PostgresRAW", resultscount, "Select count(*) from LODTriples2", ResultOutPath);
		
		
		//SQL Query GET 		
		exp = "Exp3.4, GET SQL Queries List from CSV Time";
		start = Instant.now();
		List<String[]> Qdata = getQryStrfromCSV2(SQLQueryPath, 0);	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		
		//SQL Query Write/Log
		exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
		start = Instant.now();
		List<String[]> r2data = SQLQuery(Qdata, "All", ResultOutPath, DBCon1, U1, P1);	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		
		//SQL Query Results Save to CSV
		//exp = "Exp3.5.1, Bulk Insert Query Results Data To CSV check [Copy of the Data but with Instant start & Instant end data]";
		exp = "Exp3.5.1, List of Queries used in above exp.";
		start = Instant.now();
		writeCSVB3(ResultOutPath, r2data, append);	 //Append	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		start = Instant.now();
		resultscount = SQLQuery("Select count(distinct(sub, obj, pred)) from LODTriples2", "0");
		start = Instant.now();
		logTimetoCSV(start, end, "Exp3.6.0, After query count(distinct(sub, obj, pred)) Check PostgreSQL", resultscount, "Select count(distinct(sub, obj, pred)) from LODTriples2", ResultOutPath);
		
		//Delete Data
		start = Instant.now();	
		resultscount = delCSVFile(NODBfileOutPath);
		end = Instant.now();
		logTimetoCSV(start, end, "Exp3.6.1, Delete file CSV & Create New", resultscount, "delCSVFile(NODBfileOutPath) = " + NODBfileOutPath, ResultOutPath);
	
		resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
		logTimetoCSV(start, end, "Exp3.7, After Delete Count Check PostgreRAW", resultscount, "Select count(*) from LODTriples2", ResultOutPath);
	
		writeCSV(ResultOutPath, "RAW + SQL3+ EXP END. ", append);	
		
		
		//DBMS start, PostgresSQL **********************************************
		writeCSV(ResultOutPath, "DBMS + SQL3.8+ EXP Start... ", append);	
		
		exp = "Exp3.8, Basic GET SQL insert statements Data from CSV check ";
		start = Instant.now();
		List<String> SQLqrydata = getInsertSqlLstFromCSV(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		exp = "Exp3.9  Errors listed above, Total time to Save errors came during the insert,";
		start = Instant.now();
		writeCSV(ResultOutPath, SQLqrydata, append);
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		/*Not needed in this
		 * 
		 * exp = "Exp3.9, Bulk Insert Data To PostgresSQL check";
		start = Instant.now();		
		SQLBatchInsert(SQLqrydata, "0");
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		*
		*
		*/
		
		start = Instant.now();		
		resultscount = SQLQuery("Select count(*) from LODTriples", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp3.10, Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples", ResultOutPath);
		
		//SQL Query GET 
		exp = "Exp3.11, GET SQL Queries List from CSV Time";
		start = Instant.now();
		List<String[]> Q2data = getQryStrfromCSV2(SQLQueryPath2, 0);	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		//SQL Query RUN
		exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
		start = Instant.now();
		List<String[]> r3data = SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1);	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		resultscount = SQLQuery("Select count(distinct(sub, obj, pred)) from LODTriples", "0");
		logTimetoCSV(start, end, "Exp3.12.0, After query count(distinct(sub, obj, pred)) Check PostgreSQL", resultscount, "Select count(distinct(sub, obj, pred)) from LODTriples", ResultOutPath);
		
		//SQL Query Results Save to CSV
		exp = "Exp3.12.1, Bulk Insert Query Results Data To CSV check [Copy of the Data but with Instant start & Instant end data]";
		start = Instant.now();
		writeCSVB3(ResultOutPath, r3data, append);	 //Append	
		end = Instant.now();
		logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
		
		//Delete Data
		start = Instant.now();	
		resultscount = SQLQuery("TRUNCATE TABLE LODTriples", "0");
		end = Instant.now();
		logTimetoCSV(start, end, "Exp3.13, Delete table data PGSQL", resultscount, "TRUNCATE TABLE LODTriples", ResultOutPath);
					
		resultscount = SQLQuery("Select count(*) from LODTriples", "0");
		logTimetoCSV(start, end, "Exp3.14, After Delete Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples", ResultOutPath);
				
		writeCSV(ResultOutPath, "DBMS + SQL 3.8+ Exp END ", append);		
		System.out.println("Exp Ended....");
		
		return "";
	}
	//Get data from CSV in String Array
	public static String[] getStrfromCSV(String filepath) {
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = null;
			int i = 0;
			String[] sqllist = new String[6000000];	
			while ((line = reader.readLine()) != null) {				
				i = 0;
				sqllist[i]=line;
				i++;									
			}			
			reader.close();
			return sqllist;
		}
		catch(IOException e)
	    {
			 String[] sqllist = new String[2];
			 sqllist[0] = "SQL exception occured" + e;
	         return sqllist;
	    }	  
	}
	
	//Get data from CSV in List String 
	public static List<String> getLstfromCSV(String filepath) {
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = null;			
			List<String> sqllist = new ArrayList<>();	
			while ((line = reader.readLine()) != null) {
				sqllist.add(line);
			}
			reader.close();
			return sqllist;
		}
		catch(IOException e)
	    {
			 List<String> sqllist = new ArrayList<>();	
			 sqllist.add("SQL exception occured" + e);				 
	         return sqllist;
	    }	  
	}
	
	//Get data from CSV in List String 
	public static List<String> getInsertSqlLstFromCSV(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2, String SQLQueryPath, String SQLQueryPath2,  int bulkNo, boolean runQueries){
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(SQLfileInpath));
			String line = null;			
			String exp = "";
			List<String> sqllist = new ArrayList<>();
			List<String[]> Q2data = new ArrayList<>();
			Instant start;
			Instant end;
			int i=0;
			int scaletime =0;
			if(runQueries == true)
			{
				//SQL Query GET 
				exp = "Exp3.11, GET SQL Queries List from CSV Time";
				start = Instant.now();
				Q2data = getQryStrfromCSV2(SQLQueryPath2, 0);	
				end = Instant.now();
				logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			}
			while ((line = reader.readLine()) != null) {
				sqllist.add(line);
				i++;
				if(i==bulkNo)
				{
					start = Instant.now();					
					SQLBatchInsert(sqllist, "0");
					end = Instant.now();
					logSimpalQryOrInsetTimeToCSV(start, end, "insert,"+i+",", "-", ResultOutPath);					
					sqllist.clear();
					i=0;
					
					//to scale the records, //1lakh->1+1->2 lakh = 2+2 =4lakh
					if(scale == true && scaletime != 0)
					{
						bulkNo = bulkNo+bulkNo;
					}
					scaletime++;
					if(runQueries == true)
					{					
						start = Instant.now();		
						String rcPgSQL = SQLQuery("Select count(*) from LODTriples2", "0");
						end = Instant.now();
						logTimetoCSV(start, end, "Exp3.10, Count Check PostgreSQL", rcPgSQL, "Select count(*) from LODTriples2", ResultOutPath);
						
						//SQL Query RUN
						exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
						start = Instant.now();
						List<String[]> r3data = SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1);	
						end = Instant.now();
						logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
					}
				}
			}
			reader.close();
			
			start = Instant.now();
			SQLBatchInsert(sqllist, "0");
			end = Instant.now();
			logSimpalQryOrInsetTimeToCSV(start, end, "insert,"+i+",", "-", ResultOutPath);					
			sqllist.clear();
			i=0;
			
			if(runQueries == true)
			{				
				//SQL Query RUN
				exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
				start = Instant.now();
				List<String[]> r3data = SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1);	
				end = Instant.now();
				logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			}
			return sqllist;
		}
		catch(IOException e)
	    {
			 List<String> sqllist = new ArrayList<>();	
			 sqllist.add("SQL exception occured" + e);				 
	         return sqllist;
	    }	  
	}
	//Get records from CSV as List of String Arrays, strLength = 3 for Triplestore, It's basically No_of_columns of a table
	public static List<String[]> getStrfromCSV2(String filepath, int strLength) {
		try
		{			
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = null;
			List<String[]> recList = new ArrayList<>();
		
			while ((line = reader.readLine()) != null) {
				String [] rec = line.split(",");				
				recList.add(rec);
			}
			reader.close();			
			return(recList);
		}
		catch(IOException e)
	    {
			 List<String[]> reclist = new ArrayList<>();
			 String[] rec= new String[2];
			 rec[0] = "SQL exception occured" + e;
			 reclist.add(rec);
	         return reclist;
	    }
	}
	
	//Get records from CSV as List of String Arrays, strLength = 3 for Triplestore, It's basically No_of_columns of a table
		public static List<String[]> getInsertStrfromCSV3(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2, String SQLQueryPath, String SQLQueryPath2,  int bulkNo, boolean runQueries, boolean isSQLDataset){
			try
			{			
				BufferedReader reader = new BufferedReader(new FileReader(fileInpath));
				String line = null;
				List<String[]> recList = new ArrayList<>();
				List<String[]> Qdata = new ArrayList<>();
				int i=0;
				int scaletime=0; //first time, we don't want to increase bulkno
				Instant start;
				Instant end;
				String exp;
				if(runQueries == true)
				{
					exp = "Exp3.4, GET SQL Queries List from CSV Time";
					start = Instant.now();
					Qdata = getQryStrfromCSV2(SQLQueryPath, 0);	
					end = Instant.now();
					logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
					
				}
				while ((line = reader.readLine()) != null) {					
					if(isSQLDataset == true)
					{					
						line = line.split(" VALUES" )[1].trim().replace("('", "").replace("', '", ",").replace("');", "");
					/*	
					 * //System.out.println(line+"\n");
						line = line.replace("('", "");
						//System.out.println(line+"\n");
						line = line.replace("', '", ",");
						//System.out.println(line+"\n");
						line = line.replace("');", "");
						//System.out.println("Hey 2.3\n");
						//System.out.println(line+"\n"); 
						 * 
						 */										
					}
					String [] rec = line.split(",");				
					recList.add(rec);
					i++;
					if(i==bulkNo)
					{						
						start = Instant.now();
						writeCSVB3(NODBfileOutPath, recList, append);		
						end = Instant.now();
						logSimpalQryOrInsetTimeToCSV(start, end, "insert,"+i+",", "-", ResultOutPath);
						recList.clear();
						//System.gc();

						i=0;
						
						//to scale the records, //1lakh->1+1->2 lakh = 2+2 =4lakh
						if(scale == true && scaletime != 0)
						{
							bulkNo = bulkNo+bulkNo;
						}
						scaletime++;
						if(runQueries == true)
						{
							start = Instant.now();		
							String rcCSV = SQLQuery("Select count(*) from LODTriples", "0");
							end = Instant.now();
							logTimetoCSV(start, end, "Exp3.3, Count Check PostgresRAW", rcCSV, "Select count(*) from LODTriples", ResultOutPath);
							
							//SQL Query RUN
							exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
							start = Instant.now();
							List<String[]> r2data = SQLQuery(Qdata, "All", ResultOutPath, DBCon1, U1, P1);	
							end = Instant.now();
							logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
						}
					}
				}
				reader.close();
				start = Instant.now();
				writeCSVB3(NODBfileOutPath, recList, append);
				end = Instant.now();
				logSimpalQryOrInsetTimeToCSV(start, end, "insert,"+i+",", "-", ResultOutPath);
				recList.clear();
				//System.gc();
				i=0;
				if(runQueries == true)
				{					
					//SQL Query RUN
					exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
					start = Instant.now();
					List<String[]> r2data = SQLQuery(Qdata, "All", ResultOutPath, DBCon1, U1, P1);	
					end = Instant.now();
					logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
				}
				return(recList);
			}
			catch(IOException e)
		    {
				System.out.println("Hey Error 3\n");
				 List<String[]> reclist = new ArrayList<>();
				 String[] rec= new String[2];
				 rec[0] = "SQL exception occured" + e;
				 reclist.add(rec);
		         return reclist;
		    }
		}
	
	//getQryStrfromCSV2
	//Get Query records from CSV as List of String Arrays, strLength = 3 for Triplestore, It's basically No_of_columns of a table
	public static List<String[]> getQryStrfromCSV2(String filepath, int strLength) {
		try
		{			
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = null;
			List<String[]> recList = new ArrayList<>();
		
			while ((line = reader.readLine()) != null) {
				String[] rec = line.toString().split(",", 2);
				rec[1] = rec[1].replace("\"\"", "\"");  //A single double quote is replaced by double double quote in readLine 
				System.out.println("line ="+line.toString()+ "\n rec[0] =" + rec[0].toString()+ "rec[1] =" + rec[1].toString());				
				recList.add(rec);
			}
			reader.close();
			return(recList);
		}
		catch(IOException e)
	    {
			 List<String[]> reclist = new ArrayList<>();
			 String[] rec= new String[2];
			 rec[0] = "SQL exception occured" + e;
			 reclist.add(rec);
	         return reclist;
	    }
	}
	
	
	/* Test CSV WRITE DATA code
	public static float writeCSV(String filePath, String[] records, boolean append) {
		File file = new File(filePath);
		 
	    try {
	        // create FileWriter object with file as parameter
	        FileWriter outputfile = new FileWriter(file, append);
	 
	        // create CSVWriter object filewriter object as parameter
	        CSVWriter writer = new CSVWriter(outputfile);
	        
	        for(int i=0; i<records.length; i++)
	        {
	        	writer.writeNext(new String[] {records[i]});
	        }
	        
	        // BULK SAVE:  create a List which contains String array
	        //writer.writeAll(records);
	        
	        
	        //List<String[]> data = new ArrayList<String[]>();
	        //data.add(new String[] { "Name", "Class", "Marks" });
	        //data.add(new String[] { "Aman", "10", "620" });
	        //data.add(new String[] { "Suraj", "10", "630" });
	        //writer.writeAll(data);
	 
	        // closing writer connection
	        writer.close();
	        return 1;
	    }
	    catch (IOException e) {
	        // 
	        e.printStackTrace();
	        return 0;
	    }
	}*/
	
	
		
	/* Test CSV GET DATA code
	public static String[] getfromCSV(String filepath, int lines, boolean append) {
		try
		{
			// open file input stream
			//BufferedReader reader = new BufferedReader(new FileReader("employees.csv"));
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			
			// read file line by line
			String line = null;
			//Scanner scanner = null;
			int i = 0;
			String[] sqllist = new String[6000000];
	
			while ((line = reader.readLine()) != null) {
				//Employee emp = new Employee();
				//scanner = new Scanner(line);
				//scanner.useDelimiter(",");
				//while (scanner.hasNext()) {
				i = 0;
				sqllist[i]=line;
				System.out.println("data::" + line);
				i++;
				
				//}
				//index = 0;			
			}
			
			//close reader
			reader.close();
			return sqllist;
		}
		catch(IOException e)
	    {
			 String[] sqllist = new String[2];
			 sqllist[0] = "SQL exception occured" + e;
	         return sqllist;
	    }	  
	}
	*/
	
	// CSV WRITE DATA code, 1 write Single Record data to CSV, File opens again and closes after saving single record
	public static float writeCSV(String filePath, String record, boolean append) {
		File file = new File(filePath);
		 
	    try {	       
	        FileWriter outputfile = new FileWriter(file, append); //true is append data at end of CSV file
	        CSVWriter writer = new CSVWriter(outputfile);
	        writer.writeNext(record.split(","));
	        writer.close();
	        return 1;
	    }
	    catch (IOException e) {
	        e.printStackTrace();
	        return 0;
	    }
	}
	// CSV WRITE DATA code, 1 by 1 write data to CSV, File open once from String Array
	public static float writeCSV(String filePath, String[] records, boolean append) {
		File file = new File(filePath);
		 
	    try {	       
	        FileWriter outputfile = new FileWriter(file, append);
	        CSVWriter writer = new CSVWriter(outputfile);
	        for(int i=0; i<records.length; i++)
	        {
	        	System.out.print("records.length" + records.length);
	        	if(records[i] != "")
	        	{
	        		System.out.print(records[i]);
	        		writer.writeNext(records[i].split(","));
	        	}
	        	System.out.print(records[i]);
	        }
	        writer.close();
	        return 1;
	    }
	    catch (IOException e) {
	        e.printStackTrace();
	        return 0;
	    }
	}
	
	// CSV WRITE DATA code, 1 by 1 write data to CSV, File open once from List String
	public static float writeCSV(String filePath, List<String> records, boolean append) {
		File file = new File(filePath);
		 
	    try {	       
	        FileWriter outputfile = new FileWriter(file, append);
	        CSVWriter writer = new CSVWriter(outputfile);
	        for(int i=0; i<records.size(); i++)
	        {
	        	writer.writeNext(records.get(i).split(","));
	        }
	        writer.close();
	        return 1;
	    }
	    catch (IOException e) {
	        e.printStackTrace();
	        return 0;
	    }
	}
	
	// CSV WRITE DATA code,BULK write data to CSV, File open once from String Array
	public static float writeBCSV(String filePath, String[] records, boolean append) {
		File file = new File(filePath);
		 
	    try {	       
	        FileWriter outputfile = new FileWriter(file, append);
	        CSVWriter writer = new CSVWriter(outputfile);
	        writer.writeNext(records);	        
	        writer.close();
	        return 1;
	    }
	    catch (IOException e) {
	        e.printStackTrace();
	        return 0;
	    }
	}
	// CSV WRITE DATA code,BULK write data to CSV, File open once from String Array
	public static float writeCSVB2(String filePath, List<String> records, boolean append) {
		File file = new File(filePath);
		 
	    try {	       
	        FileWriter outputfile = new FileWriter(file, append);
	        CSVWriter writer = new CSVWriter(outputfile);	 
	        String[] S = records.toArray(new String[0]);
	        writer.writeNext(S);       
	        writer.close();
	        return 1;
	    }
	    catch (IOException e) {
	        e.printStackTrace();
	        return 0;
	    }
	}
	// CSV WRITE DATA code,BULK write data to CSV, File open once from String Array
	public static float writeCSVB3(String filePath, List<String[]> records, boolean append) {
		File file = new File(filePath);
		 
	    try {	       
	        FileWriter outputfile = new FileWriter(file, append);
	        CSVWriter writer = new CSVWriter(outputfile);	 
	        writer.writeAll(records);       
	        writer.close();
	        return 1;
	    }
	    catch (IOException e) {
	        e.printStackTrace();
	        return 0;
	    }
	}
	
	// DELETE CSV WRITE DATA code, 1 write Single Record data to CSV, File opens again and closes after saving single record
	public static String delCSVFile(String filePath) {
		try
		{
			File file = new File(filePath);
			if(file.exists())
			{
				file.delete();
			}
			if (file.createNewFile()){
				System.out.println("File is created!");
			}
			return "File deleted and new created";
		}
		catch(IOException e)
		{
			return "Error in File deletion and creation "+ e.getMessage().toString();
		}	   
	}
	
	public static int logTimetoCSV(Instant start, Instant end, String exp, String fileInpath, String NODBfileOutPath, String ResultOutPath) {
		float timeElapsed = calcTime(start, end);	
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		System.out.println(dtf.format(now)); //2016/11/16 12:08:43
		writeCSV(ResultOutPath, "Exp:, "+ exp +", " + timeElapsed +", milliseconds, Currect DateTime, "+ dtf.format(now) +", fileInpath, "+ fileInpath + ", NODBfileOutPath, " + NODBfileOutPath +", ResultOutPath "+ResultOutPath + "\n", true);
		//writeCSV(ResultOutPath, , 0);		
		return 1;
	}
	
	public static float calcTime(Instant start, Instant end){
		Duration timeElapsed = Duration.between(start, end);
		System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");		
		return timeElapsed.toMillis();
	}
	
	public static int logSimpalQryOrInsetTimeToCSV(Instant start, Instant end, String s1, String s2, String ResultOutPath){
		float timeElapsed = calcTime(start, end);	
		writeCSV(ResultOutPath, s1+","+timeElapsed+", milliseconds, "+s2, true);
		return 1;
	}
	
	//SQL Insert means, Directly executing SQL statements, not creating insert statements and then executing them
	//1 by 1 multiple query execute, Connection made only once, NO TRANSACTION 
	public static String SQLInsert(String[] qry, String qno) {
	      try 
	      {
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();	     
	         for(int i=0; i<qry.length; i++)
	         {
	        	 //System.out.println("");
	        	 //System.out.println(qry[i].replace('"', ' ').trim());
	        	 stmt.executeUpdate(qry[i].replace('"', ' ').trim());
	         }
	         //con.close();
	         return ("Update/Insert success.");
	      }
	      catch(SQLException e)
	      {
	    	  System.out.println("SQL exception occured" + e);
	         return ("SQL exception occured" + e);
	      }
	}
	
	//Bulk BATCH query execute, Connection made only once, TRANSACTION TRUE 
	public static String SQLBatchInsert(String[] qry, String qno) {
	      try 
	      {
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();	     
	         //con.autoCommit(false); 
	         for(int i=1; i<= qry.length;i++){
	        	 stmt.addBatch(qry[i].replace('"', ' ').trim());
	         }
	         int[] result = stmt.executeBatch();	
	         con.commit();
	         //con.close();
	         return ("Update/Insert success." + result.length);
	      }
	      catch(SQLException e)
	      {
	         return ("SQL exception occured" + e);
	      }
	}
	
	//Bulk BATCH query execute, Connection made only once, TRANSACTION TRUE 
	public static String SQLBatchInsert(List<String> qry, String qno) {
	      try 
	      {
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();	     
	         //con.autoCommit(false); 
	         for(int i=0; i< qry.size();i++){
	        	 stmt.addBatch(qry.get(i).replace('"', ' ').trim());
	         }
	         int[] result = stmt.executeBatch();	
	         con.commit();
	         //con.close();
	         return ("Update/Insert success." + result.length);
	      }
	      catch(SQLException e)
	      {
	         return ("SQL exception occured" + e);
	      }
	}
	
	//1 by 1 multiple query execute, Connection made only once, NO TRANSACTION 
	public static String SQLInsert(List<String> qry, String qno) {
	      try 
	      {
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();	     
	         for(int i=0; i<qry.size(); i++)
	         {
		         stmt.executeUpdate(qry.get(i).replace('"', ' ').trim());		       
	         }
	         //con.close();
	         return ("Update/Insert success.");
	      }
	      catch(SQLException e)
	      {
	         return ("SQL exception occured" + e);
	      }
	}
	//1 by 1, multiple query execute , Connection made only once, Only "For()" loop changes, NO TRANSACTION 
	public static String SQLInsert2(List<String> qry, String qno) {
	      try 
	      {
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();	     
	         for(String s : qry)
	         {
		         stmt.executeUpdate(s);		       
	         }
	         //con.close();
	         return ("Update/Insert success.");
	      }
	      catch(SQLException e)
	      {
	         return ("SQL exception occured" + e);
	      }
	}

	// 1 query execute, each time function called, connection is made., NO TRANSACTION 
	public static String SQLInsert(String qry, String qno) {
	      try 
	      {
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();
	         stmt.executeUpdate(qry);
	         //con.close();
	         return ("Update/Insert success.");
	      }
	      catch(SQLException e)
	      {
	         return ("SQL exception occured" + e);
	      }
	}
	
	//BuildSQLInset => Build SQL statements then Insert
	//1 by 1 insert, Connection made once, NO TRANSACTION 
	public static String BuildSQLInsert(List<String[]> qry, String qno){
		return "Code not exist yet, Write one";
	}
	
	//BBuildSQLInset => Bulk Build SQL statements then Insert
	//BULK insert, Connection made once, TRUE TRANSACTION 
	public static String BBuildSQLInsert2(List<String[]> qry, String qno){
		return "Code not exist yet, Write one";
	}
	
	/* 
	 * //Test Query Execute code, with extra codes
	public static String SQLQuery(String[] qry, String qno) {
		try 
	      {
	    	  Class.forName("org.postgresql.Driver");
	      }
	      catch(ClassNotFoundException e) 
	      {
	         System.out.println("Class not found "+ e);
	      }
	      System.out.println("JDBC Class found");    	      
	      try 
	      { 
	    	  int no_of_rows = 0;
	         Connection con = DriverManager.getConnection 
	        		 ("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();
	         //ResultSet rs = stmt.executeQuery ("insert into public.LODTriples valuse(sub, obj, pred) ('2','3','4');");
	         for(int i=0; i<qry.length; i++)
	         {
		         ResultSet rs = stmt.executeQuery (qry[i]);
		         //To get the Rows count directly
		         //ResultSet rs = stmt.executeQuery ("select count(*) from (" + qry[i] + ") tbl");
		         while (rs.next()) 
		         {
		            no_of_rows++;
		            //  System.out.println(rs.getString(1)+" | "+rs.getString(2)+" | "+rs.getString(3));	
		         }
		         System.out.println (qry[i]+" \n There are "+ no_of_rows + " record in the table");
		         no_of_rows = 0;
	         }
	         return ("There are "+ no_of_rows + " record in the table");
	      }
	      catch(SQLException e)
	      {
	         return ("SQL exception occured" + e);
	      }		  
	}
	*/
	
	//1 Bulk query execute, Connection made only once
	public static String SQLQuery(String[] qry, String qno) {
		try 
	      {
	    	  Class.forName("org.postgresql.Driver");
	      }
	      catch(ClassNotFoundException e) 
	      {
	         System.out.println("Class not found "+ e);
	      }
	      System.out.println("JDBC Class found");    	      
	      try 
	      { 
	    	 int no_of_rows = 0;
	    	 String r = "";
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();	        
	         for(int i=0; i<qry.length; i++)
	         {
		         ResultSet rs = stmt.executeQuery (qry[i]);		        
		         while (rs.next()) 
		         {
		            no_of_rows++;
		         }
		         r = qry[i]+" \n There are "+ no_of_rows + " record in the table";
		         System.out.println (r);
		         no_of_rows = 0;
	         }
	         //con.close();
	         return (r);
	      }
	      catch(SQLException e)
	      {
	         return ("SQL exception occured" + e);
	      }		  
	}
	
	
	/*
	 * //1 by 1 multiple query execute, Connection made only once
	public static List<String> SQLQuery(List<String> qry, String qno) {
		try 
	      {
	    	  Class.forName("org.postgresql.Driver");
	      }
	      catch(ClassNotFoundException e) 
	      {
	         System.out.println("Class not found "+ e);
	      }
	      System.out.println("JDBC Class found");    	      
	      try 
	      { 
	    	 int no_of_rows = 0;
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();	        
	         for(String S : qry)
	         {
		         ResultSet rs = stmt.executeQuery(S);		        
		         while (rs.next()) 
		         {
		            no_of_rows++;		
		            
		         }
		         System.out.println (S+" \n There are "+ no_of_rows + " records in the result");
	         }
	         return qry;
	      }
	      catch(SQLException e)
	      {	    	  
	    	 return ("SQL exception occured" + e);;
	      }
	}*/
	
	//1 by 1 multiple query execute, Connection made only once
	public static List<String[]> SQLQuery(List<String[]> qry, String qno, String ResultOutPath, String DBCon1, String U1, String P1) {		      
	      try 
	      { 
	    	 int no_of_rows = 0;
	         //Connection con = DriverManager.getConnection(DBCon1, U1, P1);  
	         Statement stmt = con.createStatement();	        
	         Instant start;
	         Instant end;
	         System.out.println("No. of Qry in list = ,"+qry.size());
	         for(int i=0; i< qry.size();i++){
	           	// String S = qry.get(i)[1].toString().replace('"', ' ').trim(); // Errors in results as queryy had (") in middle
	        	 String S = qry.get(i)[1].toString().substring(1, qry.get(i)[1].length()-1).trim();//.replace("\"", "\\\"");  // Replace "" to " 
	             System.out.println("Test!,"+S);
	          	 start = Instant.now();
	           	 ResultSet rs = stmt.executeQuery(S);
	           	 while (rs.next())
		         {
		            no_of_rows++;
		         }
	         	 end = Instant.now();	         	
	         	 logSimpalQryOrInsetTimeToCSV(start, end, "Query ID,  "+qry.get(i)[0],", no. of records in the query result = ,"+ no_of_rows, ResultOutPath);
		         System.out.println(qry.get(i)[0]+", \n no. of records in the result = ,"+ no_of_rows+"Query:"+S);
		         no_of_rows=0;
	         }
	         //con.close();
	         return qry;
	      }
	      catch(SQLException e)
	      {	    
	    	  System.out.println("Error");
	    	  List<String[]> reclist = new ArrayList<>();
			  String[] rec= new String[2];
			  rec[0] = "SQL exception occured" + e;
			  reclist.add(rec);
			  System.out.println("\n Error= "+ e.getMessage() + "\n SQL Ex=" + e.getSQLState() + "\n Other = " + e.getLocalizedMessage());
	          return reclist;
	      }
	}	
	
	// 1 by 1 query execute, each time , connection is made.
	public static String SQLQuery(String qry, String qno) {
	      try 
	      {
	    	 int no_of_rows = 0;	
	    	 String r="";
	         //Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");  
	         Statement stmt = con.createStatement();
	         ResultSet rs = stmt.executeQuery (qry);	     
	         while (rs.next()) 
	         {	
	        	no_of_rows++;        	
	        	r=qry+" \n There are  line = "+ no_of_rows + "record in the table = " + rs.getString(0)+ "," + rs.getString(1) + ", Full ="+ rs.toString();	            	           
	         }
	         no_of_rows = 0;
	         System.out.println (r);	  
	         //con.close();
	         return (r);
	      }
	      catch(SQLException e)
	      {
	         return ("SQL exception occured" + e);
	      }
	}
}
