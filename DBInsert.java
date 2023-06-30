import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.opencsv.CSVWriter;

public class DBInsert {
	// public static long globalData = 0;
	public static Connection conSQL ;
	public static Connection conRaw;
	
	public static List<String[]> RMList = new ArrayList<>();
	public static boolean scale = false; // Scale means, it is stream scaling, 1M records added then on top 1M , 
									// then on top of 2M append/Load data incrementally 2M => 4M => total 8M records added in 4 Loop
	public static boolean Iscountqueries = false; //To record Result Count //AutoMatically changing True/False based on Query
	public static boolean IsExplainQuery = false; 						   //AutoMatically changing True/False based on Query
	public static boolean IsRawQuery = false; //****if this is false. Its will run query in new connection. For Raw data needs to be cached and same connectoin needed.
	
	public static boolean IsRunOnceStatic = false; //true=>Means break the execution once the required data is added in bulk once. I.E. bulkNo = 1Million, add 1M and break, Don't go further.
						//IsRunOnceStatic = false; //False=> Stream data insert allowed, Query after every 1M.
	public static boolean RUNPgRAW = true;
	public static boolean RUNPgSQL = true;
	public static boolean IsValuesOnlyDataset = true;

	public static int runQueriesatAfterNoofrecords = 500000;
	public static boolean IsrunQueriesatAfterNoofrecords = true;
	public static String ResultOutPath = "";
	public static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"); // for data, as per DBMS DateTime format
	public static DateTimeFormatter File_dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss"); // special file format for file_names (without ":") so these files can easily open in windows OS

	public static LocalDateTime now = LocalDateTime.now();
	public static int QueryTimeOutSec = 120;

	// New Attributes for Results output CSV. or Raw dataset
	public static LocalDateTime Exp_startDateTime = LocalDateTime.now();
	public static String DBMS_Type = "NA"; // when loading starts change to PgRAW or PgSQL
	public static String Data_Op = "NA"; // DLT or QET
	public static String Data_Op_ID = "NA"; // QueryID or BulkNo
	public static int Records_In_DB = 0; // Total No. of records added into DBMS/CSV File. to identify Queries running at different scale.

	// dtf.format(now) to -===>>>> dtf.format(LocalDateTime.now()) //so we get the	// current time every time.

	//RM Variables
	public static int ThreadsCount=0;
	public static int MaxThreads=4;

	public static float RM_AvailCPU=0;
	public static float RM_IOWait=0;
	public static float RM_AvailMem=0;

	public static float Min_AvailCPU=-20;
	public static float Min_IOWait=50;    // Max wait, if more stop...
	public static float Min_AvailMem=-10;
	public static boolean Is_SET_WM = true;  //Automatically assign maximum RAM to complex queries based on RM availability... 
	public static int CPU_Cores = Runtime.getRuntime().availableProcessors();
	public static double RMFreq = 1;
	public static boolean runStatThreads = true; //True=Run Resource Monitoring Threads



	public static void main(String[] args) {
		try {

			int lines = 0;
			boolean append = true;
			int bulkNo =1000000;    // bulkNo = 2000000; // it fails to load more than 5M records in bulk, so error after this.
			int StaticDataScale = 1000000; //when IsRunOnceStatic = true. Static increase Bulk No
			boolean runQueries = true; // Run Queries in between of bulk save //false=>Run once at end... true=>run after every bulk insert
			boolean isSQLDataset = false;// added 6feb 2019, take same dataset file
			
			String DBCon1 = "jdbc:postgresql://localhost:5432/om";
			String U1 = "om";
			String P1 = "om";
			String DBCon2 = "jdbc:postgresql://localhost:5432/om"; // mayank
			String U2 = "om"; // mayank
			String P2 = "om"; // postgres

			String ExpQuerydesc = "With Io stats";
// Datasets
			String fileInpath = "/home/om/workspace/DBRAW1/Datasets/LODTriplesPlainK_10M.txt";
			String SQLfileInpath = "/home/om/workspace/DBRAW1/Datasets/LODTriplesPlainK_10M.txt";
// Queries
			// DBMS LODTriples
			String SQLQueryPath = "/home/om/workspace/DBRAW1/queries/LODTriplesQueries.csv";
			// RAW LODTriples2
			String SQLQueryPath2 = "/home/om/workspace/DBRAW1/queries/LODTriples2Queries.csv";
// PostgresRAW - NoDB Linked File	
			// Now RAW DB file table is changed to LODTriples2. from 31-oct-2018
			String NODBfileOutPath = "/home/om/workspace/DBRAW1/NODB/LODTriples2.csv";
			
			NODBfileOutPath = "/mnt/ram_space/nodb/LODTriples2.csv";

			
//Start 						
			System.out.print("Hello world");
			System.out.println(dtf.format(LocalDateTime.now())); // 2016/11/16 12:08:43
			//DBInsert.con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");
			
			//Removed 28-nov-2020, because I think, QET       is not running due tosingle connectio is made
	//		DBInsert.conRaw = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om");

	//		DBInsert.conSQL = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");

			
			
			// ThreadSubclass thread1 = new ThreadSubclass();
			ThreadSubClassIO thread1 = new ThreadSubClassIO();
			thread1.IoThreadParam("CPU & RAM", "top -i -b -d "+RMFreq+" -H -o +%MEM -p 9999");
			//Working but showing results after 30-60seconds  -
			//thread1.IoThreadParam("CPU & RAM", "(top -i -b -d "+RMFreq+" -H | grep '^%Cpu' & top -i -b -d "+RMFreq+" -H | grep 'KiB Mem')");
			//thread1.IoThreadParam("CPU & RAM", "top -i -b -d "+RMFreq+" -H | grep '^%Cpu'");// | grep %Cpu
			
		//	 ThreadSubClassIO thread2 = new ThreadSubClassIO();
			// thread2.IoThreadParam("IO_Speed", "sudo iotop -b -o -d 1");
			// thread2.IoThreadParam("IO_postgres", "sudo iotop -b -o -d 1 -a | grep postgres"); //sudo iotop -b -o -d 1 -a

			ThreadSubClassIO thread3 = new ThreadSubClassIO();
			// thread3.IoThreadParam("IO_Total", "/home/om/workspace/DBRAW1/src/./t1.sh");
			thread3.IoThreadParam("IO_Total", "sudo iotop -b -o -d "+RMFreq+" -a -k -P -p 9999"); //sudo iotop -b -o -d 1 -a -k // sudo iotop -b -o -d 1 -a | grep 'Total DISK' (issies in grep)


			//Added 25-Nov-2020
	/*		ThreadRawQuery RawThread = new ThreadRawQuery();
			RawThread.QueryThread(true, "");
			
			//Added around 7-Aug-2021 // For BASCI CPU Maximization Exp.
			ThreadRawQuery SQLThread2 = new ThreadRawQuery();
			SQLThread2.QueryThread(false, "");
			ThreadRawQuery SQLThread3 = new ThreadRawQuery();
			SQLThread3.QueryThread(false, "");
			ThreadRawQuery SQLThread4 = new ThreadRawQuery();
			SQLThread4.QueryThread(false, "");
			ThreadRawQuery SQLThread5 = new ThreadRawQuery();
			SQLThread5.QueryThread(false, "");
			ThreadRawQuery SQLThread6 = new ThreadRawQuery();
			SQLThread6.QueryThread(false, "");
			ThreadRawQuery SQLThread7 = new ThreadRawQuery();
			SQLThread7.QueryThread(false, "");
			ThreadRawQuery SQLThread8 = new ThreadRawQuery();
			SQLThread8.QueryThread(false, "");
			ThreadRawQuery SQLThread9 = new ThreadRawQuery();
			SQLThread9.QueryThread(false, "");
			ThreadRawQuery SQLThread10 = new ThreadRawQuery();
			SQLThread10.QueryThread(false, "");
			ThreadRawQuery SQLThread11 = new ThreadRawQuery();
			SQLThread11.QueryThread(false, "");
		*/	
			ResultOutPath = "/home/om/workspace/DBRAW1/results/result" + File_dtf.format(LocalDateTime.now()) + ".csv";

			if (IsValuesOnlyDataset == true) {
				fileInpath = "/home/om/workspace/DBRAW1/Datasets/LODValues/LODTriplesPlainK_10M_V_SQL.txt";  // Used same variable/path for Prep. SQL Loading Method //Added 1-Dec-2020
				if(isSQLDataset == false)
					fileInpath = "/home/om/workspace/DBRAW1/Datasets/LODValues/LODTriplesPlainK_10M_V.txt"; 
				SQLfileInpath = "/home/om/workspace/DBRAW1/Datasets/LODValues/LODTriplesPlainK_10M_V.txt";
				SQLfileInpath = "/home/om/workspace/DBRAW1/Datasets/LODValues/LODTriplesPlainK_10M_V_SQL.txt";  // Used same variable/path for Prep. SQL Loading Method //Added 1-Dec-2020
//for bulk load
			//	SQLQueryPath = "/home/om/workspace/DBRAW1/Datasets/LODValues/Q2.csv"; // PgRAW
				SQLQueryPath = "/home/om/workspace/DBRAW1/Datasets/LODValues/Q.csv"; // PgRAW

				System.out.print("Hello world2");

				SQLQueryPath2 = "/home/om/workspace/DBRAW1/Datasets/LODValues/Q.csv"; // PgSQL
				
			}
			
			boolean SDSSDataset = false;
			if(SDSSDataset == true)
			{
				//RAW PARTITIONING Queries - 11-Oct-2021
				//SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/PART/QP2.csv";  //PgRAW+PgSQL
				//SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/PART/QP3.csv";  //PgRAW+PgSQL
				SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/PART/QP4.csv";  //PgRAW+PgSQL

				//Full
				SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_Q2.csv"; // PgRAW

				//SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_Q.csv"; // PgSQL
				SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_P1_Q.csv"; // PgSQL
				//SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_P1_Q2.csv"; // PgRAW
				
				//For QTA Experiments
				SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_Q2.csv"; // PgRAW
				SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_P1_Q.csv"; // PgSQL

					
			}

			System.out.println("2.3GB + K File size LODTriples, Dataset used, 10000000 10M records" + SQLQueryPath
					+ ",\n" + ExpQuerydesc);
			writeCSV(ResultOutPath, "2.3GB + K  File size LODTriples, Dataset used, 10000000 10M records" + SQLQueryPath
					+ ",\n" + ExpQuerydesc, true);

 			// Delete the data in CSV file
		//	delCSVFile(NODBfileOutPath);

			writeCSV(ResultOutPath, "insert 2nd run", true);

	
			// for (int SD=1; SD<11; SD ++)
			{
				// Run multiple times same experiment for multiple static data scaling results	// ...

				if (IsRunOnceStatic == true) {
					// bulkNo=bulkNo+StaticDataScale; //When Run IsRunOnceStatic = true, bulkNo
					// becomes total no. of records
				}

				System.out.println("Ruuning Exp for: , " + bulkNo);
				Data_Op = "Thread.sleep";
				Data_Op_ID = "500";
				Thread.sleep(500);
				Data_Op = "NA";
				Data_Op_ID = "NA";

				// 1
				ResultOutPath = "/home/om/workspace/DBRAW1/results/result" + File_dtf.format(LocalDateTime.now())
						+ "CountQueries" + bulkNo + ".csv";
				// bulkNo=1000000;
				// IsrunQueriesatAfterNoofrecords = true; //if false . it runs query for all the
				// data in end.
				// runQueriesatAfterNoofrecords = 1000000;

				if (runStatThreads == true) {
					thread1.start();
					// thread2.start();
					thread3.start();
				
				//	RawThread.start();

			/*		SQLThread2.start();
					SQLThread3.start();

					SQLThread4.start();
					SQLThread5.start();
					SQLThread6.start();
					SQLThread7.start();

					SQLThread8.start();
					SQLThread9.start();
					SQLThread10.start();
					SQLThread11.start();
*/


				}

				
				//Thread.sleep(2000000);
				exp3SQL(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2,
						U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries, isSQLDataset);
			//	exp3Raw(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2,
			//			U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries, isSQLDataset);

				
				//Thread.sleep(150500);

				// for TPC_H

				// at TPC_H, Scale Factor_1...
				SQLQueryPath = "/home/om/workspace/DBRAW1/queries/Final_TPC_H_Queries/TPC_H_ALL2.csv"; // Raw SQL
																										// queries
				// 2 for RAW but variable is 1
				SQLQueryPath2 = "/home/om/workspace/DBRAW1/queries/Final_TPC_H_Queries/TPC_H_ALL.csv"; // DB SQL queries

				// exp4(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines,
				// append, DBCon1, U1, P1, DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo,
				// runQueries, isSQLDataset);

				if (runStatThreads == true) {
					System.out.println("Back to main program");
					Data_Op = "Thread.sleep";
					Data_Op_ID = "500";
					Thread.sleep(500);

					thread1.interrupt();
					// thread2.interrupt();
					thread3.interrupt();
				//	RawThread.interrupt();
				//	SQLThread2.interrupt();

					System.out.println("Back to main program2 from IO2");
					System.out.println("Back to main program3");
				}
				// exp3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines,
				// append, DBCon1, U1, P1, DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo,
				// runQueries, isSQLDataset);
				// System.gc();

			}
			System.exit(0);

		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage() + ",\n" + e.getMessage());
			return;
		}
	}




	public static String exp3Raw(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,
			int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2,
			String SQLQueryPath, String SQLQueryPath2, int bulkNo, boolean runQueries, boolean isSQLDataset) {
		String resultscount = "";
		Instant start = Instant.now();
		Instant end = Instant.now();
		String exp = "";

		if (RUNPgRAW == true) {
			// CSV(insert) RAW Start, NODB, PostgresRAW (Queries only)
			DBMS_Type = "PgRAW";
			System.out.println("at RUNPgRAW");

			try {
				Data_Op = "Thread.sleep";
				Data_Op_ID = "500";
				Thread.sleep(500);
				Data_Op = "System.gc";
				System.gc();
				Data_Op = "Thread.sleep";
				Data_Op_ID = "500";
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			append = true;
			writeCSV(ResultOutPath, "RAW + SQL Exp3+ START with Queries RAW, " + dtf.format(LocalDateTime.now()),
					append);
			exp = "Exp3.1, Basic GET Data from CSV check," + dtf.format(LocalDateTime.now());
			start = Instant.now();
			getInsertStrfromCSV3(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1,
					P1, DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries, isSQLDataset);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			System.out.println("Got data from CSV");
			System.gc();

			/*
			 * Not needed in this
			 *
			 * exp = "Exp3.2, -Save-x- -x-Errors "BUT It inserts data
			 * again" during Bulk Insert Data To CSV check"; start = Instant.now();
			 * writeCSVB3(ResultOutPath, rdata, append); end = Instant.now();
			 * logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			 *
			 */

			/*
			 * Not needed in this
			 *
			 * exp = "Exp3.2, Bulk Insert Data To CSV check"; append = false; start =
			 * Instant.now(); writeCSVB3(NODBfileOutPath, rdata, append); end =
			 * Instant.now(); logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath,
			 * ResultOutPath); append = true;
			 *
			 *
			 */

	/*		Data_Op = "QET";
			Data_Op_ID = "Q00_Count_extra";
			start = Instant.now();
			resultscount = SQLQuery("Select count(*) from LODTriples2", "0", true);
			end = Instant.now();
			logTimetoCSV(start, end, "Exp3.3, Count Check PostgresRAW," + dtf.format(LocalDateTime.now()), resultscount,
					"Select count(*) from LODTriples2", ResultOutPath);
*/
			// SQL Query GET
			exp = "Exp3.4, GET SQL Queries List from CSV Time," + dtf.format(LocalDateTime.now());
			start = Instant.now();
			List<String[]> Qdata = getQryStrfromCSV2(SQLQueryPath, 0);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			// SQL Query Write/Log
			exp = "Total Time for All SQL Queries to RUN & Save To CSV check," + dtf.format(LocalDateTime.now());
			start = Instant.now();
			List<String[]> r2data = SQLQuery(Qdata, "All", ResultOutPath, DBCon1, U1, P1, true);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			// SQL Query Results Save to CSV
			// exp = "Exp3.5.1, Bulk Insert Query Results Data To CSV check [Copy of the
			// Data but with Instant start & Instant end data]";
			exp = "Exp3.5.1, List of Queries used in above exp.";
			start = Instant.now();
			writeCSVB3(ResultOutPath, r2data, append); // Append
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			Data_Op = "QET";
			Data_Op_ID = "Q000_Count_distinct";
			start = Instant.now();
			resultscount = SQLQuery("Select count(distinct(sub, obj, pred)) from LODTriples2", "0", true);
			start = Instant.now();
			logTimetoCSV(start, end, "Exp3.6.0, After query count(distinct(sub, obj, pred)) Check PostgreSQL",
					resultscount, "Select count(distinct(sub, obj, pred)) from LODTriples2", ResultOutPath);

			Data_Op = "DEL";
			Data_Op_ID = "CSV_DEL";
			// Delete Data
			start = Instant.now();
			resultscount = delCSVFile(NODBfileOutPath);
			end = Instant.now();

			logTimetoCSV(start, end, "Exp3.6.1, Delete file CSV & Create New", resultscount,
					"delCSVFile(NODBfileOutPath) = " + NODBfileOutPath, ResultOutPath);
			Data_Op = "QET";
			Data_Op_ID = "Q0_End_Count_extra";
			resultscount = SQLQuery("Select count(*) from LODTriples2", "0", true);
			logTimetoCSV(start, end, "Exp3.7, After Delete Count Check PostgreRAW", resultscount,
					"Select count(*) from LODTriples2", ResultOutPath);

			writeCSV(ResultOutPath, "RAW + SQL3+ EXP END. ", append);

		}
		return "";
	}
	
	public static String exp3SQL(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,
			int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2,
			String SQLQueryPath, String SQLQueryPath2, int bulkNo, boolean runQueries, boolean isSQLDataset) {
		String resultscount = "";
		Instant start = Instant.now();
		Instant end = Instant.now();
		String exp = "";
		
		if (RUNPgSQL == true) {

			DBMS_Type = "PgSQL";
			Data_Op = "NA";
			Data_Op_ID = "NA";

			try {
				Data_Op = "Thread.sleep";
				Data_Op_ID = "500";
				Thread.sleep(500);
				Data_Op = "System.gc";
				System.gc();
				Data_Op = "Thread.sleep";
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Data_Op = "NA";
			// DBMS start, PostgresSQL **********************************************
			writeCSV(ResultOutPath, "DBMS + SQL3.8+ EXP Start... ", append);

			exp = "Exp3.8, Basic GET SQL insert statements Data from CSV check ";
			start = Instant.now();
			
			//Prep SQL, Single SQL line for multple record insertion
		//	List<String> SQLqrydata = getInsert_PREP_SqlLstFromCSV(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath,
		//			lines, append, DBCon1, U1, P1, DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
		
			//	For Bulk Insert SQL, Multiple SQL Insert lines
		//	List<String> SQLqrydata = getInsertSqlLstFromCSV(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath,
		//			lines, append, DBCon1, U1, P1, DBCon2, U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
		
			
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			exp = "Exp3.9  Errors listed above, Total time to Save errors came during the insert,";
			start = Instant.now();
		//	writeCSV(ResultOutPath, SQLqrydata, append);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			/*
			 * Not needed in this
			 *
			 * exp = "Exp3.9, Bulk Insert Data To PostgresSQL check"; start = Instant.now();
			 * SQLBatchInsert(SQLqrydata, "0"); end = Instant.now(); logTimetoCSV(start,
			 * end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			 *
			 *
			 */
		/*	Data_Op = "QET";
			Data_Op_ID = "Q00_Count_extra";
			start = Instant.now();
			resultscount = SQLQuery("Select count(*) from LODTriples", "0", false);
			end = Instant.now();
			logTimetoCSV(start, end, "Exp3.10, Count Check PostgreSQL", resultscount, "Select count(*) from LODTriples",
					ResultOutPath);
*/
			// SQL Query GET
			exp = "Exp3.11, GET SQL Queries List from CSV Time";
			start = Instant.now();
			List<String[]> Q2data = getQryStrfromCSV2(SQLQueryPath2, 0);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			// SQL Query RUN
			exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
			start = Instant.now();
			List<String[]> r3data = SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1, IsRawQuery);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

		/*	Data_Op = "QET";
			Data_Op_ID = "Q000_Count_distinct";
			start = Instant.now();
			resultscount = SQLQuery("Select count(distinct(sub, obj, pred)) from LODTriples", "0");
			end = Instant.now();
			logTimetoCSV(start, end, "Exp3.12.0, After query count(distinct(sub, obj, pred)) Check PostgreSQL",
					resultscount, "Select count(distinct(sub, obj, pred)) from LODTriples", ResultOutPath);

			// SQL Query Results Save to CSV
			exp = "Exp3.12.1, Bulk Insert Query Results Data To CSV check [Copy of the Data but with Instant start & Instant end data]";
			start = Instant.now();
			writeCSVB3(ResultOutPath, r3data, append); // Append
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
*/
		/*	Data_Op = "DEL";
			Data_Op_ID = "TRUNCATE";
			// Delete Data
			start = Instant.now();
			resultscount = SQLQuery("TRUNCATE TABLE LODTriples", "0", false);
			end = Instant.now();
			logTimetoCSV(start, end, "Exp3.13, Delete table data PGSQL", resultscount, "TRUNCATE TABLE LODTriples",
					ResultOutPath);

			Data_Op = "QET";
			Data_Op_ID = "Q0_End_Count_extra";
			resultscount = SQLQuery("Select count(*) from LODTriples", "0", false);
			logTimetoCSV(start, end, "Exp3.14, After Delete Count Check PostgreSQL", resultscount,
					"Select count(*) from LODTriples", ResultOutPath);
*/
			writeCSV(ResultOutPath, "DBMS + SQL 3.8+ Exp END ", append);
			System.out.println("Exp Ended....");

			DBMS_Type = "Exp Ended...";
			Data_Op = "NA";
			Data_Op_ID = "NA";
		}
		return "";
	}

	public static String exp4(String fileInpath, String SQLfileInpath, String NODBfileOutPath, String ResultOutPath,
			int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2, String U2, String P2,
			String SQLQueryPath, String SQLQueryPath2, int bulkNo, boolean runQueries, boolean isSQLDataset) {
		String resultscount = "";
		Instant start = Instant.now();
		Instant end = Instant.now();
		String exp = "";

		if (RUNPgRAW == true) {
			// CSV(insert) RAW Start, NODB, PostgresRAW (Queries only)
			System.out.println("at RUNPgRAW");
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			append = true;
			writeCSV(ResultOutPath, "RAW + SQL Exp3+ START with Queries RAW, " + dtf.format(LocalDateTime.now()),
					append);
			exp = "Exp3.1, Basic GET Data from CSV check," + dtf.format(LocalDateTime.now());
			start = Instant.now();
			// List<String[]> rdata = getInsertStrfromCSV3(fileInpath, SQLfileInpath,
			// NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2, U2,
			// P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries, isSQLDataset);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			System.out.println("Got data from CSV");
			System.gc();

			/*
			 * Not needed in this
			 *
			 * exp = "Exp3.2, -Save-x- -x-Errors "BUT It inserts data
			 * again" during Bulk Insert Data To CSV check"; start = Instant.now();
			 * writeCSVB3(ResultOutPath, rdata, append); end = Instant.now();
			 * logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			 *
			 */

			/*
			 * Not needed in this
			 *
			 * exp = "Exp3.2, Bulk Insert Data To CSV check"; append = false; start =
			 * Instant.now(); writeCSVB3(NODBfileOutPath, rdata, append); end =
			 * Instant.now(); logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath,
			 * ResultOutPath); append = true;
			 *
			 *
			 */

			start = Instant.now();
			resultscount = SQLQuery("Select count(*) from lineitem2", "0", true);
			end = Instant.now();
			logTimetoCSV(start, end, "Exp3.3, Count Check PostgresRAW," + dtf.format(LocalDateTime.now()), resultscount,
					"Select count(*) from lineitem2", ResultOutPath);

			// SQL Query GET
			exp = "Exp3.4, GET SQL Queries List from CSV Time," + dtf.format(LocalDateTime.now());
			start = Instant.now();
			List<String[]> Qdata = getQryStrfromCSV2(SQLQueryPath, 0);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			// SQL Query Write/Log
			exp = "Total Time for All SQL Queries to RUN & Save To CSV check," + dtf.format(LocalDateTime.now());
			start = Instant.now();
			List<String[]> r2data = SQLQuery(Qdata, "All", ResultOutPath, DBCon1, U1, P1, true);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			// SQL Query Results Save to CSV
			// exp = "Exp3.5.1, Bulk Insert Query Results Data To CSV check [Copy of the
			// Data but with Instant start & Instant end data]";
			exp = "Exp3.5.1, List of Queries used in above exp.";
			start = Instant.now();
			writeCSVB3(ResultOutPath, r2data, append); // Append
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			start = Instant.now();
			resultscount = SQLQuery("Select count(*) from lineitem2", "0", true);
			start = Instant.now();
			logTimetoCSV(start, end, "Exp3.6.0, After query Select count(*) from lineitem2 Check PostgresRAW",
					resultscount, "Select count(*) from lineitem2", ResultOutPath);

			/*
			 * //Delete Data start = Instant.now(); //resultscount =
			 * delCSVFile(NODBfileOutPath); end = Instant.now();
			 * 
			 * logTimetoCSV(start, end, "Exp3.6.1, Delete file CSV & Create New",
			 * resultscount, "delCSVFile(NODBfileOutPath) = " + NODBfileOutPath,
			 * ResultOutPath);
			 * 
			 * resultscount = SQLQuery("Select count(*) from LODTriples2", "0");
			 * logTimetoCSV(start, end, "Exp3.7, After Delete Count Check PostgreRAW",
			 * resultscount, "Select count(*) from LODTriples2", ResultOutPath);
			 */
			writeCSV(ResultOutPath, "RAW + SQL3+ EXP END. ", append);

		}

		if (RUNPgSQL == true) {
			// DBMS start, PostgresSQL **********************************************
			writeCSV(ResultOutPath, "DBMS + SQL3.8+ EXP Start... ", append);

			exp = "Exp3.8, Basic GET SQL insert statements Data from CSV check ";
			start = Instant.now();
			// List<String> SQLqrydata = getInsertSqlLstFromCSV(fileInpath, SQLfileInpath,
			// NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2, U2,
			// P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			exp = "Exp3.9  Errors listed above, Total time to Save errors came during the insert,";
			start = Instant.now();
			// writeCSV(ResultOutPath, SQLqrydata, append);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			/*
			 * Not needed in this
			 *
			 * exp = "Exp3.9, Bulk Insert Data To PostgresSQL check"; start = Instant.now();
			 * SQLBatchInsert(SQLqrydata, "0"); end = Instant.now(); logTimetoCSV(start,
			 * end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			 *
			 *
			 */

			start = Instant.now();
			resultscount = SQLQuery("Select count(*) from lineitem", "0", false);
			end = Instant.now();
			logTimetoCSV(start, end, "Exp3.10, Count Check PostgreSQL", resultscount, "Select count(*) from lineitem",
					ResultOutPath);

			// SQL Query GET
			exp = "Exp3.11, GET SQL Queries List from CSV Time";
			start = Instant.now();
			List<String[]> Q2data = getQryStrfromCSV2(SQLQueryPath2, 0);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			// SQL Query RUN
			exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
			start = Instant.now();
			List<String[]> r3data = SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1, false);
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			resultscount = SQLQuery("Select count(*) from lineitem", "0", false);
			logTimetoCSV(start, end, "Exp3.12.0, After query Select count(*) from lineitem Check PostgreSQL",
					resultscount, "Select count(*) from lineitem", ResultOutPath);

			// SQL Query Results Save to CSV
			exp = "Exp3.12.1, Bulk Insert Query Results Data To CSV check [Copy of the Data but with Instant start & Instant end data]";
			start = Instant.now();
			writeCSVB3(ResultOutPath, r3data, append); // Append
			end = Instant.now();
			logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			/*
			 * //Delete Data start = Instant.now(); resultscount =
			 * SQLQuery("TRUNCATE TABLE LODTriples", "0"); end = Instant.now();
			 * logTimetoCSV(start, end, "Exp3.13, Delete table data PGSQL", resultscount,
			 * "TRUNCATE TABLE LODTriples", ResultOutPath);
			 * 
			 * resultscount = SQLQuery("Select count(*) from LODTriples", "0");
			 * logTimetoCSV(start, end, "Exp3.14, After Delete Count Check PostgreSQL",
			 * resultscount, "Select count(*) from LODTriples", ResultOutPath);
			 * 
			 * writeCSV(ResultOutPath, "DBMS + SQL 3.8+ Exp END ", append);
			 */

			System.out.println("Exp Ended....");
		}
		return "";
	}

	// Get data from CSV in String Array
	public static String[] getStrfromCSV(String filepath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = null;
			int i = 0;
			String[] sqllist = new String[6000000];
			while ((line = reader.readLine()) != null) {
				i = 0;
				sqllist[i] = line;
				i++;
			}
			reader.close();
			return sqllist;
		} catch (IOException e) {
			String[] sqllist = new String[2];
			sqllist[0] = "SQL exception occured" + e;
			return sqllist;
		}
	}

	// Get data from CSV in List String
	public static List<String> getLstfromCSV(String filepath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = null;
			List<String> sqllist = new ArrayList<>();
			while ((line = reader.readLine()) != null) {
				sqllist.add(line);
			}
			reader.close();
			return sqllist;
		} catch (IOException e) {
			List<String> sqllist = new ArrayList<>();
			sqllist.add("SQL exception occured" + e);
			return sqllist;
		}
	}

	// Get data from CSV in List String  //READ SQL Insert lines & Bulk Insert
	public static List<String> getInsertSqlLstFromCSV(String fileInpath, String SQLfileInpath, String NODBfileOutPath,
			String ResultOutPath, int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2,
			String U2, String P2, String SQLQueryPath, String SQLQueryPath2, int bulkNo, boolean runQueries) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(SQLfileInpath));
			String line = null;
			String exp = "";
			List<String> sqllist = new ArrayList<>();
			List<String[]> Q2data = new ArrayList<>();
			Instant start;
			Instant end;
			int i = 0;
			int scaletime = 0;
			Data_Op = "DLT";
			Data_Op_ID = bulkNo + "";

			if (runQueries == true) {
				// SQL Query GET
				exp = "Exp3.11, GET SQL Queries List from CSV Time";
				start = Instant.now();
				Q2data = getQryStrfromCSV2(SQLQueryPath2, 0);
				end = Instant.now();
				logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			}
			while ((line = reader.readLine()) != null) {
				sqllist.add(line);
				i++;
				if (i == bulkNo) {
					System.out.println("Intermediate record insert bulkNo=" + bulkNo);
					start = Instant.now();
					// System.out.println("sqllist =" + sqllist.get(1).toString()+"\n sqllist count
					// 9,99,999= " + sqllist.get(999999));
					SQLBatchInsert(sqllist, "0");
					end = Instant.now();
					logSimpalQryOrInsetTimeToCSV(start, end, "insert," + i + ",", "-", ResultOutPath);
					sqllist.clear();

					Records_In_DB = Records_In_DB + i;
					i = 0;

					// to scale the records, //1lakh->1+1->2 lakh = 2+2 =4lakh
					if (scale == true && scaletime != 0) {
						bulkNo = bulkNo + bulkNo;
					}
					scaletime++;
					if (runQueries == true) {
						Data_Op = "QET";
						Data_Op_ID = "Q0_Count_extra";

						if ((IsrunQueriesatAfterNoofrecords == false) || (IsrunQueriesatAfterNoofrecords == true
								&& (scaletime * bulkNo) >= runQueriesatAfterNoofrecords)) {
							System.out.println("\n PgSQL runQueriesatAfterNoofrecords = ,"
									+ runQueriesatAfterNoofrecords + ",Scale = " + scaletime + ",Bulk = ," + bulkNo);

							start = Instant.now();
							String rcPgSQL = SQLQuery("Select count(*) from LODTriples", "0", false);
							end = Instant.now();
							logTimetoCSV(start, end, "Exp3.10, Count Check PostgreSQL", rcPgSQL,
									"Select count(*) from LODTriples", ResultOutPath);

							// SQL Query RUN
							exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
							start = Instant.now();
							SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1, false);
							end = Instant.now();
							logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
						}
						Data_Op = "DLT";
						Data_Op_ID = bulkNo + "";
					}
					if (IsRunOnceStatic == true) {
						System.out.print("break");
						break;
					}
				}

			}
			reader.close();

			//start = Instant.now();
			System.out.println("Not Inserting Last remaining records insert bulkNo= " + bulkNo);
		//	SQLBatchInsert(sqllist, "0");
			//end = Instant.now();
			//logSimpalQryOrInsetTimeToCSV(start, end, "insert," + i + ",", "-", ResultOutPath);
			sqllist.clear();

			Records_In_DB = Records_In_DB + i;
			i = 0;

			if (runQueries == true) {
				Data_Op = "QET";
				Data_Op_ID = "Q0_Count_extra";
				// SQL Query RUN
				exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
				start = Instant.now();
				SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1, false);
				end = Instant.now();
				logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
			}
			return sqllist;
		} catch (IOException e) {
			System.out.println("Errors in bulk, getInsertSqlLstFromCSV =" + e.getMessage());
			List<String> sqllist = new ArrayList<>();
			sqllist.add("SQL exception occured" + e);
			return sqllist;
		}
	}
	
	// Get data from CSV in List String  //READ String VALUES & Create single Prep. SQL Insert statement of 1 lines & insert
		public static List<String> getInsert_PREP_SqlLstFromCSV(String fileInpath, String SQLfileInpath, String NODBfileOutPath,
				String ResultOutPath, int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2,
				String U2, String P2, String SQLQueryPath, String SQLQueryPath2, int bulkNo, boolean runQueries) {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(SQLfileInpath));
				String line = null;
				String exp = "";
				List<String> prep_sqllist = new ArrayList<>();
				List<String[]> Q2data = new ArrayList<>();
				Instant start;
				Instant end;
				int i = 0;
				int scaletime = 0;
				Data_Op = "DLT";
				Data_Op_ID = bulkNo + "";

				if (runQueries == true) {
					// SQL Query GET
					exp = "Exp3.11, GET SQL Queries List from CSV Time";
					start = Instant.now();
					Q2data = getQryStrfromCSV2(SQLQueryPath2, 0);
					end = Instant.now();
					logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
				}
				while ((line = reader.readLine()) != null) {
					line = "('" + line.trim().replace(",", "','").replace(", ", "','")+ "')";
					prep_sqllist.add(line);
					i++;
					if (i == bulkNo) {
						System.out.println("Intermediate record insert USING PREPSQL bulkNo=" + bulkNo);
						start = Instant.now();
						// System.out.println("sqllist =" + sqllist.get(1).toString()+"\n sqllist count
						// 9,99,999= " + sqllist.get(999999));
						//SQLBatchInsert(sqllist, "0");
						String PrepSQL = "INSERT INTO LODTriples (sub, pred, obj) VALUES " + String.join(",", prep_sqllist)+";";
					//	System.out.println("\n"+PrepSQL + "\n");
			//	c		
						SQLInsert(PrepSQL, "Insert Prep SQL");
					
						//creatting PrepSQL CSV single line insert statement file
							
					/*	writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "TURNCATE,\"TRUNCATE LODTriples;\"", append);
						
						writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "Insert_Prep,\""+PrepSQL+"\"", append);
						
						writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "Q0_C,\"select count(*) from LODTriples;\"", append);
						writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "Q1_C,\"select count(*) from LODTriples L1, LODTriples L2, LODTriples L3 where L1.pred like 'sensor-observation.owl#hasLocation' and L1.sub like 'LocatedNearRel4UT01' and L2.pred like 'sensor-observation.owl#floatValue' and L2.obj like '6.0' and L3.pred like 'sensor-observation.owl#uom' and L3.obj like 'weather.owl#fahrenheit' and L2.sub=L3.sub;\"", append);
						writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "Q1_C_H1,\"select count(*) from LODTriples L1, LODTriples L2, LODTriples L3 where L1.pred like 'sensor-observation.owl#hasLocation' and L1.sub like 'LocatedNearRel4UT01' and L2.pred like 'sensor-observation.owl#floatValue' and L2.obj like '6.0' and L3.pred like 'sensor-observation.owl#uom' and L3.obj like 'weather.owl#fahrenheit' and L2.sub=L3.sub;\"", append);
						writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "Q1_C_H2,\"select count(*) from LODTriples L1, LODTriples L2, LODTriples L3 where L1.pred like 'sensor-observation.owl#hasLocation' and L1.sub like 'LocatedNearRel4UT01' and L2.pred like 'sensor-observation.owl#floatValue' and L2.obj like '6.0' and L3.pred like 'sensor-observation.owl#uom' and L3.obj like 'weather.owl#fahrenheit' and L2.sub=L3.sub;\"", append);
						writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "Q1_C_H3,\"select count(*) from LODTriples L1, LODTriples L2, LODTriples L3 where L1.pred like 'sensor-observation.owl#hasLocation' and L1.sub like 'LocatedNearRel4UT01' and L2.pred like 'sensor-observation.owl#floatValue' and L2.obj like '6.0' and L3.pred like 'sensor-observation.owl#uom' and L3.obj like 'weather.owl#fahrenheit' and L2.sub=L3.sub;\"", append);
						writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "TURNCATE,\"TRUNCATE LODTriples;\"", append);
						writeCSV("/home/om/workspace/DBRAW1/Datasets/LODValues/2M_K_V_Prep_SQL.csv", "Q0_C_e,\"select count(*) from LODTriples;\"", append);

						*/

						end = Instant.now();
						logSimpalQryOrInsetTimeToCSV(start, end, "insert," + i + ",", "-", ResultOutPath);
						prep_sqllist.clear();

						Records_In_DB = Records_In_DB + i;
						i = 0;

						// to scale the records, //1lakh->1+1->2 lakh = 2+2 =4lakh
						if (scale == true && scaletime != 0) {
							bulkNo = bulkNo + bulkNo;
						}
						scaletime++;
						if (runQueries == true) {
							Data_Op = "QET";
							Data_Op_ID = "Q0_Count_extra";

							if ((IsrunQueriesatAfterNoofrecords == false) || (IsrunQueriesatAfterNoofrecords == true
									&& (scaletime * bulkNo) >= runQueriesatAfterNoofrecords)) {
								System.out.println("\n PgSQL runQueriesatAfterNoofrecords = ,"
										+ runQueriesatAfterNoofrecords + ",Scale = " + scaletime + ",Bulk = ," + bulkNo);

								start = Instant.now();
								String rcPgSQL = SQLQuery("Select count(*) from LODTriples", "0", false);
								end = Instant.now();
								logTimetoCSV(start, end, "Exp3.10, Count Check PostgreSQL", rcPgSQL,
										"Select count(*) from LODTriples", ResultOutPath);

								// SQL Query RUN
								exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
								start = Instant.now();
								SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1, false);
								end = Instant.now();
								logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
							}
							Data_Op = "DLT";
							Data_Op_ID = bulkNo + "";
						}
						if (IsRunOnceStatic == true) {
							System.out.print("break");
							break;
						}
					}

				}
				reader.close();

				//start = Instant.now();
				System.out.println("Not Inserting Last remaining records insert bulkNo= " + bulkNo);
			//	SQLBatchInsert(sqllist, "0");
				//end = Instant.now();
				//logSimpalQryOrInsetTimeToCSV(start, end, "insert," + i + ",", "-", ResultOutPath);
				prep_sqllist.clear();

				Records_In_DB = Records_In_DB + i;
				i = 0;

				if (runQueries == true) {
					Data_Op = "QET";
					Data_Op_ID = "Q0_Count_extra";
					// SQL Query RUN
					exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
					start = Instant.now();
					SQLQuery(Q2data, "All", ResultOutPath, DBCon1, U1, P1, false);
					end = Instant.now();
					logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
				}
				return prep_sqllist;
			} catch (IOException e) {
				System.out.println("Errors in bulk, getInsertSqlLstFromCSV =" + e.getMessage());
				List<String> sqllist = new ArrayList<>();
				sqllist.add("SQL exception occured" + e);
				return sqllist;
			}
		}

	// Get records from CSV as List of String Arrays, strLength = 3 for Triplestore,
	// It's basically No_of_columns of a table
	public static List<String[]> getStrfromCSV2(String filepath, int strLength) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = null;
			List<String[]> recList = new ArrayList<>();

			while ((line = reader.readLine()) != null) {
				String[] rec = line.split(",");
				recList.add(rec);
			}
			reader.close();
			return (recList);
		} catch (IOException e) {
			List<String[]> reclist = new ArrayList<>();
			String[] rec = new String[2];
			rec[0] = "SQL exception occured" + e;
			reclist.add(rec);
			return reclist;
		}
	}

	// Get records from CSV as List of String Arrays, strLength = 3 for Triplestore,
	// It's basically No_of_columns of a table
	public static List<String[]> getInsertStrfromCSV3(String fileInpath, String SQLfileInpath, String NODBfileOutPath,
			String ResultOutPath, int lines, boolean append, String DBCon1, String U1, String P1, String DBCon2,
			String U2, String P2, String SQLQueryPath, String SQLQueryPath2, int bulkNo, boolean runQueries,
			boolean isSQLDataset) {
		try {
			System.out.println("at getInsertStrfromCSV3" + fileInpath + ", \n SQLfileInpath= " + SQLfileInpath
					+ "\n NODBfileOutPath = " + NODBfileOutPath + "\n ResultOutPath =" + ResultOutPath);
			try {
				Data_Op = "Thread.sleep";
				Data_Op_ID = "500";
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			Data_Op = "DLT";
			Data_Op_ID = bulkNo + "";

			BufferedReader reader = new BufferedReader(new FileReader(fileInpath));
			String line = null;
			List<String[]> recList = new ArrayList<>();
			List<String[]> Qdata = new ArrayList<>();
			int i = 0;
			int scaletime = 0; // first time, we don't want to increase bulkno
			Instant start;
			Instant end;
			String exp;
			if (runQueries == true) {
				exp = "Exp3.4, GET SQL Queries List from CSV Time";
				start = Instant.now();
				Qdata = getQryStrfromCSV2(SQLQueryPath, 0);
				end = Instant.now();
				logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			}
			while ((line = reader.readLine()) != null) {
				if (isSQLDataset == true) {
					line = line.split(" VALUES")[1].trim().replace("('", "").replace("', '", ",").replace("');", "");
					/*
					 * //System.out.println(line+"\n"); line = line.replace("('", "");
					 * //System.out.println(line+"\n"); line = line.replace("', '", ",");
					 * //System.out.println(line+"\n"); line = line.replace("');", "");
					 * //System.out.println("Hey 2.3\n"); //System.out.println(line+"\n");
					 * 
					 */
				}
				String[] rec = line.split(",");
				recList.add(rec);
				i++;
				if (i == bulkNo) {
					start = Instant.now();
					writeCSVB3(NODBfileOutPath, recList, append);
					end = Instant.now();
					logSimpalQryOrInsetTimeToCSV(start, end, "insert," + i + ",", "-", ResultOutPath);
					recList.clear();
					// System.gc();
					Records_In_DB = Records_In_DB + i;
					i = 0;

					// to scale the records "while streaming", //1lakh->1+1->2 lakh = 2+2 =4lakh
					if (scale == true && scaletime != 0) {
						bulkNo = bulkNo + bulkNo;
					}
					scaletime++;
					if (runQueries == true) {
						Data_Op = "QET";
						Data_Op_ID = "Q0_Count_extra";
						if ((IsrunQueriesatAfterNoofrecords == false) || (IsrunQueriesatAfterNoofrecords == true
								&& (scaletime * bulkNo) >= runQueriesatAfterNoofrecords)) {
							System.out.println("\n PgRAW runQueriesatAfterNoofrecords = ,"
									+ runQueriesatAfterNoofrecords + ",Scale = " + scaletime + ",Bulk = ," + bulkNo);
							
							// SQL Query RUN
							exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
							start = Instant.now();
							SQLQuery(Qdata, "All", ResultOutPath, DBCon1, U1, P1, true);
							end = Instant.now();
							logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);
						}
						Data_Op = "DLT";
						Data_Op_ID = bulkNo + "";
					}

					if (IsRunOnceStatic == true) // break after inserting bulkno of records once
					{
						System.out.print("break");
						break;
					}
				}
			}
			reader.close();
			//start = Instant.now(); //Not adding remaining records
			//writeCSVB3(NODBfileOutPath, recList, append);
			//end = Instant.now();
			//logSimpalQryOrInsetTimeToCSV(start, end, "insert," + i + ",", "-", ResultOutPath);
			recList.clear();
			System.gc();
			Records_In_DB = Records_In_DB + i;
			i = 0;
			if (runQueries == true) {
				Data_Op = "QET";
				Data_Op_ID = "Q0_Count_extra";
				// SQL Query RUN
				exp = "Total Time for All SQL Queries to RUN & Save To CSV check";
				start = Instant.now();
				SQLQuery(Qdata, "All", ResultOutPath, DBCon1, U1, P1, true);
				end = Instant.now();
				logTimetoCSV(start, end, exp, fileInpath, NODBfileOutPath, ResultOutPath);

			}
			return (recList);
		} catch (IOException e) {
			System.out.println("Hey Error 3\n");
			List<String[]> reclist = new ArrayList<>();
			String[] rec = new String[2];
			rec[0] = "SQL exception occured" + e;
			reclist.add(rec);
			return reclist;
		}
	}

	// getQryStrfromCSV2
	// Get Query records from CSV as List of String Arrays, strLength = 3 for
	// Triplestore, It's basically No_of_columns of a table
	public static List<String[]> getQryStrfromCSV2(String filepath, int strLength) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = null;
			List<String[]> recList = new ArrayList<>();

			while ((line = reader.readLine()) != null) {
				String[] rec = line.toString().split(",", 2);
				rec[1] = rec[1].replace("\"\"", "\""); // A single double quote is replaced by double double quote in
														// readLine
				System.out.println("line1 =" + line.toString() + "\n rec[0] =" + rec[0].toString() + "rec[1] ="
						+ rec[1].toString());
				recList.add(rec);
			}
			reader.close();
			return (recList);
		} catch (IOException e) {
			List<String[]> reclist = new ArrayList<>();
			String[] rec = new String[2];
			rec[0] = "SQL exception occured" + e;
			reclist.add(rec);
			return reclist;
		}
	}

	/*
	 * Test CSV WRITE DATA code public static float writeCSV(String filePath,
	 * String[] records, boolean append) { File file = new File(filePath);
	 * 
	 * try { // create FileWriter object with file as parameter FileWriter
	 * outputfile = new FileWriter(file, append);
	 * 
	 * // create CSVWriter object filewriter object as parameter CSVWriter writer =
	 * new CSVWriter(outputfile);
	 * 
	 * for(int i=0; i<records.length; i++) { writer.writeNext(new String[]
	 * {records[i]}); }
	 * 
	 * // BULK SAVE: create a List which contains String array
	 * //writer.writeAll(records);
	 * 
	 * 
	 * //List<String[]> data = new ArrayList<String[]>(); //data.add(new String[] {
	 * "Name", "Class", "Marks" }); //data.add(new String[] { "Aman", "10", "620"
	 * }); //data.add(new String[] { "Suraj", "10", "630" });
	 * //writer.writeAll(data);
	 * 
	 * // closing writer connection writer.close(); return 1; } catch (IOException
	 * e) { // e.printStackTrace(); return 0; } }
	 */

	/*
	 * Test CSV GET DATA code public static String[] getfromCSV(String filepath, int
	 * lines, boolean append) { try { // open file input stream //BufferedReader
	 * reader = new BufferedReader(new FileReader("employees.csv")); BufferedReader
	 * reader = new BufferedReader(new FileReader(filepath));
	 * 
	 * // read file line by line String line = null; //Scanner scanner = null; int i
	 * = 0; String[] sqllist = new String[6000000];
	 * 
	 * while ((line = reader.readLine()) != null) { //Employee emp = new Employee();
	 * //scanner = new Scanner(line); //scanner.useDelimiter(","); //while
	 * (scanner.hasNext()) { i = 0; sqllist[i]=line; System.out.println("data::" +
	 * line); i++;
	 * 
	 * //} //index = 0; }
	 * 
	 * //close reader reader.close(); return sqllist; } catch(IOException e) {
	 * String[] sqllist = new String[2]; sqllist[0] = "SQL exception occured" + e;
	 * return sqllist; } }
	 */

	// CSV WRITE DATA code, 1 write Single Record data to CSV, File opens again and
	// closes after saving single record
	public static float writeCSV(String filePath, String record, boolean append) {
		File file = new File(filePath);

		try {
			// File yourFile = new File(file);
			// file.createNewFile(); // if file already exists will do nothing
			// FileOutputStream oFile = new FileOutputStream(file, false);

			FileWriter outputfile = new FileWriter(file, append); // true is append data at end of CSV file
			CSVWriter writer = new CSVWriter(outputfile);
			writer.writeNext(record.split(","));
			writer.close();
			return 1;
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	// CSV WRITE DATA code, 1 by 1 write data to CSV, File open once from String
	// Array
	public static float writeCSV(String filePath, String[] records, boolean append) {
		File file = new File(filePath);

		try {
			FileWriter outputfile = new FileWriter(file, append);
			CSVWriter writer = new CSVWriter(outputfile);
			for (int i = 0; i < records.length; i++) {
				System.out.print("records.length" + records.length);
				if (records[i] != "") {
					System.out.print(records[i]);
					writer.writeNext(records[i].split(","));
				}
				System.out.print(records[i]);
			}
			writer.close();
			return 1;
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	// CSV WRITE DATA code, 1 by 1 write data to CSV, File open once from List
	// String
	public static float writeCSV(String filePath, List<String> records, boolean append) {
		File file = new File(filePath);

		try {
			FileWriter outputfile = new FileWriter(file, append);
			CSVWriter writer = new CSVWriter(outputfile);
			for (int i = 0; i < records.size(); i++) {
				writer.writeNext(records.get(i).split(","));
			}
			writer.close();
			return 1;
		} catch (IOException e) {
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
		} catch (IOException e) {
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
		} catch (IOException e) {
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
		} catch (IOException e) {
			e.printStackTrace();
			//throw e; 
			return 1;
		}
	}

	// DELETE CSV WRITE DATA code, 1 write Single Record data to CSV, File opens
	// again and closes after saving single record
	public static String delCSVFile(String filePath) {
		try {
			File file = new File(filePath);
			if (file.exists()) {
				file.delete();
			}
			if (file.createNewFile()) {
				System.out.println("File is created!");
			}
			return "File deleted and new created";
		} catch (IOException e) {
			return "Error in File deletion and creation " + e.getMessage().toString();
		}
	}

	public static int logTimetoCSV(Instant start, Instant end, String exp, String fileInpath, String NODBfileOutPath,
			String ResultOutPath) {
		float timeElapsed = calcTime(start, end);
		// DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd
		// HH:mm:ss.SSS");
		// LocalDateTime now = LocalDateTime.now();
		System.out.println(dtf.format(LocalDateTime.now())); // 2016/11/16 12:08:43
		writeCSV(ResultOutPath,
				"Exp:, " + exp + ", " + timeElapsed + ", milliseconds, Currect DateTime, "
						+ dtf.format(LocalDateTime.now()) + ", fileInpath, " + fileInpath + ", NODBfileOutPath, "
						+ NODBfileOutPath + ", ResultOutPath " + ResultOutPath + "\n",
				true);
		// writeCSV(ResultOutPath, , 0);
		return 1;
	}

	public static float calcTime(Instant start, Instant end) {
		Duration timeElapsed = Duration.between(start, end);
		System.out.println("Time taken: " + timeElapsed.toMillis() + " milliseconds");
		return timeElapsed.toMillis();
	}

	public static int logSimpalQryOrInsetTimeToCSV(Instant start, Instant end, String s1, String s2,
			String ResultOutPath) {
		float timeElapsed = calcTime(start, end);
		LocalDateTime s = LocalDateTime.ofInstant(start, ZoneOffset.ofHoursMinutes(5, 30));
		LocalDateTime e = LocalDateTime.ofInstant(end, ZoneOffset.ofHoursMinutes(5, 30));

		System.out.println(s + "," + e);

		writeCSV(ResultOutPath, s1 + "," + timeElapsed + ", milliseconds, Start_DATETIME=," + dtf.format(s)
				+ ",End_DATETIME=," + dtf.format(e) + ", " + s2 + ", DATETIME=," + dtf.format(LocalDateTime.now()),
				true);
		return 1;
	}

	// SQL Insert means, Directly executing SQL statements, not creating insert
	// statements and then executing them
	// 1 by 1 multiple query execute, Connection made only once, NO TRANSACTION
	public static String SQLInsert(String[] qry, String qno) {
		try {
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
			Connection conSQL = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");

			
			Statement stmt = conSQL.createStatement();
			for (int i = 0; i < qry.length; i++) {
				// System.out.println("");
				// System.out.println(qry[i].replace('"', ' ').trim());
				// stmt.executeUpdate(qry[i].replace('"', ' ').trim()); //errors
				stmt.executeUpdate(qry[i].trim());
			}
			conSQL.close();
			return ("Update/Insert success.");
		} catch (SQLException e) {
			System.out.println("SQL exception occured" + e);
			return ("SQL exception occured" + e);
		}
	}

	// Bulk BATCH query execute, Connection made only once, TRANSACTION TRUE
	public static String SQLBatchInsert(String[] qry, String qno) {
		try {
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
			Connection conSQL = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");

			
			Statement stmt = conSQL.createStatement();
			int kk = 0;
			// con.autoCommit(false);
			System.out.println("\n query length = " + qry.length);
			for (int i = 1; i <= qry.length; i++) {
				// stmt.addBatch(qry[i].replace('"', ' ').trim()); //Errors
				stmt.addBatch(qry[i].trim());
				kk++;
			}
			int[] result = stmt.executeBatch();
			conSQL.commit();
			System.out.println("Count of number of added records = sqllist (kk) =" + kk);
			kk = 0;
			conSQL.close();
			return ("Update/Insert success." + result.length);
		} catch (SQLException e) {
			return ("SQL exception occured" + e);
		}
	}

	// Bulk BATCH query execute, Connection made only once, TRANSACTION TRUE
	public static String SQLBatchInsert(List<String> qry, String qno) {
		try {
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
			Connection conSQL = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om"); 
			Statement stmt = conSQL.createStatement();
			//conSQL.setAutoCommit(false);
			int kk = 0;
			System.out.println("\n qry.size() = " + qry.size());
			for (int i = 0; i < qry.size(); i++) {
				String s1 = qry.get(i).trim(); /// errors in trim.substring(1, qry.get(i).length()-1)
				stmt.addBatch(s1);
				kk++;
				// System.out.println("\n"+s1);
			}
			int[] result = stmt.executeBatch();
		//	conSQL.commit();
			System.out.println("Count of number of added records = sqllist (kk) =" + kk);
			kk = 0;
			conSQL.close();
			return ("Update/Insert success." + result.length);
		} catch (SQLException e) {
			return ("SQL exception occured" + e);
		}
	}

	// 1 by 1 multiple query execute, Connection made only once, NO TRANSACTION
	public static String SQLInsert(List<String> qry, String qno) {
		try {
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
			Connection conSQL = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");

			
			Statement stmt = conSQL.createStatement();
			for (int i = 0; i < qry.size(); i++) {
				stmt.executeUpdate(qry.get(i).trim());
			}
			 conSQL.close();
			return ("Update/Insert success.");
		} catch (SQLException e) {
			return ("SQL exception occured" + e);
		}
	}

	// 1 by 1, multiple query execute , Connection made only once, Only "For()" loop
	// changes, NO TRANSACTION
	public static String SQLInsert2(List<String> qry, String qno) {
		try {
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
			Connection conSQL = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");

			
			Statement stmt = conSQL.createStatement();
			for (String s : qry) {
				stmt.executeUpdate(s);
			}
			conSQL.close();
			return ("Update/Insert success.");
		} catch (SQLException e) {
			return ("SQL exception occured" + e);
		}
	}

	// 1 query execute, each time function called, connection is made., NO
	// TRANSACTION
	public static String SQLInsert(String qry, String qno) {
		try {
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
			Connection conSQL2 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2","om","om");
						
			Statement stmt = conSQL2.createStatement();
			stmt.executeUpdate(qry);
			// con.close();
			conSQL2.close();
			return ("Update/Insert success.");
		} catch (SQLException e) {
			return ("SQL exception occured" + e);
		}
	}

	// BuildSQLInset => Build SQL statements then Insert
	// 1 by 1 insert, Connection made once, NO TRANSACTION
	public static String BuildSQLInsert(List<String[]> qry, String qno) {
		return "Code not exist yet, Write one";
	}

	// BBuildSQLInset => Bulk Build SQL statements then Insert
	// BULK insert, Connection made once, TRUE TRANSACTION
	public static String BBuildSQLInsert2(List<String[]> qry, String qno) {
		return "Code not exist yet, Write one";
	}

	/*
	 * //Test Query Execute code, with extra codes public static String
	 * SQLQuery(String[] qry, String qno) { try {
	 * Class.forName("org.postgresql.Driver"); } catch(ClassNotFoundException e) {
	 * System.out.println("Class not found "+ e); }
	 * System.out.println("JDBC Class found"); try { int no_of_rows = 0; Connection
	 * con = DriverManager.getConnection
	 * ("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres"); Statement
	 * stmt = con.createStatement(); //ResultSet rs = stmt.executeQuery
	 * ("insert into public.LODTriples valuse(sub, obj, pred) ('2','3','4');");
	 * for(int i=0; i<qry.length; i++) { ResultSet rs = stmt.executeQuery (qry[i]);
	 * //To get the Rows count directly //ResultSet rs = stmt.executeQuery
	 * ("select count(*) from (" + qry[i] + ") tbl"); while (rs.next()) {
	 * no_of_rows++; //
	 * System.out.println(rs.getString(1)+" | "+rs.getString(2)+" | "+rs.getString(3
	 * )); } System.out.println (qry[i]+" \n There are "+ no_of_rows +
	 * " record in the table"); no_of_rows = 0; } return ("There are "+ no_of_rows +
	 * " record in the table"); } catch(SQLException e) { return
	 * ("SQL exception occured" + e); } }
	 */

	// 1 Bulk query execute, Connection made only once
	public static String SQLQuery(String[] qry, String qno, boolean IsRawQuery) {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Class not found " + e);
		}
		System.out.println("JDBC Class found");
		try {
			int no_of_rows = 0;
			String r = "";
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
			
			// Connection conRaw = DriverManager.getConnection(DBCon1, U1, P1);
			Connection conRaw = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om");
			
			Connection conSQL2 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");
						
			
			Statement stmt = conRaw.createStatement();
			
			if(IsRawQuery == false)
			{
				stmt = conSQL2.createStatement();
			}
			for (int i = 0; i < qry.length; i++) {
				ResultSet rs = stmt.executeQuery(qry[i]);
				while (rs.next()) {
					no_of_rows++;
				}
				r = qry[i] + " \n There are " + no_of_rows + " record in the table";
				System.out.println(r);
				no_of_rows = 0;
			}
			 conRaw.close();
			 conSQL2.close();
			return (r);
		} catch (SQLException e) {
			return ("SQL exception occured" + e);
		}
	}

	/*
	 * //1 by 1 multiple query execute, Connection made only once public static
	 * List<String> SQLQuery(List<String> qry, String qno) { try {
	 * Class.forName("org.postgresql.Driver"); } catch(ClassNotFoundException e) {
	 * System.out.println("Class not found "+ e); }
	 * System.out.println("JDBC Class found"); try { int no_of_rows = 0;
	 * //Connection con =
	 * DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank",
	 * "mayank", "postgres"); Statement stmt = con.createStatement(); for(String S :
	 * qry) { ResultSet rs = stmt.executeQuery(S); while (rs.next()) { no_of_rows++;
	 * 
	 * } System.out.println (S+" \n There are "+ no_of_rows +
	 * " records in the result"); } return qry; } catch(SQLException e) { return
	 * ("SQL exception occured" + e);; } }
	 */

	// Check if query ID is written properly or not.
/*	public static boolean setQueryTypeFlag_Iscountqueries_Q_ID_C(String q_id) {
		String Q_ID = q_id.toString().trim();
		String Q_ID_C = Q_ID.substring(Q_ID.length() - 1);
		if (Objects.equals(Q_ID_C, "C")) {
			System.out.println("Q_ID _C check = true!," + Q_ID);
			Iscountqueries = true;
			IsExplainQuery = false;
		} else if (Objects.equals(Q_ID_C, "E")) // Explain Query
		{
			System.out.println("Q_ID _C check (qrite <Q1_C> (without < & > in query list CSV) ) = false!," + Q_ID);
			Iscountqueries = false;
			IsExplainQuery = true;
		} else {
			System.out.println("Other Queries:: " + Q_ID);
			Iscountqueries = false;
			IsExplainQuery = false;
		}

		return Iscountqueries;
	}
*/
	public static boolean setQueryTypeFlag_Iscountqueries_QueryStatement(String q) {
		String Q = q.toString().trim();
		String Exa = "sel";
		String Exa2 = "Exp";
		String Exa3="Set";
		String Exa4="Sho";
		// String Exa5="TRU";
		int exaLen = Exa.length();
		//System.out.print("**********"+q+"--------");
		String QS = Q.substring(0, exaLen).trim(); // sel
		if (QS.equalsIgnoreCase(Exa)  || QS.equalsIgnoreCase(Exa4)) {
			//System.out.println("QueryStatement check, Iscountqueries= true!," + QS);
			Iscountqueries = true;
			IsExplainQuery = false;
		} else if (QS.equalsIgnoreCase(Exa2)) {
		//	System.out.println("QueryStatement , Iscountqueries= false!," + QS + ", Not same as =" + Exa);
			Iscountqueries = false;
			IsExplainQuery = true;
		} else {
			System.out.println("Other Queries2:: ");
			Iscountqueries = false;
			IsExplainQuery = false;
		}

		return Iscountqueries;
	}
	
	public static boolean setQueryTypeFlag_IsExplainqueries_QueryStatement(String q) {
		String Q = q.toString().trim();
		String Exa = "sel";
		String Exa2 = "Exp";
		// String Exa3="DIS";
		// String Exa4="INS";
		// String Exa5="TRU";
		int exaLen = Exa.length();
		String QS = Q.substring(0, exaLen).trim(); // sel
		if (QS.equalsIgnoreCase(Exa)) {
		//	System.out.println("QueryStatement check, Iscountqueries= true!," + QS);
			Iscountqueries = true;
			IsExplainQuery = false;
		} else if (QS.equalsIgnoreCase(Exa2)) {
		//	System.out.println("QueryStatement , Iscountqueries= false!," + QS + ", Not same as =" + Exa);
			Iscountqueries = false;
			IsExplainQuery = true;
		} else {
			System.out.println("Other Queries2:: ");
			Iscountqueries = false;
			IsExplainQuery = false;
		}

		return IsExplainQuery;
	}

	// Remove Double Quotes from first and End
	public static String RemDoubleQutes(String S) {
		// System.out.println("S.substring(0, 1) = ,"+ S.substring(0, 1)
		// +"}"+Objects.equals(S.substring(0, 1), "\"")+"{ S="+S);
		// System.out.println("S.substring(S.length()-1) = ,"+ S.substring(S.length()-1)
		// +"} S="+S);
		if (Objects.equals(S.substring(0, 1), "\"")) {
			// System.out.println("S.substring(0, 1) = ,"+ S.substring(0, 1) +"} S="+S);
			S = S.substring(1);
			// System.out.println("S' => S="+S);
		}
		if (Objects.equals(S.substring(S.length() - 1), "\"")) {
			// System.out.println("S.substring(S.length()-1) = ,"+ S.substring(S.length()-1)
			// +"} S="+S);
			S = S.substring(0, S.length() - 1);
			// System.out.println("S' => S="+S);
			// old code works when it's fix that first char is (") but later had to
			// check//qry.get(i)[1].length()-1).trim();//.replace("\"", "\\\""); // Replace
			// "" to "
		}
		return S;
	}

	// 1 by 1 multiple query execute, Connection made only once
	public static List<String[]> SQLQuery(List<String[]> qry, String qno, String ResultOutPath, String DBCon1,
		String U1, String P1, boolean IsRawQuery) {
		try {
			int no_of_rows = 0;
			String c = "\"";
			System.out.println("c =," + c + "}");
			// Connection con = DriverManager.getConnection(DBCon1, U1, P1);
			 DBInsert.conRaw = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om");
			 DBInsert.conSQL = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om");
						
			
			Data_Op = "QET";
			Data_Op_ID = "Q0_Count_extra";
			System.out.println("No. of Qry in list = ," + qry.size());
			 ExecutorService executor= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

			for (int i = 0; i < qry.size(); i++) {
				// String S = qry.get(i)[1].toString().replace('"', ' ').trim(); // Errors in
				// results as queryy had (") in middle

				String S = qry.get(i)[1].toString();
				String Q_ID = qry.get(i)[0].toString();
				Data_Op_ID = Q_ID;
				S = RemDoubleQutes(S);
				
            	System.out.println("\n Q_ID = "+Q_ID );

				String S2 = ""; // To get record count for count(*) queries...
				System.out.println("\n Test Before Runnable!," + S + "");

				////***Query Execution Code is placed in subthread***////
				
			//	QueryThread Q[] = new QueryThread[]();
			//	thread1.QueryThreadParam("CPU & RAM", "top -i -b -d "+RMFreq+" -H -o +%MEM");
				
			        try{
						System.out.println("\n Try, RM_AvailCPU="+RM_AvailCPU+",RM_AvailMem="+RM_AvailMem+" ,RM_IOWait="+RM_IOWait);
						
						while(RM_AvailCPU<Min_AvailCPU || RM_AvailMem<Min_AvailMem || RM_IOWait>Min_IOWait)
						{
			            	Thread.sleep(100);
			            	//System.out.println("\n Resources not available, RM_AvailCPU="+RM_AvailCPU+",RM_AvailMem="+RM_AvailMem+" ,RM_IOWait="+RM_IOWait);
						}
			            if (RM_AvailCPU>Min_AvailCPU && RM_AvailMem>Min_AvailMem && RM_IOWait<Min_IOWait)
			            {
			            	System.out.println("\n Resource available" );
			            
			            	executor.execute(new QueryThread(S, Q_ID, IsRawQuery, DBInsert.conRaw, DBInsert.conSQL));
			                ThreadsCount++;
			                while( (ThreadsCount)>=MaxThreads) //check & wait till assigned threads complete execution if No. of threads increase the allowed limit
			                {
								Thread.sleep(100);  //0.1sec sleep
				               // System.out.println("\n Sleep - Threads="+ThreadsCount );

			                }
			                System.out.println("\n Resource available - Threads="+ThreadsCount );
			            }
			            else
			            {
			            	//Thread not ran///
			            }
			        }catch(Exception err){
			            err.printStackTrace();
			            continue;
			        }
								
			       

				// logSimpalQryOrInsetTimeToCSV(start, end, "Query
				// ID=,"+qry.get(i)[0],"Start_DATETIME=,"+dtf.format(start)+",End_DATETIME=,"+dtf.format(end)+",
				// no. of records in the query result = ,"+ no_of_rows+", Count in results (if
				// count(*)) = ,"+S2, ResultOutPath);
			/*	
*/
				// System.gc();
			}
			 while(ThreadsCount>0)
		        {
	            	System.out.println("\n Waiting for Threads to complete. Sleep 1sec, ThreadsCount="+ThreadsCount ); //wait for threads to complete

					Thread.sleep(1000);
		        }
		        executor.shutdown(); 

        	System.out.println("\n Con. Closed" );
        	
        	DBInsert.writeCSVB3(DBInsert.ResultOutPath, DBInsert.RMList, true);
			DBInsert.RMList.clear();
			
			 DBInsert.conRaw.close();
			 DBInsert.conSQL.close();
			return qry;
		} catch (Exception e) {
			System.out.println("Error");
			List<String[]> reclist = new ArrayList<>();
			String[] rec = new String[2];
			rec[0] = "SQL exception occured" + e;
			reclist.add(rec);
			System.out.println("\n Error= " + e.getMessage() + "\n SQL Ex=" + e.getMessage() + "\n Other = "
					+ e.getLocalizedMessage());
			try {
//				DBInsert.con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");
				//Connection conRaw = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om");
				
				Connection conSQL2 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");
								
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return reclist;
		}
	}

	public static String WM_Query(float CPU, float RAM, String query)
	{
		String[] words = query.toLowerCase().split("lodtriples");
		int Join_Count = words.length - 1;
		if(Join_Count == 0)
		{
			words = query.toLowerCase().split("photoprimary");
			Join_Count = words.length - 1;
		}
		if (Join_Count == 0)
		{
			Join_Count = 1;
		}
			
		System.out.println("No. of joins in a query = " + Join_Count + " CPU COres = " + Runtime.getRuntime().availableProcessors());
		
		float No_Proc = CPU/(100/CPU_Cores);//No of processes that can be handled by available/free CPU 
		if(No_Proc<1)
		{
			No_Proc = 1;
		}
			//60=> 100%16GB => 1/4 => 160/CPU CORES => 40 => 4GB RAM maximum to each process => However complex queries can get upto 1.5x 4 = > 6GB of RAM which is 37% of Total RAM. So, atlest 3 complex queries can run in parallel. 
		int WM_value = (int) (((RAM/No_Proc)*(160/CPU_Cores))*(Join_Count/4.0));   //changed from this (Join_Count/4) to this  => (Join_Count/4.0) => Tell me what was the issue, here. ?  (3 or less divide by 4 was zero - only 4/4 and above were integer...)
		
		System.out.println("WM_value = "+ WM_value + ",((("+RAM+"/("+CPU+"/(100/"+CPU_Cores+")))*40)*("+Join_Count+"/4)) = " +(((RAM/(CPU/(100/CPU_Cores)))*150)*(Join_Count/4.0)) );
		String wm_q = "set work_mem = '"+WM_value+"MB';";		
		System.out.println(wm_q);
		return wm_q;
	}
	// 1 by 1 query execute, each time , connection is made.
	public static String SQLQuery(String qry, String qno, boolean IsRawQuery) {
		try {
			int no_of_rows = 0;
			String r = "";
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
			Connection conRaw = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om");
			
			Connection conSQL2 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");
						
			
			Statement stmt = conRaw.createStatement();
			Instant start;
			Instant end;
			start = Instant.now();
			//String s1;
			String s2 = "";
			
			if(IsRawQuery == false)
			{
				stmt = conSQL2.createStatement();
			}			
			ResultSet rs = stmt.executeQuery(qry);
			while (rs.next()) {
				no_of_rows++;
				r = qry + " \n, There are  line = ," + no_of_rows + ",record in the table = ," + rs.getString(1)
						+ ", Full =," + rs.toString();
				s2 = rs.getString(1);
			}
			
			end = Instant.now();
			logSimpalQryOrInsetTimeToCSV(start, end, "Query ID=," + qno,
					"DATETIME=," + DBInsert.dtf.format(LocalDateTime.now()) + ", NEW SHOW WM - no. of records in the query result = ,"
							+ no_of_rows + ", Count in results (if count(*)) = ," + s2+",ThreadID=,"+Thread.currentThread().getName()+",TotalCurrentRunninThreads=,"+DBInsert.ThreadsCount+",Query=,"+qry,
					DBInsert.ResultOutPath);
					
			no_of_rows = 0;
			System.out.println(r);
			 conRaw.close();
			 conSQL2.close();
			 return (r);
		} catch (SQLException e) {
			return ("SQL exception occured" + e);
		}
	}
	public static String SQLQuery(String qry, String qno, boolean IsRawQuery, Statement stmt) {
		try {
			int no_of_rows = 0;
			String r = "";
			// Connection con =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank",
			// "postgres");
		//	Connection conRaw = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om");
			
		//	Connection conSQL2 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2", "om", "om");
						
			
		//	Statement stmt = con.createStatement();
			Instant start;
			Instant end;
			start = Instant.now();
			//String s1;
			String s2 = "";
			
		//	if(IsRawQuery == false)
		//	{
			//	stmt = con.createStatement();
		//	}			
			ResultSet rs = stmt.executeQuery(qry);
			while (rs.next()) {
				no_of_rows++;
				r = qry + " \n, There are  line = ," + no_of_rows + ",record in the table = ," + rs.getString(1)
						+ ", Full =," + rs.toString();
				s2 = rs.getString(1);
			}
			
			end = Instant.now();
			logSimpalQryOrInsetTimeToCSV(start, end, "Query ID=," + qno,
					"DATETIME=," + DBInsert.dtf.format(LocalDateTime.now()) + ", NEW SHOW WM - no. of records in the query result = ,"
							+ no_of_rows + ", Count in results (if count(*)) = ," + s2+",ThreadID=,"+Thread.currentThread().getName()+",TotalCurrentRunninThreads=,"+DBInsert.ThreadsCount+",Query=,"+qry,
					DBInsert.ResultOutPath+"WM");
					
			no_of_rows = 0;
			System.out.println(r);
			 //conRaw.close();
			// con.close();
			 return (r);
		} catch (SQLException e) {
			return ("SQL exception occured" + e);
		}
	}
}