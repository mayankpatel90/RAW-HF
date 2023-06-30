import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.DriverManager;
import java.time.LocalDateTime;

public class ThreadRawQuery extends Thread {

	private String t_cmd="";
	private boolean isRAW;
	
	 public void QueryThread(boolean isRawww, String thread_cmd) {
	     this.isRAW = isRawww;   
		 this.t_cmd = thread_cmd;
	}
	 
	@Override
	public void run() {		
		System.out.println("Thread Raw Query is running");
		System.out.println("0"); 	
		
		try
		{
			int lines = 0;
			boolean append = true;
			int bulkNo = 1000000;    // bulkNo = 2000000; // it fails to load more than 5M records in bulk, so error after this.
			int StaticDataScale = 1000000; //when IsRunOnceStatic = true. Static increase Bulk No
			boolean runQueries = false; // Run Queries in between of bulk save //false=>Run once at end... true=>run after every bulk insert
			boolean isSQLDataset = true;// added 6feb 2019, take same dataset file
			
			String DBCon1 = "jdbc:postgresql://localhost:5432/om";
			String U1 = "om";
			String P1 = "om";
			String DBCon2 = "jdbc:postgresql://localhost:5432/om"; // mayank
			String U2 = "om"; // mayank
			String P2 = "om"; // postgres
	
			String ExpQuerydesc = "With Io stats";
	//Datasets
			String fileInpath = "/home/om/workspace/DBRAW1/Datasets/LODTriplesPlainK_10M.txt";
			String SQLfileInpath = "/home/om/workspace/DBRAW1/Datasets/LODTriplesPlainK_10M.txt";
	//Queries
			// DBMS LODTriples
			String SQLQueryPath = "/home/om/workspace/DBRAW1/queries/LODTriplesQueries.csv";
			// RAW LODTriples2
			String SQLQueryPath2 = "/home/om/workspace/DBRAW1/queries/LODTriples2Queries.csv";
	//PostgresRAW - NoDB Linked File	
			// Now RAW DB file table is changed to LODTriples2. from 31-oct-2018
			String NODBfileOutPath = "/home/om/workspace/DBRAW1/NODB/LODTriples2.csv";
	
			if (DBInsert.IsValuesOnlyDataset == true) {
				fileInpath = "/home/om/workspace/DBRAW1/Datasets/LODValues/LODTriplesPlainK_10M_V_SQL.txt";
				SQLfileInpath = "/home/om/workspace/DBRAW1/Datasets/LODValues/LODTriplesPlainK_10M_V_SQL.txt";
				SQLQueryPath = "/home/om/workspace/DBRAW1/Datasets/LODValues/Q2.csv"; // PgRAW
				SQLQueryPath2 = "/home/om/workspace/DBRAW1/Datasets/LODValues/Q.csv"; // PgSQL
			}
			
	//Start 						
			System.out.print("Hello world from RAW Query");
			System.out.println(DBInsert.dtf.format(LocalDateTime.now())); // 2016/11/16 12:08:43
			//DBInsert.con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mayank","mayank", "postgres");
			//DBInsert.con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om");

			
		//	DBInsert.RUNPgRAW = true;
		//	DBInsert.RUNPgSQL = true;
			String ResultOutPath = "/home/om/workspace/DBRAW1/results/result"+ "CountQueries" + bulkNo + "RAWTHREAD.csv";
			
			
			boolean SDSSDataset = true;
			if(SDSSDataset == true)
			{
				SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_Q2.csv"; // PgRAW
				//SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_Q.csv"; // PgSQL
			//	SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_P1_Q.csv"; // PgSQL
				//SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/SDSS_P1_Q2.csv"; // PgRAW
				
				
				//For QTA
				SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/QTA_pgRAW_Raw_R_MT.csv"; // PgRAW

				SQLQueryPath2 = "/media/om/G/SDSS/NODB/SDSS/Queries/QTA_pgRAW_Raw_R_MT.csv"; // PgRAW


			}
			
			//Thread.sleep(5000);
			if(isRAW == true)
			{
			DBInsert.exp3Raw(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2,
					U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries, isSQLDataset);
			}
			else
			{
				DBInsert.exp3SQL(fileInpath, SQLfileInpath, NODBfileOutPath, ResultOutPath, lines, append, DBCon1, U1, P1, DBCon2,
						U2, P2, SQLQueryPath, SQLQueryPath2, bulkNo, runQueries, isSQLDataset);
			
			}
		
		
		} catch (Exception e) {
			System.out.println(e.getLocalizedMessage() + ",\n" + e.getMessage());
			return;
		}
	}

}