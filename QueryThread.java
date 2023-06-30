import java.lang.invoke.WrongMethodTypeException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class QueryThread implements Runnable {
	private String S = "";
	private String Q_ID = "";

	private String S2 = "";
	private int no_of_rows = 0;
	private Connection conRaw;
	private Connection conSQL2;
	private boolean IsRawQuery;

	public QueryThread(String S, String Q_ID, boolean IsRawQuery, Connection conRaw, Connection conSQL2) {
		this.S = S;
		this.Q_ID = Q_ID;
		this.conRaw = conRaw;
		this.conSQL2 = conSQL2;
		this.IsRawQuery = IsRawQuery;
	}

	@Override
	public void run() {
		try {
			// DBInsert.conRaw =
			// DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om",
			// "om"); //for each thread new connection, maybe single connection issues in
			// multithreading
			Connection conSQL2N = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om", "om", "om"); //

			Instant start;
			Instant end;
			start = Instant.now();
			String WM_Q = "";

			System.out.println("QueryThread is running Thread_NO: " + DBInsert.ThreadsCount);

			Statement stmt = DBInsert.conRaw.createStatement();

			if (IsRawQuery == false || (IsRawQuery == false && DBInsert.MaxThreads > 1)) // For multi-threading separate
																							// connections and RAM
																							// needed. In single
																							// connection it seems, only
																							// single CORE is used.
			{
				stmt = conSQL2N.createStatement();
			}
			// stmt.setQueryTimeout(DBInsert.QueryTimeOutSec); ///quit query execution if
			// query
			// doesn't answer with in 2000sec = 33min
			String getPID = "select pg_backend_pid()";
			ResultSet rs1 = stmt.executeQuery(getPID);
			ResultSetMetaData rsmd1 = rs1.getMetaData();
			int PID=0;
			
			while (rs1.next()) {
				System.out.println("PID from Query="+rs1.getString(1));
				PID= Integer.parseInt(rs1.getString(1));
			}
			
			System.out.println("PID of Connection = " + PID);

			ThreadSubClassIO_QueryRM t1 = new ThreadSubClassIO_QueryRM();
			t1.IoThreadParam("RMQueryID=," + Q_ID + ",CPU & RAM",
					"top -i -b -d " + DBInsert.RMFreq + " -H -o +%MEM -p " + PID);

			ThreadSubClassIO_QueryRM i1 = new ThreadSubClassIO_QueryRM();
			// thread3.IoThreadParam("IO_Total", "/home/om/workspace/DBRAW1/src/./t1.sh");
			i1.IoThreadParam("RMQueryID=," + Q_ID + ",IO_Total",
					"sudo iotop -b -o -d " + DBInsert.RMFreq + " -a -k -P -p " + PID); // sudo iotop -b -o -d 1 -a -k //
																						// sudo iotop -b -o -d 1 -a |
																						// grep 'Total DISK' (issies in
																						// grep)
			if (DBInsert.runStatThreads == true) {
				t1.start();
				i1.start();
			}

			if (DBInsert.setQueryTypeFlag_Iscountqueries_QueryStatement(S) == true
					|| DBInsert.setQueryTypeFlag_IsExplainqueries_QueryStatement(S) == true) {

				if (DBInsert.Is_SET_WM == true) {

					
					Instant start2;
					Instant end2;
					start2 = Instant.now();
					WM_Q = DBInsert.WM_Query(DBInsert.RM_AvailCPU, DBInsert.RM_AvailMem, S); // create work memory query

					boolean Executed = stmt.execute(WM_Q);

					end2 = Instant.now();
					System.out.println("WM_QUERY TIME === " + DBInsert.calcTime(start2, end2) + "milliseconds");

					System.out.println("IS Insert/Truncate/other executed?=  " + S + "S =" + Executed);

					// DBInsert.SQLQuery("Show work_mem;", Q_ID+" S_WM", false, stmt);
					// executor.execute(new QueryThread(WM_Q, "SET_WM",Q_ID IsRawQuery,
					// DBInsert.conRaw, DBInsert.conSQL));
					// ThreadsCount++;
					// executor.execute(new QueryThread("Show work_mem;", "SHOW_WM", IsRawQuery,
					// DBInsert.conRaw, DBInsert.conSQL));
					// ThreadsCount++;
				}

				ResultSet rs = stmt.executeQuery(S);
				ResultSetMetaData rsmd = rs.getMetaData();
				// System.out.println("querying SELECT * FROM XXX");
				int columnsNumber = rsmd.getColumnCount();
				while (rs.next()) {
					no_of_rows++;
					if (DBInsert.setQueryTypeFlag_Iscountqueries_QueryStatement(S) == true && rs.getString(1) != null) {
						// System.out.println("TT2 rs.getString(1) = " + rs.getString(1));
						S2 = ", TT2 rs.getString(1) = ," + rs.getString(1);
					} else if (DBInsert.setQueryTypeFlag_IsExplainqueries_QueryStatement(S) == true) {
						System.out.println("Explain query,");
						for (int m = 1; m <= columnsNumber; m++) {
							if (m > 1) {
								System.out.print(",  ");
							}
							String columnValue = rs.getString(m);
							System.out.print(columnValue + " " + rsmd.getColumnName(m));
							S2 = S2 + columnValue + " " + rsmd.getColumnName(m) + ",\n";
						}
					} else {
						// System.out.println("Other queries, TT2 rs.getString(0) = "+ rs.getString(0));
					}				
				}
				
				rs = null;
			} else {
				boolean Executed = stmt.execute(S);
				System.out.println("IS Insert/Truncate/other executed?=  " + S + "S =" + Executed);
			}
			
			if (DBInsert.runStatThreads == true) {
				t1.interrupt();
				i1.interrupt();
			}
			end = Instant.now();

			System.out.println(Q_ID + ", \n no. of records in the result =," + no_of_rows
					+ ", Count in results (if count(*))= ," + S2 + ", Query=," + S);

			DBInsert.logSimpalQryOrInsetTimeToCSV(start, end, "Query ID=," + Q_ID, "WM = " + WM_Q + ", DATETIME=,"
					+ DBInsert.dtf.format(LocalDateTime.now()) + ", no. of records in the query result = ," + no_of_rows
					+ ", Count in results (if count(*)) = ," + S2 + ",ThreadID=," + Thread.currentThread().getName()
					+ ",TotalCurrentRunninThreads=," + DBInsert.ThreadsCount + ",Query=," + S, DBInsert.ResultOutPath);

			// logSimpalQryOrInsetTimeToCSV(start, end, "Query
			// ID=,"+qry.get(i)[0],"Start_DATETIME=,"+dtf.format(start)+",End_DATETIME=,"+dtf.format(end)+",
			// no. of records in the query result = ,"+ no_of_rows+", Count in results (if
			// count(*)) = ,"+S2, ResultOutPath);
			System.out.println(Q_ID + ", \n no. of records in the result =," + no_of_rows
					+ ", Count in results (if count(*))= ," + S2 + ", Query=," + S);
			no_of_rows = 0;

			DBInsert.ThreadsCount--;
			// conRawN.close();
			conSQL2N.close();
		} catch (SQLException e) {
			System.out.println("Error");
			List<String[]> reclist = new ArrayList<>();
			String[] rec = new String[2];
			rec[0] = "SQL exception occured" + e;
			reclist.add(rec);
			System.out.println("\n Error1= " + e.getMessage() + "\n SQL Ex=" + e.getSQLState() + "\n Other = "
					+ e.getLocalizedMessage());
			S2 = "\n Error= ," + e.getMessage() + "\n SQL Ex=" + e.getSQLState() + "\n Other = "
					+ e.getLocalizedMessage();
			// conRaw = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om",
			// "om", "om");

			// conSQL2 = DriverManager.getConnection("jdbc:postgresql://localhost:5432/om2",
			// "om", "om");
			// return reclist;
		}
	}
}
