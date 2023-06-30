import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ThreadSubClassIO_QueryRM extends Thread {
	private String t_cmd="";
	private String t_name="";
	
	 public void IoThreadParam(String thread_name, String thread_cmd) {
	     this.t_name = thread_name;   
		 this.t_cmd = thread_cmd;
	}
	 
	@Override
	public void run() {
		try 
		{
			System.out.println("RM ThreadSubClassIO is running: "+t_name);
			System.out.println("Thread x.0: "+t_cmd); 				
			
		//	DBInsert DBinsObj = new DBInsert();
		//	DBInsert.writeCSV(DBInsert.ResultOutPath, "Write DISK IO Usage", true);
			int i=0;
			String rec = "";
			String subrec="";
			
			DBInsert.RMList.clear();
			LocalDateTime nowD = LocalDateTime.now();
			Process cmdProc;
			
			System.out.println("Thread x.1: "+t_name); 
			//cmdProc = Runtime.getRuntime().exec("sudo iotop -b -o -d 1");  //sudo iotop -b -o -d 1 -a // -b batch, -o processes using disk, -d dealy 1sec, -a cumulative total disk read/write
			//cmdProc = Runtime.getRuntime().exec("sudo iotop -b -o -d 1 -a"); //new thread needed
				
			//cmdProc = Runtime.getRuntime().exec(t_cmd); //worked but error when "| grep Total" was added, //also helpful in running .sh files by giving full path but skips 60sec RM results same as new String[]{"sh","-c", t_cmd}
			cmdProc = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",t_cmd});   //error with grep, it was skipping 60sec of RM results data when delay/freq. is 1sec
			
			//cmdProc = Runtime.getRuntime().exec("iostat -x -t 1");  //
		
			BufferedReader stdoutReader = new BufferedReader(
			         new InputStreamReader(cmdProc.getInputStream()));
			String line;
			String MyRMline="";
			String time="";
			//String MyIOline="";
			
			int j=0;
			int p=0;
			while ((line = stdoutReader.readLine()) != null) {
				i=i+1;
								
				nowD = LocalDateTime.now();
				if(line.contains("%Cpu(s)"))
				{					
					subrec = t_name +",TTIME=,"+Duration.between(DBInsert.Exp_startDateTime, nowD).toMillis()+","+DBInsert.DBMS_Type+ ","+ DBInsert.Records_In_DB + ","+DBInsert.Data_Op_ID+ ","+DBInsert.Data_Op+ ","+DBInsert.dtf.format(DBInsert.Exp_startDateTime)+","+DBInsert.dtf.format(nowD)+",";
					rec=subrec+ MyRMline.trim().replaceAll(" +", ",");
					time="TTIME=,"+Duration.between(DBInsert.Exp_startDateTime, nowD).toMillis();
					DBInsert.RMList.add(rec.split(","));

				   
				// process procs standard output here
				//	System.out.println(rec); 
					
					//DBMS_Type , Data_Op_ID, Data_Op, Exp_startDateTime, Record Observe Current DateTime, RESOURCE Records
					//below code was writing one by one. Now writeCSVB3 will bulk write all record in one go.
					//DBInsert.writeCSV(DBInsert.ResultOutPath, t_name + ","+DBInsert.DBMS_Type+ ","+DBInsert.Data_Op_ID+ ","+DBInsert.Data_Op+ ","+DBInsert.Exp_startDateTime+","+DBInsert.dtf.format(LocalDateTime.now())+","+ line.trim().replaceAll(" +", ","), true); 
					if(i>60)		 //Filtered records after each 100 lines, added in file		
					{
						try
						{
							DBInsert.writeCSVB3(DBInsert.ResultOutPath+"RM", DBInsert.RMList, true);
						}
						catch (Exception e) {
							e.printStackTrace();
							continue;
						}
						i=0;
						DBInsert.RMList.clear();
					}
					j=1;
					p=0;
					
				}				
				else if(line.contains("Total DISK"))
				{
					rec = t_name +",TTIME=,"+Duration.between(DBInsert.Exp_startDateTime, nowD).toMillis()+ ","+DBInsert.DBMS_Type+ ","+ DBInsert.Records_In_DB + ","+DBInsert.Data_Op_ID+ ","+DBInsert.Data_Op+ ","+DBInsert.dtf.format(DBInsert.Exp_startDateTime)+","+DBInsert.dtf.format(nowD)+","+ MyRMline.trim().replaceAll(" +", ",");
					time="TTIME=,"+Duration.between(DBInsert.Exp_startDateTime, nowD).toMillis();
					DBInsert.RMList.add(rec.split(","));
					
					if(i>70)		 //Filtered records after each 100 lines, added in file		
					{
						try
						{
							DBInsert.writeCSVB3(DBInsert.ResultOutPath+"RM", DBInsert.RMList, true);
						}
						catch(Exception e)
						{
							continue;
						}
						i=0;
						DBInsert.RMList.clear();
					}		
				}
				else if((line.contains("postgres") || line.contains("java")) && t_name.contains("CPU & RAM"))
				{
					int ip2=0;
					if(j<=5 && p<5) {
						j++;
						p++;   //take only top 5 most memory consuming processes(java or postgres only- incase of Multi-threading there will be multiple postgres processes) and discard the rest...
												
					String[] PgMEM = line.replaceAll(" +"," ").split(" "); //remove all blank spaces with single space
								
					if(line.contains("postgres"))
					{
						ip2= Arrays.asList(PgMEM).indexOf("postgres");
					}else
					{
						ip2= Arrays.asList(PgMEM).indexOf("java");
					}
					float PgM = Float.parseFloat(PgMEM[ip2-2]);// Memory-Postgres Procelse if((line.contains("postgres") || line.contains("java")) && t_name.contains("CPU & RAM"))ess
					String MyCPUline = "\n"+t_name+",CPU_RAM_PROCESS, PID"+PgMEM[0]+",CPU_1coreUsageByProcess=,"+PgMEM[ip2-3]+",Mem%=,"+PgM+","+PgMEM[ip2];
					
					DBInsert.RMList.add(MyCPUline.split(","));
					
				//	System.out.println("PostgresMem%=,"+ PgM);
					}

				}
				else if((line.contains("postgres") || line.contains("java")) && t_name.contains("IO_Total"))
				{
					String[] PgMEM = line.replaceAll(" +"," ").split(" "); //remove all blank spaces with single space
					
				//	System.out.println("line: Error Disk Read=="+line+"x"); //
				//	System.out.println("[3]x: Error Disk Read=="+PgMEM[4]+"x"); //
					float TR=0,TW=0;
					
					try
					{
						//System.out.println("[3]x:  Disk Read=="+PgMEM[(Arrays.asList(PgMEM).indexOf("K"))-1]+"x"); //[3]
					TR = (Float.parseFloat(PgMEM[(Arrays.asList(PgMEM).indexOf("K"))-1]))/1024;//Value DISK READ in K/s
					//System.out.println("[5]x:  Disk Read=="+PgMEM[(Arrays.asList(PgMEM).indexOf("K"))+1]+"x"); //
					TW = (Float.parseFloat(PgMEM[(Arrays.asList(PgMEM).indexOf("K"))+1]))/1024;//Val DISK WRITE in K/s.  //7 swap% //9 IO%//
					}
					catch(Exception e)
					{}
					//String[] IOR = line.replaceAll(" +","_").split("%");
					
					//String MyIOline = "\nIO_PROCESS,"+time+",DISK_READ_MBperSec=,"+TR+",DISK_WRITE_MBperSec=,"+TW+",SWAPIN%=,"+PgMEM[8]+",IO>%=,"+PgMEM[10]+",ProcessName=,"+IOR[2];
					String MyIOline = "\n"+t_name+",IO_PROCESS, PID"+PgMEM[0]+",DISK_READ_MBperSec=,"+TR+",DISK_WRITE_MBperSec=,"+TW;//+",SWAPIN%=,"+PgMEM[8]+",IO>%=,"+PgMEM[10]+",ProcessName=,"+IOR[2];
					
					//System.out.println("MMM: Error Disk Read=="+MyIOline+"x"); //

					DBInsert.RMList.add(MyIOline.split(","));
					
					
				}
		
			}
	
			System.out.println("Thread x.2: "+t_name); 
			//subThread.isInterrupted();  //Not working. checks if Gets interrupt signal from main and stops this thread immediately, no need of return
			
			BufferedReader stderrReader = new BufferedReader(
			         new InputStreamReader(cmdProc.getErrorStream()));
			while ((line = stderrReader.readLine()) != null) {
			   // process procs standard error here
				System.out.println(line); 
			}
			//cmdProc = Runtime.getRuntime().exec("Ctrl+C");
			
		//	int retValue = cmdProc.exitValue();
			System.out.println("Thread x.3: "+t_name); 
			System.out.println("ThreadSubclassIO is Ended: "+t_name);
	//new Code 24-Apr-2019
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("error ThreadSubclassIO is Ended");
		}
	}	
}
