import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ThreadSubClassIO extends Thread {
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
					if(i>100)		 //Filtered records after each 100 lines, added in file		
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
					String[] CPU_Val = line.replaceAll(" +"," ").split(" ");
					
					//String[] CPU_Val = line.replaceAll("\\s+"," ").split(" ");  //not working sometimes
			/*		System.out.println("\n0CPUx="+line+"x"); 
				System.out.println("1x="+CPU_Val[1]+"x"); 
				System.out.println("5x"+CPU_Val[5]+"x");
				System.out.println("7x"+CPU_Val[7]+"x"); //Idle Value- Unused CPU
					System.out.println("8x"+CPU_Val[8]+"x"); //id symbol
					System.out.println("x9x"+CPU_Val[9]+"x");  // WA VALUE- CPU WAITING
					System.out.println("10x"+CPU_Val[10]+"x"); //wa symbol
					System.out.println("11x"+CPU_Val[11]+"x"); 
					System.out.println("12x"+CPU_Val[12]+"x"); 
					System.out.println("13x"+CPU_Val[13]+"x"); 
					System.out.println("14x"+CPU_Val[14]+"x"); */
					
					
				//	System.out.println("9x"+CPU_Val[9]+"x"); 
//
//					if(CPU_Val[7].contains("id"))
//					{
//						System.out.println("INSIDE IDDDDDDDDDD="+line+"x"); 
//
//						MyRMline = "\n CPU_USED=,"+CPU_Val[1]+" CPU_Unused=,"+CPU_Val[6]+",CPU_IOWait=,"+CPU_Val[8];
//						try
//						{
//							DBInsert.RM_AvailCPU = Float.parseFloat(CPU_Val[6]);
//							DBInsert.RM_IOWait = Float.parseFloat(CPU_Val[8]);
//						}
//						catch(Exception e)
//						{
//							System.out.print("error thread sub class CPU RM parsing");
//							System.out.print(line);
//							continue;
//						}
//
//					}
//					else
//					{
						//System.out.println("ELSE SIDE 0000000000000IDDDDDDDDDD="+line+"x"); 

						MyRMline = "\n CPU_USED=,"+CPU_Val[(Arrays.asList(CPU_Val).lastIndexOf("us,"))-1]+" CPU_Unused=,"+CPU_Val[(Arrays.asList(CPU_Val).lastIndexOf("id,"))-1]+",CPU_IOWait=,"+CPU_Val[(Arrays.asList(CPU_Val).lastIndexOf("wa,"))-1];
					
						try
						{
							DBInsert.RM_AvailCPU = Float.parseFloat(CPU_Val[(Arrays.asList(CPU_Val).lastIndexOf("id,"))-1]);
							DBInsert.RM_IOWait = Float.parseFloat(CPU_Val[(Arrays.asList(CPU_Val).lastIndexOf("wa,"))-1]);
						}
						catch(Exception e)
						{
							System.out.print("error thread sub class CPU RM parsing");
							System.out.print(line);
							continue;
						}
					//}
				}
				else if(line.contains("buff/cache"))
				{
					j=2;
					String[] MEM_Val = line.replaceAll(" +"," ").split(" ");
				/*	System.out.println("0x"+line+"x"); //
					System.out.println("1x"+MEM_Val[1]+"x"+Arrays.asList(MEM_Val).lastIndexOf("total,")); //
					System.out.println("3x"+MEM_Val[3]+"x"); //Value Total Memory
					System.out.println("4x"+MEM_Val[4]+"x"); //
					System.out.println("5x"+MEM_Val[5]+"x"); //Val Free Memory
					System.out.println("6x"+MEM_Val[6]+"x"); //

					System.out.println("7x"+MEM_Val[7]+"x"); //
					System.out.println("8x"+MEM_Val[8]+"x"); //
					System.out.println("9x"+MEM_Val[9]+"x");  // 
					System.out.println("10x"+MEM_Val[10]+"x"); //*/
					
					float TM = Float.parseFloat(MEM_Val[(Arrays.asList(MEM_Val).lastIndexOf("total,"))-1]);//Value Total Memory
					float FM = Float.parseFloat(MEM_Val[(Arrays.asList(MEM_Val).lastIndexOf("free,"))-1]);//[5]);//Val Free Memory
					float BM = Float.parseFloat(MEM_Val[(Arrays.asList(MEM_Val).lastIndexOf("buff/cache"))-1]);//[9]);//Buffer Memory

					float AM = ((FM+BM)/TM)*100;    //Available Memory
					//float AM = ((FM)/TM)*100;    //Available Memory
					
					DBInsert.RM_AvailMem = AM;
					MyRMline = MyRMline + ",Total_Memory=,"+MEM_Val[3]+",FreeMemory=,"+MEM_Val[5]+",MemoryAvialblein%=,"+AM+",Buff/Cache_Memory=,"+BM;

				//	System.out.println("Available memory in % ="+ AM); //

				}
				else if(line.contains("Total DISK"))
				{
					rec = t_name +",TTIME=,"+Duration.between(DBInsert.Exp_startDateTime, nowD).toMillis()+ ","+DBInsert.DBMS_Type+ ","+ DBInsert.Records_In_DB + ","+DBInsert.Data_Op_ID+ ","+DBInsert.Data_Op+ ","+DBInsert.dtf.format(DBInsert.Exp_startDateTime)+","+DBInsert.dtf.format(nowD)+","+ MyRMline.trim().replaceAll(" +", ",");
					time="TTIME=,"+Duration.between(DBInsert.Exp_startDateTime, nowD).toMillis();
					DBInsert.RMList.add(rec.split(","));
					
					if(i>100)		 //Filtered records after each 100 lines, added in file		
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
					
					String[] MEM_Val = line.replaceAll(" +"," ").split(" ");
				/*	System.out.println("0TDx"+line+"x"); 
					
					System.out.println("3x"+MEM_Val[3]+"x"); //

					System.out.println("4x="+MEM_Val[4]+"x"); //

					System.out.println("5x="+MEM_Val[5]+"x"); //
					System.out.println("10x="+MEM_Val[10]+"x"); //

					System.out.println("11x="+MEM_Val[11]+"x"); //
					System.out.println("12x="+MEM_Val[12]+"x"); //*/

					float TR = (Float.parseFloat(MEM_Val[(Arrays.asList(MEM_Val).lastIndexOf("READ"))+2]))/1024;//[4] Value Total READ in K/s
					float TW = (Float.parseFloat(MEM_Val[(Arrays.asList(MEM_Val).lastIndexOf("WRITE"))+2]))/1024;//[11] Val Free WRITE in K/s.
					
			//		System.out.println("TOTAL READ	 in % ="+ TR);
			//		System.out.println("TOTAL WRITE	 in % ="+ TW);
					MyRMline = "\nTOTAL_READ_in_MB=,"+ TR +",TOTAL_WRITE_in_MB=," + TW;
					//MyIOline = "TOTAL READ in MB % =,"+ TR +", TOTAL WRITE in MB % =," + TW;
				}
				else if((line.contains("postgres") || line.contains("java")) && t_name.contains("CPU & RAM"))
				{
					int ip2=0;
					if(j<=5 && p<5) {
						j++;
						p++;   //take only top 5 most memory consuming processes(java or postgres only- incase of Multi-threading there will be multiple postgres processes) and discard the rest...
												
					String[] PgMEM = line.replaceAll(" +"," ").split(" "); //remove all blank spaces with single space
			
					
					//	PgMEM.indexOf("java"); 
				/*	System.out.println("1x"+PgMEM[1]+"x"); //
					System.out.println("3x"+PgMEM[3]+"x"); //
					System.out.println("4x"+PgMEM[4]+"x"); //
					System.out.println("5x"+PgMEM[5]+"x"); //
					System.out.println("6x"+PgMEM[6]+"x"); //

					System.out.println("7x"+PgMEM[7]+"x"); //
					System.out.println("9xCPUx"+PgMEM[9]+"x"); //CPU- Single core
					System.out.println("10x"+PgMEM[10]+"x");  // Memory-Postgres Process
				//	System.out.println("10xTime"+PgMEM[10]+"x"); //Time
					System.out.println("11x"+PgMEM[11]+"x"); //Pg
	*/				
					if(line.contains("postgres"))
					{
						ip2= Arrays.asList(PgMEM).indexOf("postgres");
					}else
					{
						ip2= Arrays.asList(PgMEM).indexOf("java");
					}
					float PgM = Float.parseFloat(PgMEM[ip2-2]);// Memory-Postgres Procelse if((line.contains("postgres") || line.contains("java")) && t_name.contains("CPU & RAM"))ess
					String MyCPUline = "\nCPU_RAM_PROCESS, PID"+PgMEM[0]+",CPU_1coreUsageByProcess=,"+PgMEM[ip2-3]+",Mem%=,"+PgM+","+PgMEM[ip2];
					
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
					//	System.out.println("[3]x:  Disk Read=="+PgMEM[(Arrays.asList(PgMEM).indexOf("K"))-1]+"x"); //[3]
					TR = (Float.parseFloat(PgMEM[(Arrays.asList(PgMEM).indexOf("K"))-1]))/1024;//Value DISK READ in K/s
				//	System.out.println("[5]x:  Disk Read=="+PgMEM[(Arrays.asList(PgMEM).indexOf("K"))+1]+"x"); //
					TW = (Float.parseFloat(PgMEM[(Arrays.asList(PgMEM).indexOf("K"))+1]))/1024;//Val DISK WRITE in K/s.  //7 swap% //9 IO%//
					}
					catch(Exception e)
					{}
					//String[] IOR = line.replaceAll(" +","_").split("%");
					
					//String MyIOline = "\nIO_PROCESS,"+time+",DISK_READ_MBperSec=,"+TR+",DISK_WRITE_MBperSec=,"+TW+",SWAPIN%=,"+PgMEM[8]+",IO>%=,"+PgMEM[10]+",ProcessName=,"+IOR[2];
					String MyIOline = "\nIO_PROCESS, PID"+PgMEM[0]+",DISK_READ_MBperSec=,"+TR+",DISK_WRITE_MBperSec=,"+TW;//+",SWAPIN%=,"+PgMEM[8]+",IO>%=,"+PgMEM[10]+",ProcessName=,"+IOR[2];
					
				//	System.out.println("MMM: Error Disk Read=="+MyIOline+"x"); //

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
