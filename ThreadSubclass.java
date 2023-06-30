import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ThreadSubclass extends Thread {

	@Override
	public void run() {		
		try 
		{
			System.out.println("ThreadSubclass is running");
			System.out.println("0"); 			
		//	DBInsert DBinsObj = new DBInsert();
			DBInsert.writeCSV(DBInsert.ResultOutPath, "Write CPU Usage", true);
			Process cmdProc;
			//String S_Cmd="while read -r a b c d e f g h i j k l; do \\echo $a,$b,$c,$d,$e,$f,$g,$h,$i,$j,$k,$l; done < <(top -b -d 1 -H)";
			//cmdProc = Runtime.getRuntime().exec(S_Cmd);  //iostat -x -t 1 //top -b ->   //top -b -d -1 -H// -d 1->1sec delay, -H show all threads/ if -H removed shows per task/process no subthreads shown
			cmdProc = Runtime.getRuntime().exec("top -b -d 1 -H");
			
			System.out.println("1"); 
			BufferedReader stdoutReader = new BufferedReader(
			         new InputStreamReader(cmdProc.getInputStream()));
			String line;
			while ((line = stdoutReader.readLine()) != null) {
			   // process procs standard output here
			//	System.out.println(line); 
				DBInsert.writeCSV(DBInsert.ResultOutPath, "CPU & RAM,"+ line.trim().replaceAll(" +", ","), true);
				/*if( subThread.isInterrupted())
				{
					System.out.println("subThread.isInterrupted():"+subThread.isInterrupted());
					return;  //return is needed when subthread exists before main on its own.
				}*/
			}
			
			System.out.println("2"); 
			//subThread.isInterrupted();  //Not working. checks if Gets interrupt signal from main and stops this thread immediately, no need of return
			
			BufferedReader stderrReader = new BufferedReader(
			         new InputStreamReader(cmdProc.getErrorStream()));
			while ((line = stderrReader.readLine()) != null) {
			   // process procs standard error here
				System.out.println(line); 
			}
			//cmdProc = Runtime.getRuntime().exec("Ctrl+C");
			
		//	int retValue = cmdProc.exitValue();
			System.out.println("3"); 
			System.out.println("ThreadSubclass is Ended");
	//new Code 24-Apr-2019
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}