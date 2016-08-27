package neu.mr;

import java.io.InputStream;

public class Test {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	//	Process child = Runtime.getRuntime().exec(new String[] {"RScript", "/Users/Surekha/A42.R", "/Users/Surekha/output"});
		Process child = Runtime.getRuntime().exec(new String[] {"/usr/bin/R", "CMD", "BATCH", "/Users/Surekha/A42.R", "--args", "/Users/Surekha/output"});
		
		//Process child = Runtime.getRuntime().exec("/usr/bin/R CMD BATCH /Users/Surekha/Documents/NEU/Courses/MapReduce/Jadhwani_Singh_A4/A4local.R '--args /Users/Surekha/Documents/NEU/Courses/MapReduce/Jadhwani_Singh_A4/output'");
		Thread.sleep(2000);
        int code = child.waitFor();
		
        System.out.println(child.exitValue());
        
        switch (code) {
            case 0:
            	System.out.println("Done");
                //normal termination, everything is fine
                break;
            case 1:
            	System.out.println("!Done");
                //Read the error stream then
            	InputStream error = child.getErrorStream();
            	   for (int i = 0; i < error.available(); i++) {
            	   System.out.println("" + error.toString());
            	   }
                
            default:
            	System.out.println(child.exitValue());
            	System.out.println(child.getErrorStream().toString());
        }
	}

}
