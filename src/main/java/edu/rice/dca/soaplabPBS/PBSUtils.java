package edu.rice.dca.soaplabPBS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.soaplab.services.JobState;

public class PBSUtils {

	private static final boolean DEBUG = false;
	
	private static Pattern job_status_pattern;
	private static Pattern job_exitval_pattern;
	private static Map<String,Integer> pbs_status_to_soaplab_status;
	
	static{
		job_status_pattern = Pattern.compile("job_state[ ]*=[ ]*(.)");
		job_exitval_pattern = Pattern.compile("exit_status[ ]*=[ ]*(.)");
		
		pbs_status_to_soaplab_status = new HashMap<String,Integer>();
		pbs_status_to_soaplab_status.put("Q", new Integer(JobState.CREATED));
		pbs_status_to_soaplab_status.put("R", new Integer(JobState.RUNNING));
		pbs_status_to_soaplab_status.put("C", new Integer(JobState.COMPLETED));
		pbs_status_to_soaplab_status.put("E", new Integer(JobState.TERMINATED_BY_ERROR));
		pbs_status_to_soaplab_status.put("U", new Integer(JobState.UNKNOWN));
	}
	
	public static int get_job_exitcode(String jobid){
		String info = runQstat(jobid);
		int exit_status = 0;
		
		Matcher exitStatusMatcher = job_exitval_pattern.matcher(info);
		if(exitStatusMatcher.find()){
			exit_status = Integer.parseInt(exitStatusMatcher.group(1));
		}
		
		return exit_status;
	}
	
	public static int get_job_status(String jobid){
		String qstat_status = "U";
		String info = runQstat(jobid);
		int exit_status = 0;
		
		Matcher myMatcher = job_status_pattern.matcher(info);
		if(myMatcher.find()){
			qstat_status = myMatcher.group(1);
		}
		
		Matcher exitStatusMatcher = job_exitval_pattern.matcher(info);
		if(exitStatusMatcher.find()){
			exit_status = Integer.parseInt(exitStatusMatcher.group(1));
		}
		
		if(DEBUG){System.out.println("Job Status : " + qstat_status);}
		
		//Special logic for jobs that completed with error while running.
		//PBS does not report errors if they are not _PBS_ errors, but it does get returnvalue.
		if(0 != exit_status && ("C".equals(qstat_status) || "E".equals(qstat_status)) ){
			qstat_status = "E";
		}
		
		if(pbs_status_to_soaplab_status.containsKey(qstat_status))
			return pbs_status_to_soaplab_status.get(qstat_status).intValue();
		else
			return JobState.UNKNOWN;
	}
	
	public static void qdel(String jobid){
		try{
			ProcessBuilder pb = new ProcessBuilder();
			pb.command().add("qdel");
			pb.command().add(jobid);
			Process qdel = pb.start();
		}catch(Exception e){/*just return, don't even wait for the process to finish*/}
	}
	
	public static String runQstat(String jobid){
		try{
		ProcessBuilder pb = new ProcessBuilder();
		pb.command().add("qstat");
		pb.command().add("-f");
		if(jobid != null){
			pb.command().add(jobid);
		}
		Process qstat = pb.start();
		
		int qstatExitCode = -324;
		while (true) {
		    try {
		    	qstatExitCode = qstat.waitFor();//waiting for qsub to finish
		    	break;
		    } catch (InterruptedException e) {/*Don't care*/}
		}
		
		String qstatret = readAll(qstat.getInputStream());
		return qstatret;
		}catch(Exception e){}
		return "";
	}
	
	
	public static String readAll(InputStream s){
		String retstr = "";
		try{
			StringBuilder outBuilder = new StringBuilder();
			BufferedReader br = new BufferedReader(new InputStreamReader(s));
			String tmp = br.readLine();
			while(tmp != null){
				outBuilder.append(tmp.trim());
				tmp = br.readLine();
			}
			retstr = outBuilder.toString();
		}catch(Exception e){e.printStackTrace();}
		return retstr;
	}
	
	public static ProcessBuilder createQsubProccessBuilder(ProcessBuilder pb, String[] otherOptions, File jobDir, File stdout2Report, File stderr2Report) throws java.io.IOException {
		 /*Copy the ProcessBuilder*/
	    ProcessBuilder qsubpb = new ProcessBuilder();
	   //Don't copy the command! Just the environment and directory and stuff!
	    qsubpb.environment().putAll(pb.environment());
	    qsubpb.directory(pb.directory());
	    qsubpb.redirectErrorStream(pb.redirectErrorStream());
	    
	    //Setup the qsub command
	    qsubpb.command().clear();
	    qsubpb.command().add("/usr/bin/qsub");
	    
	    for(int i = 0;i<otherOptions.length;i++){
	    	String anOtherOption = otherOptions[i].trim();
	    	if(anOtherOption != null && !anOtherOption.equals(""))//Blank or empty options break things.
	    		qsubpb.command().add(anOtherOption);
	    }
	    
	    qsubpb.command().add("-d" + jobDir.getAbsolutePath());
	    
	    qsubpb.command().add("-o" + stdout2Report.getAbsolutePath());
	    
	    qsubpb.command().add("-e" + stderr2Report.getAbsolutePath());
	    
	    qsubpb.command().add("-V");//Because we used a copied process builder, it now gets all the environment variables it would have gotten anyway.
	    
	    qsubpb.command().add("__thescript");//Script file to qsub...
	    
	    
	    //create the script
	    File script_file = new File(jobDir,"__thescript");
	    PrintWriter fw = new PrintWriter(new BufferedWriter(new FileWriter(script_file.getAbsoluteFile())));

	    List<String> theCommand = pb.command();
	    
	    for(int i = 0;i<theCommand.size();i++){
	    	fw.print(theCommand.get(i));
	    	fw.print(" ");
	    }
	    fw.println("");
	    fw.flush();
	    fw.close();
	    //done creating script
	    
	    return qsubpb;
	}
}
