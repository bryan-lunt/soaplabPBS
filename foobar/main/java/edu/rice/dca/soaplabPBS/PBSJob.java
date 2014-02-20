package edu.rice.dca.soaplabPBS;
// SowaJob.java
//
// Created: April 2007
//
// Copyright 2007 Martin Senger
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//



import org.soaplab.share.SoaplabConstants;
import org.soaplab.share.SoaplabException;
import org.soaplab.services.cmdline.CmdLineJob;
import org.soaplab.services.Config;
import org.soaplab.services.Reporter;
import org.soaplab.services.JobState;
import org.soaplab.services.IOData;
import org.soaplab.services.metadata.MetadataAccessor;
import org.soaplab.services.JobState;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Enumeration;
import java.util.Properties;
import edu.rice.dca.soaplabPBS.*;
import java.io.*;

/**
 * This is a job executing external command-line tools. It represents
 * what was previously done by AppLab project. <p>
 *
 * It does not use, however, any "launcher". In the AppLab times, the
 * launcher was a Perl script that was a inter-mediator between a Java
 * class and an external program. This class calls the external tools
 * directly. <p>
 *
 * The class may also serve as a parent for those classes that need to
 * fiddle with command-line parameters before an external tool is
 * invoked. <p>
 *
 * @author <A HREF="mailto:martin.senger@gmail.com">Martin Senger</A>
 * @version $Id: SowaJob.java,v 1.15 2010/08/05 12:11:31 mahmutuludag Exp $
 */

public class PBSJob
    extends CmdLineJob {

    private static org.apache.commons.logging.Log log =
       org.apache.commons.logging.LogFactory.getLog (PBSJob.class);

    // control the init() method
    private static boolean jobClassInitialized = false;

    // process controlling the external program
    protected Process process;
    
    protected boolean terminated = false;
    
    public String[] qsubOptions;
    
    public String pbs_jid;
    public static final int QSTAT_INTERVAL = 2000;
    public static final boolean DEBUG = false;
    
    /**************************************************************************
     * The main constructor.
     **************************************************************************/
    public PBSJob (String jobId,
		    MetadataAccessor metadataAccessor,
		    Reporter reporter,
		    Map<String,Object> sharedAttributes,
		    boolean jobRecreated) throws SoaplabException {
	super(jobId, metadataAccessor, reporter, sharedAttributes, jobRecreated);

 	
    qsubOptions = metadataAccessor.getAnalysisDef().launcher.trim().split("\\s+");
    //DEBUG
    String foobar = "";
    for(int i = 0;i<qsubOptions.length;i++)
    	foobar = foobar + " " + qsubOptions[i];
    
    System.out.println("PBSJOB launcher = \"" + foobar + "\"");
	
	
	
    }

    /**************************************************************************
     *
     **************************************************************************/
    @Override protected synchronized void init()
	throws SoaplabException {
	super.init();

	if (jobClassInitialized)
	    return;
	jobClassInitialized = true;

	// here comes my own initialization (done just once for all jobs)

    }

    /**************************************************************************
     *
     **************************************************************************/
    @Override public void run()
	throws SoaplabException {
	if (! runAllowed())
	    return;

	checkInputs();
	if (log.isDebugEnabled())
	    log.debug ("Inputs check passed (" + inputs.size() + " inputs).");
	try {
        ProcessBuilder pb = getProcessBuilder();
        preExec (pb);
        Thread executeThread = getExecuteThread (pb);
        executeThread.start();
    } catch (RuntimeException e) {
    	// it looks helpful if we have the stack trace printed
    	SoaplabException.formatAndLog(e, log);
        internalError ("Problems with starting the job process: " + e.getMessage());        
    }
	synchronized (reporter){
	    while (reporter.getState().get() == JobState.CREATED)
	    try {
	        reporter.wait();
	    }
	    catch (InterruptedException e) {
	    }
	}
    }


    /**************************************************************************
     * Create and return a ProcessBuilder filled with the command-line
     * arguments and environment as defined in the metadata and user
     * input data for this job.
     *
     * It throws an exception when it was not able to fill the
     * ProcessBuilder.
     **************************************************************************/
    protected ProcessBuilder getProcessBuilder()
	throws SoaplabException {

	// command-line arguments
	ProcessBuilder pb = new ProcessBuilder (createArgs());
	pb.command().add (0, getExecutableName());

	// environment from several sources...
	Map<String,String> env = pb.environment();
	// ...from the user (as defined by the service metadata)
	addProperties (env, createEnvs());
	// ...from the service configuration
	addProperties (env,
		       Config.getMatchingProperties (Config.PROP_ENVAR,
						     getServiceName(), this));
	// ...combine the current PATH and Soaplab's addtopath properties
        addToPath (env, Config.getStrings (Config.PROP_ADDTOPATH_DIR,
					   null,
					   getServiceName(),
					   this));

	// working directory
	pb.directory (getJobDir());

	return pb;
    }

    /**************************************************************************
     * Update the environment variable PATH by our own directories
     * 'dirs' (taken originally from the Soaplab's property {@link
     * Config#PROP_ADDTOPATH_DIR}).
     *
     * Note that updating the PATH variable does not have any effect
     * on finding the executable - but it is made available to the
     * executable once the executable has been started (which solves,
     * for example, EMBOSS's wossname problem).
     *
     * The updated variable is stored in the given 'env'.
     **************************************************************************/
    protected void addToPath (Map<String,String> env,
			      String[] dirs) {
	if (dirs == null || dirs.length == 0)
	    return;
	String path = System.getenv ("PATH");
	if (path == null)
	    return;

	StringBuilder buf = new StringBuilder();
	for (String dir: dirs) {
	    buf.append (dir);
	    buf.append (File.pathSeparator);
	}
	buf.append (path);
	env.put ("PATH", buf.toString());
    }

    /**************************************************************************
     * Return a fully qualified executable name for this application -
     * if it can be found on the additions to the PATH (defined by
     * property PROP_ADDTOPATH_DIR) AND unless the executable already
     * has a fully qualified path.
     *
     * Note that this cannot be substituted by what the method
     * addToPath() does - because the environment variable PATH may be
     * changed only after the executable has been started.
     *
     * Otherwise return a plain executable name as it appears in the
     * service metadata.
     *
     * Throw an exception if the access to service metadata failed.
     **************************************************************************/
    @Override protected String getExecutableName()
	throws SoaplabException {

	String executable = super.getExecutableName();
	String[] paths = Config.getStrings (Config.PROP_ADDTOPATH_DIR,
					    null,
					    getServiceName(),
					    this);
	if (paths.length == 0 ||
	    new File (executable).isAbsolute())
	    return executable;

	// look into our (if any) additions to the PATH
	for (String path: paths) {
	    File file = new File (path, executable);
	    if (file.exists())
		return file.getAbsolutePath();
	}
 	return executable;
    }

    /**************************************************************************
     * Add the contents of 'props' to 'result'.
     **************************************************************************/
    private void addProperties (Map<String,String> result, Properties props) {
	for (Enumeration<Object> en = props.keys(); en.hasMoreElements(); ) {
	    String envName = (String)en.nextElement();
	    result.put (envName, props.getProperty (envName));
	}
    }

    /**************************************************************************
     * Called before the execution. Here, a sub-class has a chance to
     * change/add/remove some command-line arguments and/or
     * environment variables, or even the working directory (all of
     * these are in the given ProcessBuilder).
     *
     * The method can thrown an exception if the current set of
     * arguments is not good for being executed.
     **************************************************************************/
    protected void preExec (ProcessBuilder pb)
	throws SoaplabException {

	report ("Name: " + getServiceName());
	report ("Job ID: " + getId());
	StringBuilder buf = new StringBuilder();
	buf.append ("Program and parameters:\n");
	for (String str: pb.command()) {
	    buf.append (str);
	    buf.append ("\n");
	}
	buf.append ("--- end of parameters\n");
	report (buf.toString());
    }
    
    Thread executeThread = null;

    /**************************************************************************
     *
     **************************************************************************/
    protected synchronized Thread getExecuteThread (final ProcessBuilder pb)
	throws SoaplabException {
    	
        executeThread = new ExecuteThread (pb);

        return executeThread;
    }

    /**************************************************************************
     *
     **************************************************************************/
    protected void processResults (IOData[] ioData)
	throws SoaplabException {

	// find a result name (as defined in service metadata)
	for (IOData io: ioData) {
	    if (io.getDefinition().isOutput()) {
		File data = io.getData();
		if (data.length() > 0) {
		    reporter.setResult (io.getDefinition().id, data);
		}
	    }
	}
    }

    /**************************************************************************
     * Terminate this running job.
     **************************************************************************/
    public void terminate()
    throws SoaplabException {
		log.debug(getId() + " has been requested to terminate");
		synchronized (reporter) {
			if (process == null || terminated == true) {
				log.debug(getId() + " terminate request ignored");
				return;
			}
			terminated = true;
			PBSUtils.qdel(pbs_jid);
			try {
				// we shouldn't wait here much, under normal circumstances above
				// process.destroy() call should return fairly quickly
				reporter.wait(SoaplabConstants.JOB_TIMEOUT_FOR_TERMINATE_CALLS);
			} catch (InterruptedException e) {
			}
			if(process==null)
			    return;
			try {
				int ev = process.exitValue();
				log.debug(getId() + " process exit value: " + ev);
			} catch (IllegalThreadStateException e) {
				log.debug(getId() + " second call for process.destroy()");
				// we need this second call for LSF jobs as LSF replaces bsub
				// process by a bkill process
				// and the second process doesn't always dies smoothly,
				// especially when using LSF 7
				PBSUtils.qdel(pbs_jid);
			}
			try {
				process.exitValue();
			} catch (IllegalThreadStateException e) {
				// if it was not possible to terminate the process
				// then interrupt the execute thread
				executeThread.interrupt();
			}
		}
	}
    
    /**************************************************************************
     *
     * A thread invoking an external process and waiting for its completion
     *
     **************************************************************************/
    protected class ExecuteThread
	extends Thread {

    ProcessBuilder qsubpb;
	ProcessBuilder pb;
	
	File stdout2Report;
	File stderr2Report;
	IOData[] ioData;

	boolean running = false;

	public ExecuteThread (final ProcessBuilder pb) throws SoaplabException {
	    this.pb = pb;
	    setName (getName().replace ("Thread", "PBSSowaRunner"));

	    // create IO streams (as defined in service metadata)
	    ioData = createIO();


		// catch STDOUT and STDERR even if not defined in service metadata
	    stdout2Report = new File (getJobDir(), "stdout");
	    stderr2Report = new File (getJobDir(), "stderr");



	    
	    /*Copy the ProcessBuilder*/
	    try{
	    	String[] finalOptions = new String[qsubOptions.length + 1];
	    	for(int i = 0;i<qsubOptions.length;i++){
	    		finalOptions[i] = qsubOptions[i];
	    	}
	    	finalOptions[finalOptions.length-1] = "-N" + getId();
	    	
	    	this.qsubpb = PBSUtils.createQsubProccessBuilder(pb,finalOptions,getJobDir(),stdout2Report,stderr2Report);
	    }catch(Exception e){internalError("Problems creating a script for qsub : " + e.getMessage());}
	    
	    if(DEBUG){
	    	System.out.println("PBSJOB : About to run: " + qsubpb.command());
	    	System.out.println("PBSJOB : The script is : " + pb.command());
	    }
	}

	public void run() {
	    try {
			// submit via qsub

			process = qsubpb.start();
			

	
			// wait until the qsub process ends
			int qsubexitCode = -324;
			while (true) {
			    try {
			    	qsubexitCode = process.waitFor();//waiting for qsub to finish
			    	break;
			    } catch (InterruptedException e) {/*Don't care*/}
			}
		
			
			
			pbs_jid = PBSUtils.readAll(process.getInputStream());
			
			int jobExitCode = qsubexitCode;
			if(qsubexitCode == 0){
				running = true;
				synchronized (reporter) {
				    reporter.getState().set (JobState.CREATED);
				    reporter.notifyAll();
				}
				
			}else{
				running = false;
				synchronized (reporter) {
				    reporter.getState().set (JobState.TERMINATED_BY_ERROR);
				    reporter.report("Problem Submitting Job : ", PBSUtils.readAll(process.getErrorStream()));
				    reporter.notifyAll();
				}
			}
			
			int currJobState = JobState.CREATED;
			/*this just makes it wait a bit so that it doesn't try to get job status before it shows up in qsub output.
			Really, it should be something like an FSA, it polls first starting for the info to be available, then it changes mode to waiting for it to finish.
			*/
			
			initialwait:
			for(int i=0;i<30&&running;i++){
				try{
					currJobState = PBSUtils.get_job_status(pbs_jid);
					if(currJobState != JobState.UNKNOWN){
						synchronized(reporter){
							reporter.getState().set(currJobState);
							reporter.notifyAll();
							break initialwait;
						}
					}
					Thread.sleep(500);
				}catch(InterruptedException e){}
			}
			
			while(!terminated && running) {
				try{
						currJobState = update_status(true);
					if(running)//if not running, we want to terminate immediately.
						Thread.sleep(QSTAT_INTERVAL);
				}catch(InterruptedException e){}
			}
			
			reportOutput();
			jobExitCode = PBSUtils.get_job_exitcode(pbs_jid);
			postJobOutputAndReporting(jobExitCode);
			
	    }catch(Exception e){e.printStackTrace();}		
	}
 
	private int update_status(boolean andTerminate){
		int currJobState = PBSUtils.get_job_status(pbs_jid);
		
		if(DEBUG){System.out.println("PBSJOB : Got Job State : " + currJobState);}
		
		synchronized (reporter) {
			reporter.getState().set(currJobState);
			
				if(andTerminate && (currJobState == JobState.COMPLETED || currJobState == JobState.TERMINATED_BY_ERROR)) {
					running = false;
				}
		    reporter.notifyAll();
		}
		return currJobState;
	}
	
    protected void reportOutput() {


    			// put STDOUT and STDERR to report
    			// (if not defined in the service metadata)
    			if (this.stdout2Report != null && stdout2Report.length() > 0)
    			    reporter.report ("Standard output stream", stdout2Report);
    			if (this.stderr2Report != null && stderr2Report.length() > 0) {
    			    error ("Some error messages were reported.");
    			    reporter.report ("Standard error stream", stderr2Report);
    			}
    	}
    
    private void postJobOutputAndReporting(int exitCode) {
    	// process exit code and results
    			try{
	    			report ("Exit: " + exitCode);
	    			reporter.getState().setDetailed ("" + exitCode);
	    			if ( exitCode == 0 ||
	    			     Config.isEnabled (Config.PROP_ACCEPT_ANY_EXITCODE,
	    					       false,
	    					       getServiceName(),
	    					       PBSJob.this) ) {
	    			    processResults (ioData);
	    			    reporter.getState().set (JobState.COMPLETED);
	    			}
	    			else if (process == null){
	    	        	reporter.getState().set(JobState.TERMINATED_BY_REQUEST);
	    	        }
	    			else {
	    			    reporter.getState().set (JobState.TERMINATED_BY_ERROR);
	    			}
	    			
    		    } catch (SoaplabException e) {
	    			error (e.getMessage());
	    			log.error (e.getMessage());
	    			reporter.getState().set (JobState.TERMINATED_BY_ERROR);

    		    } catch (RuntimeException e) {
	    			error (INTERNAL_ERROR + e.getMessage());
	    			reporter.getState().set (JobState.TERMINATED_BY_ERROR);
	    			SoaplabException.formatAndLog (e, log);
    			
	    		} finally {
	    			try {
	    			    reporter.setDetailedStatusResult();
	    			    reporter.setReportResult();
	    			} catch (SoaplabException e) {
	    			    log.error ("Setting special results failed. " + e.getMessage());
	    			}
	    			finally {
		    			// inform other threads waiting for the termination that
		    			// it has been done
		    			synchronized (reporter) {
		    			    if (process != null){
		    			    	process.destroy();
		    			    	process = null;
		    			    }
		    			    reporter.notifyAll();
	    			}
	    			}
    		    }
    }
    
    }


    
    /**************************************************************************
     *
     * CopyThread - reading/writing standard stream from/to external process
     *
     **************************************************************************/
    protected class CopyThread extends Thread {

	final InputStream inStream;
	final OutputStream outStream;

	public CopyThread (InputStream is, OutputStream os) {
	    inStream = is;
	    outStream = os;
	    setName (getName().replace ("Thread", "SowaCopier"));
	}

	public void run() {
	    try {
		IOUtils.copy (inStream, outStream);
	    } catch (Exception e) {
		synchronized (reporter){
		    reporter.getState().set (JobState.TERMINATED_BY_ERROR);
		    if (process != null){
		        // if the process has already been destroyed it is normal to get errors
		        // no need to report in that case
		        error ("Error when copying data to/from the analysis: " + e.toString());
		        terminated=true;
		        process.destroy();
		        process = null;
		    }
		}
	    } finally {
		IOUtils.closeQuietly (inStream);
		IOUtils.closeQuietly (outStream);
	    }
	}
    }

}
