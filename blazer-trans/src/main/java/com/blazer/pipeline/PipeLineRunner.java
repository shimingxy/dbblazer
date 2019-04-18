package com.blazer.pipeline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.beust.jcommander.JCommander;

public class PipeLineRunner {
	private static final Logger _logger = LoggerFactory.getLogger(PipeLineRunner.class);
	
	public static String OS=FAMILYOS.FAMILY_WINDOWS;
	public static ApplicationContext context;
	PipeLineRunnerArgs runnerArgs = new PipeLineRunnerArgs();
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args)  {
		PipeLineRunner runner=new PipeLineRunner();
		try{
			PathUtils.getInstance();
			//List Environment Variables
			runner.listEnvVars();
			new JCommander(runner.runnerArgs, args);
			runner.init(args);
			runner.execute(args);
			
		}catch(Exception e){
			e.printStackTrace();
		}
		return;
		
	}
	
	//Initialization ApplicationContext for Project
	public void init(String[] args){
		
		_logger.info("user.dir "+System.getProperty("user.dir"));
			
		if(runnerArgs.sqlWhere!=null){
			System.setProperty("SQL_WHERE",runnerArgs.sqlWhere);
		}
		if(runnerArgs.etlDate!=null){
			System.setProperty("ETL_DATE",runnerArgs.etlDate);
		}
		
		OS=(System.getenv("CURR_OS")==null?FAMILYOS.FAMILY_WINDOWS:System.getenv("CURR_OS".toLowerCase()));
		
		_logger.info("CURRENT OS "+OS);
			
		//System.setProperty("user.dir", "c:/");
		_logger.info("Application Context Configuration XML File  "+runnerArgs.config);
		if(runnerArgs.config == null) {
			context = new FileSystemXmlApplicationContext(new String[] {"/pipeline/applicationContext.xml"});
		}else {
			context = new FileSystemXmlApplicationContext(new String[] {"/pipeline/"+runnerArgs.config});
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public int execute(String[] args) throws Exception{
		_logger.info("execute...");
		ArrayList<PipeLineTask> pipeLineTaskList= (ArrayList<PipeLineTask>)context.getBean("pipeLineTask", ArrayList.class);
		
		for(PipeLineTask pipeLine : pipeLineTaskList){
			pipeLine.execute();
		}
		
		return 1;
	}
	
   public void listEnvVars(){
	   _logger.info("----------------------------------------------------------------------------------------------------");
	   _logger.info("List Environment Variables ");
	   Map<String, String> map = System.getenv();
	   for(Iterator<String> itr = map.keySet().iterator();itr.hasNext();){
		   String key = itr.next();
		   _logger.info(String.format("%-30s", key) + "   =" + map.get(key));
	   }   
	   _logger.info("----------------------------------------------------------------------------------------------------");
    
	   Properties properties = System.getProperties();
	   //遍历所有的属性
	   for (String key : properties.stringPropertyNames()) {
		   //输出对应的键和值
		   _logger.info(String.format("%-30s", key) + "   =" + properties.getProperty(key));
	   }
	   _logger.info("----------------------------------------------------------------------------------------------------");
   
	}
	   
}
