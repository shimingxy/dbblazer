/**
 * 
 */
package com.blazer.load.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.load.file.runner.TransDataLoadFile;
import com.blazer.pipeline.FAMILYOS;
import com.blazer.pipeline.PipeLineRunner;
import com.blazer.pipeline.PipeLineTask;



/**
 * 合并多个文件夹下同类型的文件<br>
 * 1、一般小文件合并成大文件，文件的文件名相同<br>
 * 2、合并仅生成操作系统合并命令<br>
 * 3、需要通过外部命令执行完成合并工作<br>
 * @author mhshi
 * @version 1.0
 * 
 */
public class TransDataFileMerge implements PipeLineTask{
	private static final Logger _logger = LoggerFactory.getLogger(TransDataFileMerge.class);
	DataSource sourceDataSource;
	
	int threadSize = 1;
	
	String fromUrl;
	String fromUser;
	
	int    useFord;
	String loadFilePath;
	String mergeCmd="mergedata";
	
	ArrayList <TransDataLoadFile>transDataLoadFileList;

	FileOutputStream fop = null;
	SimpleDateFormat sdf_ymdhms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	int fileNameUseSysdate = 0;

	public int execute() throws Exception {
		_logger.info("-- --From URL " + fromUrl+" , USER "+fromUser);
		
		String startTime=sdf_ymdhms.format(new Date());
		
		String envEtlDate= System.getProperty("ETL_DATE" );
	
		//替换日期
		if(envEtlDate!=null&&!envEtlDate.equals("")){
			if(fileNameUseSysdate==0) {
				//loadFileName=outFileName_a+envEtlDate+outFileName_c;
			}
		}else {
			/**
			 * TODO 
			 */
		}
		
		//文件名变换为小写
		//loadFileName=loadFileName.toLowerCase();
		//if(threadSize==5) return;
		//_logger.info("--thread  outFileName "+loadFileName+loadFileName+fileNameSuffix);
		
		_logger.info("-- loadFilePath  : "+loadFilePath);
		
		//读取指定目录文件下的文件夹，在运行前必须把*.tar or *.zip文件解压缩到多个目录
		File loadFilePathName = new File(loadFilePath);
		File [] loadFords=loadFilePathName.listFiles();
		ArrayList<File>  fordList=new ArrayList<File>();
		for(File currFile : loadFords) {
			if(currFile.isDirectory()) {//仅处理目录文件
				fordList.add(currFile);
			}
		}
		String cmdString="";
		
		_logger.info(" CURR_OS "+PipeLineRunner.OS);
		//判断操作系统的类型，创建不同的操作系统命令执行文件
		if(PipeLineRunner.OS.startsWith(FAMILYOS.FAMILY_WINDOWS)){
			cmdString=loadFilePath+mergeCmd+".bat";
		}else {
			cmdString=loadFilePath+mergeCmd+".sh";
		}
		//如果命令文件存在，则先删除再创建新文件
		File cmdFilePathName = new File(cmdString);
		if(cmdFilePathName.exists()){
			cmdFilePathName.delete();
		}
		cmdFilePathName.createNewFile();
		this.fop = new FileOutputStream(cmdFilePathName);
		//循环遍历所有文件类型
		for(  TransDataLoadFile dataFile : transDataLoadFileList) {
			mergeCmd(dataFile,fordList);
		}
		fop.flush();
		fop.close();
	
		_logger.info("-- --Start at " + startTime+" , End at  "+sdf_ymdhms.format(new Date()));
		
		return 0;
		
	}
	/**
	 * 生成多文件夹下同一类型文件合并的命令
	 * @param csv
	 * @param fordList
	 * @throws IOException
	 */
	public void mergeCmd(TransDataLoadFile dataFile ,ArrayList<File>  fordList) throws IOException {
		//TODO
		int count=1;
		StringBuffer sfile=new StringBuffer("");
		StringBuffer windowsMoveFileCmd=new StringBuffer("");
		if(PipeLineRunner.OS.startsWith(FAMILYOS.FAMILY_WINDOWS)){
			fop.write(("REM  merge "+dataFile.getLoadFileName()+"\r\n").getBytes());
		}else {
			fop.write(("#   merge "+dataFile.getLoadFileName()+"\n").getBytes());
		}
		for (File fordName : fordList) {
			String datFileString=fordName.getPath().replace("\\", "/")+"/"+dataFile.getLoadFileName()+""+dataFile.getFileNameSuffix();
			File datFile=new File(datFileString);
			if(datFile.exists()&&datFile.length()>0){
				if(PipeLineRunner.OS.startsWith(FAMILYOS.FAMILY_WINDOWS)){
					//copy file
					if(count!=fordList.size()){
						sfile.append(fordName.getName()).append("_").append(dataFile.getLoadFileName()).append("").append(dataFile.getFileNameSuffix() ).append("+");
					}else{
						sfile.append(fordName.getName()).append("_").append(dataFile.getLoadFileName()).append("").append(dataFile.getFileNameSuffix() ).append("");
					}
					//move data file
					windowsMoveFileCmd
							.append("cd ").append(fordName.getPath().replace("\\", "/"))
							.append("\r\n")
						    .append("move ").append(dataFile.getLoadFileName()).append("").append(dataFile.getFileNameSuffix())
						    		.append(" ")
						    		.append(loadFilePath).append(fordName.getName()).append("_").append(dataFile.getLoadFileName()).append("").append(dataFile.getFileNameSuffix())
						    .append("\r\r\n");
					//fop.write(("cd "+fordName.getPath().replace("\\", "/")+"\r\n").getBytes());
					//fop.write(("move "+csv.getLoadFileName()+""+csv.getFileNameSuffix()+" "+loadFilePath+fordName.getName()+"_"+csv.getLoadFileName()+""+csv.getFileNameSuffix()+"\r\r\n").getBytes());
				}else{
					//cat file
					sfile.append(datFileString).append(" ");
				}
			}
			count++;
		}
		String cmdString="";
		String target_fileString=dataFile.getLoadFileName()+""+dataFile.getFileNameSuffix();
		if(PipeLineRunner.OS.startsWith(FAMILYOS.FAMILY_WINDOWS)){
			cmdString ="copy  "+sfile+"  "+target_fileString+"\r\n";
			fop.write((windowsMoveFileCmd.toString()).getBytes());
			fop.write(("cd "+loadFilePath+"\r\n").getBytes());
			_logger.info("-- move  : \r\n"+windowsMoveFileCmd.toString());
			_logger.info("\r\ncd "+loadFilePath+"\r\n");
		}else {
			cmdString ="cat "+sfile+" > "+loadFilePath+target_fileString+"\n";
		}
		if(sfile.length()>10) {
			fop.write((cmdString).getBytes());
			_logger.info("-- copy  : \r\n"+cmdString);
		}
		//write one cmd line
	}
	
	public DataSource getSourceDataSource() {
		return sourceDataSource;
	}

	public void setSourceDataSource(DataSource sourceDataSource) {
		this.sourceDataSource = sourceDataSource;
	}

	public int getThreadSize() {
		return threadSize;
	}

	public void setThreadSize(int threadSize) {
		this.threadSize = threadSize;
	}

	public String getFromUrl() {
		return fromUrl;
	}

	public void setFromUrl(String fromUrl) {
		this.fromUrl = fromUrl;
	}

	public String getFromUser() {
		return fromUser;
	}

	public void setFromUser(String fromUser) {
		this.fromUser = fromUser;
	}

	public int getUseFord() {
		return useFord;
	}

	public void setUseFord(int useFord) {
		this.useFord = useFord;
	}

	public String getLoadFilePath() {
		return loadFilePath;
	}

	public void setLoadFilePath(String loadFilePath) {
		this.loadFilePath = loadFilePath;
	}

	public int getFileNameUseSysdate() {
		return fileNameUseSysdate;
	}

	public void setFileNameUseSysdate(int fileNameUseSysdate) {
		this.fileNameUseSysdate = fileNameUseSysdate;
	}

	public ArrayList<TransDataLoadFile> getTransDataLoadFileList() {
		return transDataLoadFileList;
	}
	public void setTransDataLoadFileList(ArrayList<TransDataLoadFile> transDataLoadFileList) {
		this.transDataLoadFileList = transDataLoadFileList;
	}
	public String getMergeCmd() {
		return mergeCmd;
	}

	public void setMergeCmd(String mergeCmd) {
		this.mergeCmd = mergeCmd;
	}


}
