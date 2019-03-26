/**
 * 
 */
package com.blazer.load.file.runner;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blazer.load.file.TransDataLoad;


/**
 * 一个线程导入数据文件
 * @author mhshi
 * 
 */
public class TransDataLoadFileRunnable implements Runnable{
	private static final Logger _logger = LoggerFactory.getLogger(TransDataLoadFileRunnable.class);
	int  	threadNumber =1;
	ArrayList <TransDataLoadFile>transDataLoadFileList;
	
	

	public TransDataLoadFileRunnable(int threadNumber,ArrayList <TransDataLoadFile>transDataLoadFileList) {
		super();
		this.transDataLoadFileList=transDataLoadFileList;
		this.threadNumber=threadNumber;
	}

	@Override
	public void run() {
			try {
				for (TransDataLoadFile csv :transDataLoadFileList) {
					csv.setThreadNumber(threadNumber);
					csv.run();
				}
				
				Thread.sleep(2000);
				
				successThread();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				_logger.error("--thread "+threadNumber+" -- Fail .");
				_logger.error("加载数据错误", e);
			}
			TransDataLoad.mCountDownLatch.countDown();
	}
	
	//TODO
	public  synchronized void successThread() {
		TransDataLoad.successThread[threadNumber-1]++;
		
		_logger.info("--thread "+threadNumber+" status "+TransDataLoad.successThread[threadNumber-1]+" -- Complete .");
	}


	public int getThreadNumber() {
		return threadNumber;
	}

	public void setThreadNumber(int threadNumber) {
		this.threadNumber = threadNumber;
	}

	public ArrayList<TransDataLoadFile> getTransDataLoadFileList() {
		return transDataLoadFileList;
	}

	public void setTransDataLoadFileList(ArrayList<TransDataLoadFile> transDataLoadFileList) {
		this.transDataLoadFileList = transDataLoadFileList;
	}

	


}
