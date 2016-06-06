package com.pa2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is used to write data into temporary file.
 * @author priyanka
 *
 */
public class WriteRunner implements Runnable {
	private List<ConcurrentLinkedQueue<List<String>>> concurrentLinkedQueueWrite;
	private Map<ConcurrentLinkedQueue<List<String>>, BufferedWriter> map = new HashMap<ConcurrentLinkedQueue<List<String>>, BufferedWriter>();
	private volatile boolean finished;
	private String tempDirName;
	private String[] fileNames;
	private int start;
	private int end;

	public WriteRunner(List<ConcurrentLinkedQueue<List<String>>> concurrentLinkedQueueWrite, String tempDirName,
			String[] fileNames, int s1, int e1) {
		super();
		this.concurrentLinkedQueueWrite = concurrentLinkedQueueWrite;
		this.tempDirName = tempDirName;
		this.fileNames = fileNames;
		this.start = s1;
		this.end = e1;
	}

	public void init() {
		try {
			File dir = new File(tempDirName);
			for (int i = start; i < end; i++) {
				File file = new File(dir, fileNames[i]);
				if (!file.exists()) {
					file.createNewFile();
				}
				map.put(concurrentLinkedQueueWrite.get(i), new BufferedWriter(new FileWriter(file)));

			}
		} catch (IOException e) {
			System.out.println(" Error in writing file " + e.getMessage());
		}

	}

	/**
	 * This method will take the data from read queue and create temporary file corresponding to it.
	 */
	@Override
	public void run() {

		BufferedWriter bufferedWriter = null;
		init();

		try {
			while (true) {

				for (int i = start; i < end; i++) {
					if (concurrentLinkedQueueWrite.get(i).size() > 0) {
						List<String> stringList = concurrentLinkedQueueWrite.get(i).poll();
						if (stringList != null && !stringList.isEmpty()) {

							for (String string : stringList) {
								try {
									bufferedWriter = map.get(concurrentLinkedQueueWrite.get(i));
									bufferedWriter.write(string + "\r\n");
								} catch (IOException e) {
									e.printStackTrace();
								}
								finally{
									if(bufferedWriter!=null){
										bufferedWriter.flush();
									}
								}
								
							}
							stringList.clear();
							stringList = null;
						}
					}
				}
					
				
				if (allqueueempty(concurrentLinkedQueueWrite) && isFinished()) {
					map.clear();
					map = null;
					break;
				}

			}
			
			if (bufferedWriter != null) {
				bufferedWriter.flush();
				bufferedWriter.close();
			}

			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	public boolean allqueueempty(List<ConcurrentLinkedQueue<List<String>>> concurrentLinkedQueueWrite) {
		for (int i = start; i < end; i++) {
			if (concurrentLinkedQueueWrite.get(i).size() > 0) {
				return false;
			}
		}
		return true;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}
	
	

}
