package com.pa2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
 * This method will take the data from sort thread and write into final output files
 */
public class WriteSortRunner implements Runnable {
	private ConcurrentLinkedQueue<List<String>> writeQueue = new ConcurrentLinkedQueue<List<String>>();
	private String outputpath;
	private boolean finished;

	public WriteSortRunner(ConcurrentLinkedQueue<List<String>> writeQueue, String outputPath) {
		this.writeQueue = writeQueue;
		this.outputpath = outputPath;
	}

	/**
	 * This function will created the output files are write the sorted data into it
	 */
	@Override
	public void run() {
		System.out.println("started with write thread");
		BufferedWriter bufferedWriter = null;

		List<String> stringList = null;
		String fileName = null;
		File dir = null;
		File file = null;
		while (true) {
			if (writeQueue.size() > 0) {
				try {
					stringList = writeQueue.poll();
					if (stringList != null && !(stringList.isEmpty())) {
						fileName = getFileName(stringList.get(0).charAt(0));
						if (fileName != null) {

							dir = new File(outputpath);
							file = new File(dir, fileName);
							if (!file.exists()) {
								file.createNewFile();
							}

							bufferedWriter = new BufferedWriter(new FileWriter(file));
							for (String string : stringList) {
								bufferedWriter.write(string + "\r\n");

							}
							if (bufferedWriter != null) {
								bufferedWriter.flush();
								bufferedWriter.close();
							}
						}
					}

				} catch (IOException e) {
					e.printStackTrace();
				}

			}
			if (writeQueue.size() == 0 && isFinished()) {
				break;
			}
		}

	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	private String getFileName(char c) {
		String fileName = null;
		char key = ' ', previousKey = ' ';
		Map<Character, String> map = FileConstant.fileNameMap;
		Iterator<Character> iterator = map.keySet().iterator();
		while (iterator.hasNext()) {
			key = iterator.next();
			if (c < key) {
				fileName = map.get(previousKey);
				break;
			}
			previousKey = key;
		}
		if(!iterator.hasNext()){
			if(c >= previousKey){
				fileName = map.get(previousKey);
			}
		}
		return fileName;
	}
}
