package com.pa2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This function will read the temporary files and pass it to sorting threads.
 * @author priyanka
 *
 */
public class ReadSortRunner implements Runnable {

	private ConcurrentLinkedQueue<List<String>> readQueue = new ConcurrentLinkedQueue<List<String>>();
	private File[] files;
	private int start;
	private int end;

	public ReadSortRunner(ConcurrentLinkedQueue<List<String>> readQueue, File[] files, int start, int end) {
		this.readQueue = readQueue;
		this.files = files;
		this.start = start;
		this.end = end;
	}

	/**
	 * This method will read the whole temporary file into list and pass it to next sorting threads for sorting.
	 */
	@Override
	public void run() {
		BufferedReader bufferedReader = null;
		String inputString = null;
		List<String> stringList = null;

		if (files != null && files.length > 0) {
			try {
				for (int i = start; i < end; i++) {

					bufferedReader = new BufferedReader(new FileReader(files[i]));
					stringList = new ArrayList<String>();
					while ((inputString = bufferedReader.readLine()) != null) {
						stringList.add(inputString);
						inputString = null;
					}
					if(stringList!=null && !stringList.isEmpty()){
						while(readQueue.size() > 2){}
						readQueue.add(stringList);
					}
					if (bufferedReader != null) {
						files[i].delete();
						bufferedReader.close();
						stringList.clear();
						stringList = null;
					}
				}
			} catch (FileNotFoundException e) {
				System.out.println("\n File not found");
			} catch (IOException e) {
				System.out.println("IO exception ");
			}

		}

	}
}
