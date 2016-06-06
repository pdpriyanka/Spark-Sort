package com.pa2;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
	
/**
 * This function will used to for sorting phase.
 * Here reads, sort and write threads are created.
 * Temporary files are read by read threads and those threads will put them in a queue.
 * Sorting threads will pick the data from read queue and perform quick sort and
 * put data into write queue.
 * write queue will take the data from write queue and write into final output files.
 * There are 95 temporary files. Hence 95 output files is created.
 * If we concatinate all into one, then we will get one big files sorted.
 * @author priyanka
 *
 */

public class SortingPhase {
	
	public void sortPhase(String tempDirName,int noOfSortThreads, String outputPath){
		File tempDir1 = new File(tempDirName);
		int number_of_files = 0, read_threads = 1, write_threads =1;
		ConcurrentLinkedQueue<List<String>> readQueue = new ConcurrentLinkedQueue<List<String>>();
		ConcurrentLinkedQueue<List<String>> writeQueue = new ConcurrentLinkedQueue<List<String>>();

		
		List<Thread> readThreads = new ArrayList<Thread>();
		List<Thread> sortThreads = new ArrayList<Thread>();
		List<Thread> writeThreads = new ArrayList<Thread>();

		List<FinalSortRunner> finalSortRunners = new ArrayList<FinalSortRunner>();
		FinalSortRunner finalSortRunner = null;

		
		List<WriteSortRunner> writeSortRunners = new ArrayList<WriteSortRunner>();
		WriteSortRunner writeSortRunner = null;

		
		if(tempDir1.exists()){
			System.out.println(" Executing sorting phase");
			File[] fileArray = tempDir1.listFiles();
			number_of_files = fileArray.length;
			
			
			//create read threads
			int r = (int) Math.floor(((double)number_of_files)/read_threads);
			int start1 = 0;
			int end = r;
			if(read_threads == 1){
				readThreads.add(new Thread(new ReadSortRunner(readQueue,fileArray,start1,end)));
			}
			else if(read_threads > 1){
				readThreads.add(new Thread(new ReadSortRunner(readQueue,fileArray,(r * (read_threads -1)),fileArray.length)));

			}
			for(int i = 0; i < read_threads-1; i++){
				
				readThreads.add(new Thread(new ReadSortRunner(readQueue,fileArray,start1,end)));
				start1 = end;
				end = end + r;
			}
			
			
			//create sort threads
			for(int i =0 ; i <noOfSortThreads; i ++){
				finalSortRunner = new FinalSortRunner(writeQueue, readQueue);
				finalSortRunners.add(finalSortRunner);
				sortThreads.add(new Thread(finalSortRunner));
			}
			
			//create write threads
			for(int i =0 ; i <write_threads; i ++){
				writeSortRunner = new WriteSortRunner(writeQueue,outputPath);
				writeSortRunners.add(writeSortRunner);
				writeThreads.add(new Thread(writeSortRunner));
			}
			
			
			//start read threads
			for (Thread thread : readThreads) {
				thread.start();
			}

			//start sort threads
			for (Thread thread : sortThreads) {
				thread.start();
			}
			
			//start write threads
			for (Thread thread : writeThreads) {
				thread.start();
			}

			try {
				for (Thread readthread : readThreads) {
					readthread.join();
				}
				System.out.println("Sorting phase - done with read threads");

				while (true) {
					if ((!isThreadsAlive(readThreads)) && readQueue.size() == 0) {

						for (FinalSortRunner sortRunner1 : finalSortRunners) {
							sortRunner1.setFinished(true);
						}
						break;

					}
				}
				readThreads.clear();
				readThreads = null;

			} catch (InterruptedException e) {
				System.out.println("Thread interrupted");
			}
			
			
			
			
			try {
				for (Thread sortThread : sortThreads) {
					sortThread.join();
				}
				System.out.println("Soring phase -done with sort threads");

				while (true) {
					if ((!isThreadsAlive(sortThreads)) && writeQueue.size() == 0) {

						for (WriteSortRunner writeSortRunner1 : writeSortRunners) {
							writeSortRunner1.setFinished(true);
						}
						break;

					}
				}
				
				
				sortThreads.clear();
				sortThreads = null;
				
				finalSortRunners.clear();
				finalSortRunners = null;
				for (Thread writeThread : writeThreads) {
					writeThread.join();
				}
			
				System.out.println("Soring phase -done with write threads");

			} catch (InterruptedException e) {
				System.out.println("Thread interrupted");
			}
			

		}
		
	}


	//check if any one of thread is alive or not
	public boolean isThreadsAlive(List<Thread> threads) {
		int count = 0;

		for (Thread thread : threads) {
			if (thread.isAlive()) {
				return true;
			} else {
				count++;
			}
		}

		if (count == threads.size()) {
			return false;
		}

		return true;
	}

}
