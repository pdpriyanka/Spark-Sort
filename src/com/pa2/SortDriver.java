package com.pa2;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is the starting point for Shared memory sort. 
 * This class will creates and start the threads for reading and writing temporary files.
 * Also this class will give start the sorting phase.
 * @author priyanka
 *
 */
public class SortDriver {
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Invalid number of arguments");
			return;
		}

		int noofQueue=95,noOfReadThreads = 4, noOfSortThreads = Integer.parseInt(args[2]),noOfWriteThreads = 4, recordSize = 100;
		long start = 1, noOfLinesToRead = 2000000;
		String inputFileName = args[0], tempDirName = "temp", outputPath = args[1];
		SortDriver sortDriver = new SortDriver();
		List<Thread> readThreads = new ArrayList<Thread>();
		List<Thread> writeThreads = new ArrayList<Thread>();
		List<WriteRunner> writeRunners = new ArrayList<WriteRunner>();

		WriteRunner writeRunner = null;
		File inputFile = null;

		long beforeTime = System.currentTimeMillis();
		long beforeTime1 = System.nanoTime();
		
		//Check the validation of input file size

		try {

			inputFile = new File(inputFileName);
			if ((inputFile.length() % recordSize) != 0) {
				System.out.println(" File size should be multiple of 100 ");
				return;
			}

			long[] partSize = sortDriver.calFilePartToread(inputFile.length(), noOfReadThreads);
			if (partSize == null || partSize[0] == 0) {
				System.out.println("Can not divide file into parts.");
				return;
			}

			
			//If temporary directory is exists then delete and create new temporary directory.
			File tempDir = new File(tempDirName);
			boolean created = false;
			if (tempDir.exists()) {
				if (tempDir.isDirectory()) {
					for (File file : tempDir.listFiles()) {
						file.delete();
					}
					tempDir.delete();
				}
			}
			created = tempDir.mkdir();

			//if temporary directory is created successfully
			if (created == true) {
				System.out.println("Temp directory created.");
				// create the 95 queues. One for each ASCII value. If ASCII value is less than 32 then it
				//is put into queue corresponding to 32 ASCII value. If ASCII value is greater than 126 then
				// store it in queue corresponding queue of 126 
				
				List<ConcurrentLinkedQueue<List<String>>> concurrentLinkedQueueList = Collections.synchronizedList(new ArrayList<ConcurrentLinkedQueue<List<String>>>());
				
				for(int i =0 ; i< noofQueue ; i++)
				{
					concurrentLinkedQueueList.add(new ConcurrentLinkedQueue<List<String>>() );
				}
				
				
				
				//create the read threads
				
				for (int i = 0; i < noOfReadThreads; i++) {
					if (i == noOfReadThreads - 1)
						readThreads.add(new Thread(new ReadRunner(concurrentLinkedQueueList, inputFileName, start,
								(start + partSize[1]) - 1, noOfLinesToRead)));
					else
						readThreads.add(new Thread(new ReadRunner(concurrentLinkedQueueList, inputFileName, start,
								(start + partSize[0]) - 1, noOfLinesToRead)));
					start = start + partSize[0];
				}

			
				
				// get the names of files to be generated.
				// 95 temporary files will get generated.
				String [] fileNames = FileConstant.fileNames;

				int filesToWrite = (int)noofQueue/noOfWriteThreads;
				int s1 = 0,e1 =filesToWrite;
				if(noOfWriteThreads > 1){
					
					writeRunner = new WriteRunner(concurrentLinkedQueueList, tempDirName,fileNames,(noOfWriteThreads-1)*filesToWrite, noofQueue);
					writeRunners.add(writeRunner);
					writeThreads.add(new Thread(writeRunner));

				}
				
				
				// creates the write threads
				for (int i = 0; i < noOfWriteThreads-1; i++) {
					writeRunner = new WriteRunner(concurrentLinkedQueueList, tempDirName,fileNames,s1, e1);
					writeRunners.add(writeRunner);
					writeThreads.add(new Thread(writeRunner));
					s1 = e1;
					e1 = e1+filesToWrite;
				}

				
				//start the read threads
				for (Thread readthread : readThreads) {
					readthread.start();
				}

			
				//start the write threads
				for (Thread writeThread : writeThreads) {
					System.out.println("starting write threads");
					writeThread.start();

				}

				try {
					
					//wait till read threads complete
					for (Thread readthread : readThreads) {
						readthread.join();
					}
					System.out.println("done with read threads");

					int setFinishedCounter = 0;
					boolean allempty = true;
					
					//if reads threads are done and there is no data to process by write threads then stop the write threads
					while (true) {
						if (!sortDriver.isThreadsAlive(readThreads)) {
							
							for (WriteRunner writeRunner1 : writeRunners) {
								allempty = true;
								if(!(writeRunner1.isFinished())){ 
									for(int i = writeRunner1.getStart() ; i < writeRunner1.getEnd(); i ++){
										if(concurrentLinkedQueueList.get(i).size() > 0){
											allempty = false;
											break;
										}
									}
									if(allempty){
										writeRunner1.setFinished(true);
										setFinishedCounter++;
									}
								}
							}
						}
						if(setFinishedCounter == noOfWriteThreads){
							break;
						}
					
					}
					
					// wait till write threads completes
					for (Thread wrThread : writeThreads) {
						wrThread.join();
					}

				} catch (InterruptedException e) {
					System.out.println("Thread interrupted");
				}
				
				
				// make object eligible for garbage collector.
				concurrentLinkedQueueList.clear();
				concurrentLinkedQueueList = null;
				
				readThreads.clear();
				writeRunners.clear();
				writeThreads.clear();
				readThreads = null;
				writeRunners = null;
				writeThreads = null;
				fileNames = null;
				
				
				//if output directory contains files corresponding to previous execution then delete the
				//output files first from output directory
				File outputDir = new File(outputPath);
				if (outputDir.exists()) {
					if (outputDir.isDirectory()) {
						for (File file : outputDir.listFiles()) {
							file.delete();
						}
						outputDir.delete();
					}
				}
				// Creates the output directory.
				boolean createdOutPutDir = outputDir.mkdir();
				if(createdOutPutDir){
					System.out.println("Sorting phase started");

				SortingPhase sortingPhase= new SortingPhase();
				sortingPhase.sortPhase(tempDirName, noOfSortThreads,outputPath);
				}
						

				System.out.println(" For File size = " + inputFile.length() + " bytes, time for shared memory sort = "
						+ (((double) System.currentTimeMillis() - beforeTime) / 1000) + " seconds");
				
				System.out.println(" For File size = " + inputFile.length() + " bytes, time for shared memory sort = "
						+ (((double) System.nanoTime() - beforeTime1) / Math.pow(10, 9)) + " seconds");


			} else {
				System.out.println("temp directory not created. ");
			}
		} catch (NumberFormatException e1) {
			System.out.println(" Invalid Input");
		}

	}

	/**
	 * This function will check if any one of thread from the given list is alive or not
	 * @param threads
	 * @return
	 */
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

	/**
	 * This function is used to splits the file into parts as per number of read threads.
	 * Each read thread will read the different portion of file
	 * @param fileLength
	 * @param noOfReadThreads
	 * @return
	 */
	public long[] calFilePartToread(long fileLength, int noOfReadThreads) {

		long[] partSize = new long[2];
		fileLength = fileLength / 100;
		// partSize[0] = (long)Math.floorDiv(fileLength,noOfReadThreads);
		partSize[0] = (long) Math.floor(fileLength / noOfReadThreads);
		partSize[1] = fileLength - ((noOfReadThreads - 1) * partSize[0]);

		return partSize;
	}

	// This function is used to calculate number of lines to merge.
	//currently this function is not in use. As I change the approach of creating temporary files.
	public static long numberofLinesToMerge(long noOfLinesToRead, String tempDirName) {
		long freeMemory = Runtime.getRuntime().freeMemory();
		File tempDir = new File(tempDirName);
		long noOflinesToMerge = noOfLinesToRead;
		if (tempDir.exists()) {
			long numberofFiles = tempDir.list().length;
			long number = (long) ((0.75 * freeMemory) / (numberofFiles * 100));
			if (number < noOflinesToMerge) {
				noOflinesToMerge = number;
			}
		}
		System.out.println(" merge line "+noOflinesToMerge);
		return noOflinesToMerge;
	}
}
