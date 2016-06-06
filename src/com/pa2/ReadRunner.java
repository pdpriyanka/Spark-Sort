package com.pa2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class will read the data from files in chunk and puts into queue.
 * There are queue corresponding to each ASCII value. Read will check the 
 * first byte ASCII value and redirect it to corresponding queue.
 * 
 * @author priyanka
 *
 */
public class ReadRunner implements Runnable {
	// stores the list of concurrent queue object. One queue queue for each corresponding to one ASCII value.
	private List<ConcurrentLinkedQueue<List<String>>> concurrentLinkedQueueList;
	List<List<String>> lists = new ArrayList<List<String>>();
	private String inputFileName;
	private long start;
	private long end;
	private long noOfLinesToRead;

	public ReadRunner(List<ConcurrentLinkedQueue<List<String>>> concurrentLinkedQueueList, String inputFileName,
			long start, long end, long noOfLinesToRead) {
		this.concurrentLinkedQueueList = concurrentLinkedQueueList;
		this.inputFileName = inputFileName;
		this.start = start;
		this.end = end;
		this.noOfLinesToRead = noOfLinesToRead;
	}

	public void init() {
		for (int i = 0; i < concurrentLinkedQueueList.size(); i++) {
			lists.add(new ArrayList<String>());
		}
	}

	/** 
	 * This function will read the files into chunks and again divide into small chunks of 1000.
	 * It will stores the input data splits into list and pass it to queue for next threads.
	 * 
	 */
	@Override
	public void run() {
		init();

		String inputString = null;
		BufferedReader bufferedReader = null;
		long counter = 0, lastCounter, line = 0;

		try {
			bufferedReader = new BufferedReader(new FileReader(inputFileName));
			bufferedReader.skip((start - 1) * 100);

			counter = (long) Math.floor(((end - start) + 1) / noOfLinesToRead);

			lastCounter = ((end - start) + 1) - (counter * noOfLinesToRead);

			while (counter > 0) {

				line = 0;
				while (line < noOfLinesToRead) {
					if ((inputString = bufferedReader.readLine()) != null) {
						putIntoQueue(inputString);
						inputString = null;
						line++;
					}

				}
				counter--;
			}

			if (lastCounter > 0) {
				line = 0;
				while (line < lastCounter) {
					if ((inputString = bufferedReader.readLine()) != null) {
						putIntoQueue(inputString);
						line++;
					}
				}
				counter--;

			}

			flushReadData();
		} catch (FileNotFoundException e) {
			System.out.println("File Not Found Error " + e.getMessage());
		} catch (IOException e) {

			System.out.println("Error in reading file.");
		} finally {
			try {
				if (bufferedReader != null)
					bufferedReader.close();
			} catch (IOException e) {
				System.out.println("Error in closing file");
			}
		}

	}

	/*
	 * if there is some remaining data to read which is smaller than the defined chunk.
	 * This function will read it and pass it to next threads.
	 */
	
	public void flushReadData() {

		for (int i = 0; i < lists.size(); i++) {
			List<String> list = lists.get(i);
			if (!list.isEmpty()) {
				concurrentLinkedQueueList.get(i).offer(list);
			}
		}
	}

	/*
	 * This function will check the ASCII value of first byte of record. According ASCII value, it will redirect 
	 * the record into queue corresponding to that ASCII value.
	 */
	
	public void putIntoQueue(String input) {

		char c = input.charAt(0);
		int queueindex = -1;

		
		if (c < 32)
			queueindex = 0;
		else if (c < 40) {
			switch (c) {
			case 32:
				queueindex = 0;
				break;

			case 33:
				queueindex = 1;
				break;

			case 34:
				queueindex = 2;
				break;

			case 35:
				queueindex = 3;
				break;

			case 36:
				queueindex = 4;
				break;

			case 37:
				queueindex = 5;
				break;

			case 38:
				queueindex = 6;
				break;

			case 39:
				queueindex = 7;
				break;
			}
		} else if (c < 50) {
			switch (c) {
			case 40:
				queueindex = 8;
				break;
				
			case 41:
				queueindex = 9;
				break;

			case 42:
				queueindex = 10;
				break;

			case 43:
				queueindex = 11;
				break;

			case 44:
				queueindex = 12;
				break;

			case 45:
				queueindex = 13;
				break;

			case 46:
				queueindex = 14;
				break;

			case 47:
				queueindex = 15;
				break;

			case 48:
				queueindex = 16;
				break;

			case 49:
				queueindex = 17;
				break;
			}
		}
		 else if (c < 60) {
				switch (c) {
				case 50:
					queueindex = 18;
					break;

				case 51:
					queueindex = 19;
					break;

				case 52:
					queueindex = 20;
					break;

				case 53:
					queueindex = 21;
					break;

				case 54:
					queueindex = 22;
					break;

				case 55:
					queueindex = 23;
					break;

				case 56:
					queueindex = 24;
					break;

				case 57:
					queueindex = 25;
					break;

				case 58:
					queueindex = 26;
					break;

				case 59:
					queueindex = 27;
					break;
				}

			}
		 else if (c < 70) {
				switch (c) {
				case 60:
					queueindex = 28;
					break;

				case 61:
					queueindex = 29;
					break;

				case 62:
					queueindex = 30;
					break;

				case 63:
					queueindex = 31;
					break;

				case 64:
					queueindex = 32;
					break;

				case 65:
					queueindex = 33;
					break;

				case 66:
					queueindex = 34;
					break;

				case 67:
					queueindex = 35;
					break;

				case 68:
					queueindex = 36;
					break;

				case 69:
					queueindex = 37;
					break;
				}

			}
		 else if (c < 80) {
				switch (c) {
				case 70:
					queueindex = 38;
					break;

				case 71:
					queueindex = 39;
					break;

				case 72:
					queueindex = 40;
					break;

				case 73:
					queueindex = 41;
					break;

				case 74:
					queueindex = 42;
					break;

				case 75:
					queueindex = 43;
					break;

				case 76:
					queueindex = 44;
					break;

				case 77:
					queueindex = 45;
					break;

				case 78:
					queueindex = 46;
					break;

				case 79:
					queueindex = 47;
					break;
				}

			}
		 else if (c < 90) {
				switch (c) {
				case 80:
					queueindex = 48;
					break;

				case 81:
					queueindex = 49;
					break;

				case 82:
					queueindex = 50;
					break;

				case 83:
					queueindex = 51;
					break;

				case 84:
					queueindex = 52;
					break;

				case 85:
					queueindex = 53;
					break;

				case 86:
					queueindex = 54;
					break;

				case 87:
					queueindex = 55;
					break;

				case 88:
					queueindex = 56;
					break;

				case 89:
					queueindex = 57;
					break;
				}

			}
		 else if (c < 100) {
				switch (c) {
				case 90:
					queueindex = 58;
					break;

				case 91:
					queueindex = 59;
					break;

				case 92:
					queueindex = 60;
					break;

				case 93:
					queueindex = 61;
					break;

				case 94:
					queueindex = 62;
					break;

				case 95:
					queueindex = 63;
					break;

				case 96:
					queueindex = 64;
					break;

				case 97:
					queueindex = 65;
					break;

				case 98:
					queueindex = 66;
					break;

				case 99:
					queueindex = 67;
					break;
				}

			}
		 else if (c < 110) {
				switch (c) {
				case 100:
					queueindex = 68;
					break;

				case 101:
					queueindex = 69;
					break;

				case 102:
					queueindex = 70;
					break;

				case 103:
					queueindex = 71;
					break;

				case 104:
					queueindex = 72;
					break;

				case 105:
					queueindex = 73;
					break;

				case 106:
					queueindex = 74;
					break;

				case 107:
					queueindex = 75;
					break;

				case 108:
					queueindex = 76;
					break;

				case 109:
					queueindex = 77;
					break;
				}

			}
		 else if (c < 120) {
				switch (c) {
				case 110:
					queueindex = 78;
					break;

				case 111:
					queueindex = 79;
					break;

				case 112:
					queueindex = 80;
					break;

				case 113:
					queueindex = 81;
					break;

				case 114:
					queueindex = 82;
					break;

				case 115:
					queueindex = 83;
					break;

				case 116:
					queueindex = 84;
					break;

				case 117:
					queueindex = 85;
					break;

				case 118:
					queueindex = 86;
					break;

				case 119:
					queueindex = 87;
					break;
				}

			}
		 else if (c < 127) {
				switch (c) {
				case 120:
					queueindex = 88;
					break;

				case 121:
					queueindex = 89;
					break;

				case 122:
					queueindex = 90;
					break;

				case 123:
					queueindex = 91;
					break;

				case 124:
					queueindex = 92;
					break;

				case 125:
					queueindex = 93;
					break;

				case 126:
					queueindex = 94;
					break;
				}

			}
		 else
			 queueindex = 94;


		/*
		 * record is added into correspond queue.
		 */
		List<String> list = lists.get(queueindex);
		list.add(input);
		if (list.size() == 1000) {
			while (concurrentLinkedQueueList.get(queueindex).size() >= 5) {

			}
			concurrentLinkedQueueList.get(queueindex).offer(list);
			list = null;
			list = new ArrayList<String>();
			lists.set(queueindex, list);
		}

	}

}
