package com.pa2;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is used to perform sorting on each temporary file.
 * @author Priyanka
 *
 */
public class FinalSortRunner implements Runnable {
	// stores the data which is passed to write threads
	private ConcurrentLinkedQueue<List<String>> writeQueue = new ConcurrentLinkedQueue<List<String>>();
	
	//get the data from read threads
	private ConcurrentLinkedQueue<List<String>> readQueue = new ConcurrentLinkedQueue<List<String>>();
	private boolean finished;

	public FinalSortRunner(ConcurrentLinkedQueue<List<String>> writeQueue, ConcurrentLinkedQueue<List<String>> readQueue){
		this.writeQueue = writeQueue;
		this.readQueue = readQueue;
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	
	//This function will read the data from read queue, sort it by using quick sort and after sorting passed data
	//to write threads.
	@Override
	public void run() {
		List<String> stringList = null;

		while (true) {
			if (readQueue.size() > 0) {
					stringList = readQueue.poll();
					if (stringList != null && !(stringList.isEmpty())) {
						quicksort(stringList,0,stringList.size()-1);
						while(writeQueue.size() > 2){}
						writeQueue.offer(stringList);
					}

			}
			if(readQueue.size() == 0 && isFinished()){
				break;
			}
		}
		
	}

	/**
	 * This function will sort the data based of ASCII characters.
	 * Quick sort is implemented and partition function of quick sort is implemented. 
	 * @param list
	 * @param start
	 * @param end
	 */
	private void quicksort(List<String> list, int start, int end){
		if(start < end){
			int pivot = partition(list, start, end);
			quicksort(list, start, pivot-1);
			quicksort(list,pivot+1,end);

		}
	}
	
	/**
	 * This function will provide the pivot of quick sort and rearrange the list such that
	 * records less than pivots will be on left side of it and records greater than it on right side.
	 * @param list
	 * @param start
	 * @param end
	 * @return
	 */
	private int partition(List<String> list, int start, int end){
		int pivotIndex = end;
		String pivot =  list.get(pivotIndex);
		int count = start;
		String temp = null;
		char []str1 = new char[10] ,str2 = new char[10];
		for(int j = 0; j < 10 ; j++){
			str2[j] = pivot.charAt(j);
		}
		
		// extracted first 10 byte key value for comparision
		for(int i = start; i < end ; i++){
			
			for(int k =0 ; k < 10 ; k++)
			{
				str1[k] = list.get(i).charAt(k);
			}
			if(checklessthanequaltoString(str1,str2)){
				temp = list.get(i);
				list.set(i, list.get(count));
				list.set(count,temp);
				count ++;
			}
		}
		temp = list.get(pivotIndex);
		list.set(pivotIndex, list.get(count));
		list.set(count,temp);
		return count;
	}
	
	/**
	 * This function will check whether first string out of given two string is less or equal to other.
	 * @param str1
	 * @param str2
	 * @return
	 */
	private boolean checklessthanequaltoString(char[] str1, char[] str2)
	{
		boolean flag = true;
		if(str1 != null && str2 != null)
		{
			for(int i =0; i < str1.length ; i++){
				if(str1[i] < str2[i])
					return flag;
				else if(str1[i] > str2[i]){
					return false;
				}
			}
		}
		return flag;
	}
	
}
