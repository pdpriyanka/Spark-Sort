package com.pa2;

import java.util.Map;
import java.util.TreeMap;

/*This class is used to defined filenames constants and map of one ASCII character and correspoding temporary file name.
 * *
 */
public class FileConstant {
	public static final String [] fileNames = {"00000", "00001", "00002","00003","00004","00005", "00006","00007","00008","00009","00010", "00011","00012","00013","00014","00015","00016","00017","00018",
											   "00019", "00020", "00021","00022","00023","00024", "00025","00026","00027","00028","00029", "00030","00031","00032","00033","00034","00035","00036","00037",
											   "00038", "00039", "00040","00041","00042","00043", "00044","00045","00046","00047","00048","00049","00050","00051","00052","00053","00054","00055","00056",
											   "00057", "00058", "00059","00060","00061","00062", "00063","00064","00065","00066","00067", "00068","00069","00070","00071","00072","00073","00074","00075",
											   "00076", "00077", "00078","00079","00080","00081", "00082","00083","00084","00085","00086", "00087","00088","00089","00090","00091","00092","00093","00094",};

	public static Map<Character,String>fileNameMap = new TreeMap<Character,String>();
	
	// create temporary files name and each temporary file which store the records correspond to one ASCII value
	static{
		char ASCII = 32;
		for(int i =0; i <fileNames.length; i++){
			fileNameMap.put(ASCII, fileNames[i]);
			ASCII += 1;
		}
	}

}
