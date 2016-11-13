package iu.pti.hbaseapp.clueweb09;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Calendar;

public class Helpers {
	
	/**
     * delete a non-empty directory
     * @param success or not
     * @return
     */
    public static boolean deleteDirectory(File dir) {
    	if (dir.exists()) {
			File[] files = dir.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					if (!deleteDirectory(files[i]))
						return false;
				} else {
					if (!files[i].delete())
						return false;
				}
			}
			return dir.delete();
		}
		return true;
    }
    
    /**
     * delete things under a directory
     * @param dir
     * @return
     */
    public static boolean deleteStuffInDir(File dir) {
    	if (dir.exists()) {
			File[] files = dir.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					if (!deleteDirectory(files[i]))
						return false;
				} else {
					if (!files[i].delete())
						return false;
				}
			}
		}
    	return true;
    }
    
    /**
	 * set the calendar value of theDateTime according to a string like "2008-08-08T08:08:08"
	 * @param theDateTime
	 * @param str
	 */
	public static void setDateTimeByString(Calendar theDateTime, String str) {
		str = str.trim();
		String year, month, day;
	  	int i1, i2, iT;
	  	i1 = str.indexOf("-");
	  	i2 = str.indexOf("-", i1+1);
	  	iT = str.indexOf('T');
	  	year = str.substring(0, i1);
	  	month = str.substring(i1+1, i2);
	  	day = str.substring(i2+1, iT);
	  	i1 = str.indexOf(':', iT+1);
	  	i2 = str.indexOf(':', i1+1);
	  	
	  	theDateTime.set(Calendar.MILLISECOND, 0);
	  	theDateTime.set(Calendar.SECOND, Integer.parseInt(str.substring(i2+1)));
	  	theDateTime.set(Calendar.MINUTE, Integer.parseInt(str.substring(i1+1, i2)));
	  	theDateTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(str.substring(iT+1, i1)));
	  	theDateTime.set(Calendar.DAY_OF_MONTH, 1);
	  	theDateTime.set(Calendar.MONTH, Integer.parseInt(month, 10)-1);
	  	theDateTime.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day, 10));
	  	theDateTime.set(Calendar.YEAR, Integer.parseInt(year, 10));
	}
	
	/** get date-time from a string like "2007-03-08T08:08:08" */
	public static Calendar getDateTimeFromString(String str) { 	
	  	Calendar ret = Calendar.getInstance();
	  	setDateTimeByString(ret, str);  	
	  	return ret;
	}
    
    /**
     * create MapReduce input files for DataLoaderClueWeb09 from a directory containing .warc.gz files
     * @param warcDir
     * @param destDir
     * @param nWarcPerFile
     */
    public static boolean createMapReduceInputForWarcDir (String warcDir, String destDir, int nWarcPerFile) throws IllegalArgumentException  {
    	File warcDirFile = new File(warcDir);
    	File destDirFile = new File(destDir);
    	if (warcDirFile.isFile() || destDirFile.isFile()) {
    		throw new IllegalArgumentException("Arguments are not both directories!");
    	}
    	if (warcDirFile.getAbsolutePath().equals(destDirFile.getAbsolutePath())) {
    		throw new IllegalArgumentException("Arguments should point to different directories.");
    	}
    	
    	if (!deleteStuffInDir(destDirFile)) {
    		System.err.println("Error in createMapReduceInputForWarcDir: can't delete files under " + destDir);
    		return false;
    	}
    	
    	File[] warcs = warcDirFile.listFiles();
    	StringBuffer sbContent = new StringBuffer();
    	int pathCount = 0;
    	int fileCount = 0;
    	for (int i=0; i<warcs.length; i++) {
    		String path = warcs[i].getAbsolutePath();
    		if (path.endsWith(".warc.gz")) {
    			if (pathCount >= nWarcPerFile) {
    				String mrInputPath = destDir + File.separator + "warcPaths_" + fileCount + ".txt";
    				writeStrToFile(mrInputPath, sbContent.toString());
    				System.out.println("MapReduce job input file " + mrInputPath + " created.");
    				fileCount++;
    				
    				pathCount = 0;
    				sbContent.setLength(0);
    			}
    			
    			sbContent.append(path).append('\n');
    			pathCount++;
    		}
    	}
    	
    	if (pathCount > 0) {
    		String mrInputPath = destDir + File.separator + "warcPaths_" + fileCount + ".txt";
			writeStrToFile(mrInputPath, sbContent.toString());
			System.out.println("MapReduce job input file " + mrInputPath + " created.");
    	}
    	
    	return true;
    }
    
    /**
	 * write content to filePath
	 * @param filePath
	 * @param content
	 */
	public static void writeStrToFile(String filePath, String content) {
		try {
			PrintWriter pwOut = new PrintWriter(new FileWriter(filePath));
			pwOut.write(content);
			pwOut.flush();
			pwOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * check if str is a string representation of a number (int, float, or hex)
	 * @param str
	 * @return
	 */
	public static boolean isNumberString(String str) {
		int dotCount = 0;
		// "other char" means characters that are not digits, ',', '.', or hex letters
		boolean hasOtherChar = false;
		boolean hasHexChar = false;
		int codeA = Character.getNumericValue('a');
		int codeF = Character.getNumericValue('f');
		str = str.toLowerCase();
		int len = str.length();
		
		// first, test if it's a number like 0xffffff
		if (str.startsWith("0x")) {
			if (len > 2) {
				for (int i = 2; i < len; i++) {
					char c = str.charAt(i);
					int code = Character.getNumericValue(c);
					if (!Character.isDigit(c) && !(code >= codeA && code <= codeF)) {
						return false;
					}
				}
				return true;
			} else {
				return false;
			}
		}
		
		for (int i=0; i<len; i++) {
			char c = str.charAt(i);
			int code = Character.getNumericValue(c);
			if (!Character.isDigit(c) && c != ',') {
				if (c == '.') {
					dotCount++;
				} else if (code >= codeA && code <= codeF) {
					hasHexChar = true;
				} else {
					hasOtherChar = true;
				}
			}			
		}
		
		if (hasOtherChar || dotCount > 1) {
			return false;
		}
		
		// if it's like a000.0b99, we don't understand it as a number
		if (hasHexChar && dotCount > 0) {
			return false;
		}
		
		return true;		
	}
	
	public static void usage() {
		System.out.println("java iu.pti.hbaseapp.clueweb09.Helpers <command> [<parameters>]");
		System.out.println("Where '<command> [<parameters>]' could be one of the following:");
		System.out.println("	create-mr-input <directory for .warc.gz files> <directory for MapReduce input files> <number of .warc.gz file paths per input file>");
		System.out.println("	calc-time-difference <date time 1> <date time 2>");
	}
    
    public static void main(String[] args) {
    	if (args.length <= 0) {
    		usage();
    		System.exit(1);
    	} else if (args[0].equals("create-mr-input")) {
    		if (args.length < 4) {
    			usage();
    			System.exit(1);
    		} else {
    			createMapReduceInputForWarcDir(args[1], args[2], Integer.valueOf(args[3]));
    		}
    	} else if (args[0].equals("calc-time-difference")) {
    		Calendar cal1 = getDateTimeFromString(args[1]);
    		Calendar cal2 = getDateTimeFromString(args[2]);
    		long diff = cal2.getTimeInMillis() - cal1.getTimeInMillis();
    		System.out.println("Difference in seconds: " + diff/1000);
    	}
	}
}
