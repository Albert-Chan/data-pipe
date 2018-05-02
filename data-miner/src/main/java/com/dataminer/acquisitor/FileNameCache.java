package com.dataminer.acquisitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;

public class FileNameCache {
	private static Logger logger = LoggerFactory.getLogger(FileNameCache.class);
	private static final int MAX_CACHED_FILE_NAMES = 60;

	private String eldestFile;
	private LinkedList<String> fileNameList = new LinkedList<>();
	private HashMap<String, Long> standbyFiles = new HashMap<>();
	private FileChecker checker;

	public void setChecker(FileChecker checker) {
		this.checker = checker;
	}

	public void add(String lastFile) {
		this.fileNameList.push(lastFile);
		if (fileNameList.size() > MAX_CACHED_FILE_NAMES) {
			eldestFile = fileNameList.removeLast();
		}
	}

	public boolean checkStandby(String fileName, long size) {
		if ((checker == null || checker.check(fileName)) && !isRetrieved(fileName)) {
			logger.debug("Checking the size of " + fileName);
			Long previousSize = standbyFiles.put(fileName, size);
			if (previousSize != null && previousSize.longValue() == size) {
				standbyFiles.remove(fileName);
				return true;
			}
		}
		return false;
	}

	private boolean isRetrieved(String fileName) {
		return isExpired(fileName) || this.fileNameList.contains(fileName);
	}

	private boolean isExpired(String fileName) {
		if (eldestFile == null || eldestFile.isEmpty()) {
			return false;
		}
		return fileName.compareTo(eldestFile) <= 0;
	}

}
