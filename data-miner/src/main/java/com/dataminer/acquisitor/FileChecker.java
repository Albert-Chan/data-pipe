package com.dataminer.acquisitor;

@FunctionalInterface
public interface FileChecker {
	public boolean check(String fileNme);
}
