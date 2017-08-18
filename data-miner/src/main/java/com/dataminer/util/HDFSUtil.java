package com.dataminer.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;

public class HDFSUtil {

	private static final Logger LOG = Logger.getLogger(HDFSUtil.class);

	public static FileSystem getFileSystem(String uri) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(uri), conf);
		return fs;
	}

	public static boolean existsPath(FileSystem fs, String path) throws IOException {
		FileStatus[] status = fs.globStatus(new Path(path));
		return status != null && status.length > 0;
	}

	public static boolean existsPath(String uri) throws IOException, URISyntaxException {
		FileSystem fs = HDFSUtil.getFileSystem(uri);
		if (HDFSUtil.existsPath(fs, uri)) {
			return true;
		}
		return false;
	}

	public static boolean deletePath(String uri, boolean recursive) throws IOException, URISyntaxException {
		FileSystem fs = HDFSUtil.getFileSystem(uri);
		return fs.delete(new Path(uri), recursive);
	}

	public static boolean deletePath(String uri) throws IOException, URISyntaxException {
		return deletePath(uri, true);
	}

	// delete corresponding HDFS
	public static void safeDelete(List<String> hdfsPath) throws Exception {
		for (String path : hdfsPath) {
			if (HDFSUtil.existsPath(path)) {
				LOG.info("delete " + path);
				HDFSUtil.deletePath(path);
			}
		}
	}

	public static void exportToHDFS(String hdfsPath, JavaRDD<?> rdd) throws Exception {
		if (null != hdfsPath && !hdfsPath.isEmpty()) {
			if (HDFSUtil.existsPath(hdfsPath)) {
				HDFSUtil.deletePath(hdfsPath);
			}
			rdd.saveAsTextFile(hdfsPath);
		}
	}

	public static void exportToHDFS(String hdfsPath, DataFrame df) throws Exception {
		if (null != hdfsPath && !hdfsPath.isEmpty()) {
			if (HDFSUtil.existsPath(hdfsPath)) {
				HDFSUtil.deletePath(hdfsPath);
			}
			RDDUtil.removeBracketOfRowRDD(df.javaRDD()).saveAsTextFile(hdfsPath);
		}
	}

	public static void rename(String oldPath, String newPath) throws IOException, URISyntaxException {
		getFileSystem(oldPath).rename(new Path(oldPath), new Path(newPath));
	}
}
