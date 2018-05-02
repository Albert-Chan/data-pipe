package com.dataminer.acquisitor;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class FTPLocalDownloader {
	private static Logger logger = LoggerFactory.getLogger(FTPDownloader.class);

	private FTPClient ftpClient;
	private String host;
	private String user;
	private String password;

	private FileNameCache cache;

	private static final String REMOTE_PATH_SEPERATOR = "/";

	public FTPLocalDownloader(String host, String user, String password) {
		this.ftpClient = new FTPClient();
		ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
		this.host = host;
		this.user = user;
		this.password = password;
		this.cache = new FileNameCache();
	}

	public synchronized void setFileChecker(FileChecker checker) {
		cache.setChecker(checker);
	}

	public void shutdown() {
		try {
			if (ftpClient.isConnected()) {
				ftpClient.disconnect();
				ftpClient.logout();
			}
		} catch (IOException e) {
			logger.warn(e.getMessage());
		}
	}

	/**
	 * Downloads the files from remoteBaseDir to localBaseDir recursively.
	 *
	 * @param remoteBaseDir
	 * @param localBaseDir
	 */
	public synchronized void download(String remoteBaseDir, String localBaseDir) {
		try {
			if (!ftpClient.isConnected()) {
				ftpClient.connect(host);
				ftpClient.login(user, password);
			}
			File localDir = new File(localBaseDir);
			if (!localDir.exists()) {
				localDir.mkdirs();
			}

			remoteBaseDir = remoteBaseDir.endsWith(REMOTE_PATH_SEPERATOR) ? remoteBaseDir
					: remoteBaseDir + REMOTE_PATH_SEPERATOR;
			if (ftpClient.changeWorkingDirectory(remoteBaseDir)) {
				ftpClient.enterLocalPassiveMode();
				FTPFile[] files = ftpClient.listFiles(remoteBaseDir);
				for (FTPFile f : files) {
					if (f.isFile()) {
						if (cache.checkStandby(f.getName(), f.getSize())) {
							downloadFile(f, localBaseDir);
						}
					} else {
						String newRemote = remoteBaseDir + f.getName();
						String newLocal = new File(localBaseDir, f.getName()).getCanonicalPath();
						download(newRemote, newLocal);
						ftpClient.changeToParentDirectory();
					}
				}
			}
		} catch (IOException e) {
			logger.warn(e.getMessage());
		}
	}

	private void downloadFile(FTPFile ftpFile, String localBase) throws IOException {
		String ftpFileName = ftpFile.getName();
		if (ftpFileName.contains("?")) {
			logger.info("file name with '?': " + ftpFileName);
		} else {
			File localFile = new File(localBase, ftpFileName);
			try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localFile))) {
				ftpClient.retrieveFile(ftpFileName, outputStream);
			}
			// ftpClient.getReply();
			logger.info(ftpFileName + " was stored in " + localFile.getCanonicalPath() + ".");
			// add this file to retrievedFiles
			cache.add(ftpFileName);
		}
	}

	public static void main(String[] args) {

		String ftpServer = "192.168.111.107";
		// String ftpServer = "192.168.100.14";
		String ftpUser = "root";
		String ftpPassword = "123456";

		FTPLocalDownloader ftpDownloader = new FTPLocalDownloader(ftpServer, ftpUser, ftpPassword);

		// ftpDownloader.setFileChecker(new FileTimeChecker(startTimestamp,
		// endTimestamp));
		ftpDownloader.download("/", "D:/");
	}

}