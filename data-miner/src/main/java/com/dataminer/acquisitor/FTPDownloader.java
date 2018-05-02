package com.dataminer.acquisitor;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPDownloader {
	private static Logger logger = LoggerFactory.getLogger(FTPDownloader.class);

	private FTPClient ftpClient;
	private String host;
	private int port;
	private String user;
	private String password;
	private FileNameCache cache;
	private PostHandler postHandler;

	private static final String REMOTE_PATH_SEPERATOR = "/";

	public FTPDownloader(String host, int port, String user, String password) {
		this.ftpClient = new FTPClient();
		ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.cache = new FileNameCache();
	}

	public synchronized void setFileChecker(FileChecker checker) {
		cache.setChecker(checker);
	}

	public void setPostHandler(PostHandler handler) {
		this.postHandler = handler;
	}

	public void shutdown() {
		try {
			if (ftpClient.isConnected()) {
				ftpClient.logout();
				ftpClient.disconnect();
				logger.info("Connection to FTP server closed.");
			}
		} catch (IOException e) {
			logger.warn(e.getMessage());
		}
	}

	/**
	 * Downloads the files from remoteBaseDir to Kafka server directly.
	 *
	 * @param remoteBaseDir
	 *            the remote FTP server download directory.
	 */
	public synchronized void download(String remoteBaseDir) {
		try {
			if (!ftpClient.isConnected()) {
				ftpClient.setConnectTimeout(60000);
				ftpClient.connect(host, port);
				if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
					ftpClient.disconnect();
					logger.error("FTP server refused connection.");
					return;
				}
				logger.info("Connected to FTP server " + host + ".");
				if (!ftpClient.login(user, password)) {
					ftpClient.disconnect();
					logger.error("Unable to login to FTP server. Please check your user name and password.");
					return;
				}
				logger.info("Login successfully.");
				ftpClient.enterLocalPassiveMode();
			}
			remoteBaseDir = remoteBaseDir.endsWith(REMOTE_PATH_SEPERATOR) ? remoteBaseDir
					: remoteBaseDir + REMOTE_PATH_SEPERATOR;
			if (ftpClient.changeWorkingDirectory(remoteBaseDir)) {
				FTPFile[] files = ftpClient.listFiles(remoteBaseDir);
				for (FTPFile f : files) {
					if (f.isFile()) {
						if (cache.checkStandby(f.getName(), f.getSize())) {
							downloadFile(f);
						}
					} else {
						String newRemote = remoteBaseDir + f.getName();
						download(newRemote);
						ftpClient.changeToParentDirectory();
					}
				}
			}
		} catch (IOException e) {
			logger.warn(e.getMessage());
		}
	}

	private void downloadFile(FTPFile ftpFile) throws IOException {
		String ftpFileName = ftpFile.getName();
		try (InputStream inputStream = ftpClient.retrieveFileStream(ftpFileName)) {
			if (null != postHandler) {
				postHandler.handle(inputStream);
			}
		}
		logger.info(ftpFileName + " was retrieved successfully.");
		ftpClient.getReply();

		// add this file to retrievedFiles
		cache.add(ftpFileName);
	}

}
