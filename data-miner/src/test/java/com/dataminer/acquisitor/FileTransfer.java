package com.dataminer.acquisitor;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

public class FileTransfer {
	public static final int SIGNALING_INTERVAL = 60 * 1000;

	private static String fromDir = "/data2/ftp/from/";
	private static String toDir = "/data2/ftp/to/";
	private static String startTime = "201607010000";
	private static String endTime = "201607072359";
	private static int timerInterval = 60000;

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			fromDir = args[0];
		}
		if (args.length > 1) {
			toDir = args[1];
		}
		if (args.length > 2) {
			startTime = args[2];
		}
		if (args.length > 3) {
			endTime = args[3];
		}
		if (args.length > 4) {
			timerInterval = Integer.parseInt(args[4]);
		}

		File toDirectory = new File(toDir);
		if (!toDirectory.exists()) {
			toDirectory.mkdirs();
		}
		Timer timer = new Timer(true);
		TimerTask task = new FileTransferTask(fromDir, toDir, startTime, endTime, SIGNALING_INTERVAL);
		timer.schedule(task, 0, timerInterval);

		System.out.println("Press 'q' to exit.");
		Scanner s = new Scanner(System.in);
		while (s.hasNextLine()) {
			if ("q".equals(s.nextLine())) {
				break;
			}
		}
		s.close();
	}
}

class FileTransferTask extends TimerTask {
	private String fromDir;
	private String toDir;
	private long timeStamp = 0;
	private long endTime = 0;
	private int interval;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

	public FileTransferTask(String fromDir, String toDir, String startTime, String endTime, int interval) {
		this.fromDir = fromDir;
		this.toDir = toDir;
		try {
			this.timeStamp = sdf.parse(startTime).getTime();
		} catch (ParseException e) {
			this.timeStamp = 0;
		}
		try {
			this.endTime = sdf.parse(endTime).getTime();
		} catch (ParseException e) {
			this.endTime = 0;
		}
		this.interval = interval;
	}

	public void run() {
		if (timeStamp == 0) {
			timeStamp = System.currentTimeMillis();
		}
		if (timeStamp > this.endTime) {
			return;
		}

		StringBuilder filePath = new StringBuilder();

		String time = sdf.format(new Date(timeStamp));
		filePath.append("TRAFF_").append(time).append("00.txt");
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(fromDir + filePath.toString()), "GBK"));
		     BufferedWriter writer = new BufferedWriter(
				     new OutputStreamWriter(new FileOutputStream(toDir + filePath.toString()), "GBK"))) {
			String strLine = reader.readLine();
			while (strLine != null) {
				writer.write(strLine + "\n");
				strLine = reader.readLine();
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		timeStamp += interval;
	}
}
