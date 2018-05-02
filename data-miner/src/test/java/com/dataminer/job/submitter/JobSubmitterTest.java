package com.dataminer.job.submitter;

import com.dataminer.job.submitter.JobSubmitter;

public class JobSubmitterTest {
	public static void main(String[] args) {
		JobSubmitter submitter = new JobSubmitter("jobSubmission", "group",
				"192.168.111.191:2181,192.168.111.192:2181,192.168.111.193:2181");
		submitter.handle();
	}
}
