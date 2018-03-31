package com.grj;

import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 用来测试com.grj.Main的定时调度
 *
 */
public class SimpleTimingScheduleTest implements Job {
	 private static Logger _log = LoggerFactory.getLogger(SimpleTimingScheduleTest.class);


	    public SimpleTimingScheduleTest() {
	    }


	    @Override
	    public void execute(JobExecutionContext context)
	        throws JobExecutionException {

	        JobKey jobKey = context.getJobDetail().getKey();
	        _log.info("SimpleJob says: " + jobKey + " executing at " + new Date());
	    }
}
