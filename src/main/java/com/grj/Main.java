package com.grj;

import java.util.Date;

import static org.quartz.DateBuilder.evenMinuteDate;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class Main {
	/**
	 * 调用com.grj.GuideJob的execute方法,进行定时调度
	 */
	public static void main(String[] args) {

		SchedulerFactory sf = new StdSchedulerFactory();
		Scheduler sched;
		try {
			sched = sf.getScheduler();
			Date runTime = evenMinuteDate(new Date());

			//create  a new GuideJob
			JobDetail job = newJob(GuideJob.class).withIdentity("job1", "group1").build();


			CronTrigger trigger = newTrigger().withIdentity("trigger1", "group1")
					.withSchedule(CronScheduleBuilder.cronSchedule("0 0 0 * * ?"))
			        .build();

			sched.scheduleJob(job, trigger);

			sched.start();

			Thread.sleep(65L * 1000L);
			sched.shutdown(true);

		} catch (SchedulerException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
