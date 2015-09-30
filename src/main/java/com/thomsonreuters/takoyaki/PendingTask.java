package com.thomsonreuters.Takoyaki;

import java.time.Instant;

public class PendingTask {
// The task to run.
	public Runnable task;
// The time when the task should be run.
	public Instant delayed_run_time;

	public PendingTask (Runnable task) {
		this.task = task;
	}
	public PendingTask (Runnable task, Instant delayed_run_time) {
		this.task = task;
		this.delayed_run_time = delayed_run_time;
	}
}

/* eof */
