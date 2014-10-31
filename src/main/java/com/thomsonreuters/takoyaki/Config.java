/* Gateway configuration.
 */

package com.thomsonreuters.Takoyaki;

import com.google.gson.Gson;

public class Config {
//  RFA sessions comprising of session names, connection names,
//  RSSL hostname or IP address and default RSSL port, e.g. 14002, 14003.
	private SessionConfig[] sessions;

//// API boiler plate nomenclature
//  RFA application logger monitor name.
	private String monitor_name = "ApplicationLoggerMonitorName";

//  RFA event queue name.
	private String event_queue_name = "EventQueueName";

	public SessionConfig[] getSessions() {
		return this.sessions;
	}

	public SessionConfig getSession() {
		return this.getSessions()[0];
	}

	public void setSessions (SessionConfig[] sessions) {
		this.sessions = sessions;
	}

	public String getMonitorName() {
		return this.monitor_name;
	}

	public String getEventQueueName() {
		return this.event_queue_name;
	}

	@Override
	public String toString() {
		return new Gson().toJson (this);
	}
}

/* eof */
