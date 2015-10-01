/* Analytic query.
 */

package com.thomsonreuters.Takoyaki;

import com.google.gson.Gson;
import org.joda.time.Interval;

public class Analytic {
	private String service;
	private String app;
	private String query;
	private String item;
	private Interval interval;

	public Analytic (String service, String app, String query, String item) {
		this.setService (service);
		this.setApp (app);
		this.setQuery (query);
		this.setItem (item);
	}

	public String getService() {
		return this.service;
	}

	public void setService (String service) {
		this.service = service;
	}

	public String getApp() {
		return this.app;
	}

	public void setApp (String app) {
		this.app = app;
	}

	public String getQuery() {
		return this.query;
	}

	public void setQuery (String query) {
		this.query = query;
	}

	public String getItem() {
		return this.item;
	}

	public void setItem (String item) {
		this.item = item;
	}

/* java.time does not provide an equivalent to org.joda.time.Interval, we do not need microsecond precision either.
 * org.threeten provides a suitable API equivalent but does not support 'datetime/duration' time interval input.
 */
	public Interval getInterval() {
		return this.interval;
	}

	public void setInterval (Interval interval) {
		this.interval = interval;
	}

	@Override
	public String toString() {
		return new Gson().toJson (this);
	}
}

/* eof */
