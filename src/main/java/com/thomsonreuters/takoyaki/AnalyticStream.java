/* Analytic stream runtime.
 */

package com.thomsonreuters.Takoyaki;

import java.util.Map;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.reuters.rfa.common.Handle;

public class AnalyticStream {
	private String query;

/* Source instruments for this analytic, e.g. MSFT.O */
	private String item_name;

/* App name, e.g. SignalApp */
	private String app_name;

/* Service origin, e.g. ECP_SAP */
	private String service_name;

/* Dispatcher for stream updates */
	private AnalyticStreamDispatcher dispatcher;

/* Custom identifier */
	private String identity;

	private Optional<Integer> stream_id;
	private int command_id;
	private Handle timer_handle;
	private int retry_count;

	private boolean is_closed;

	public AnalyticStream (AnalyticStreamDispatcher dispatcher, String identity) {
		this.dispatcher = dispatcher;
		this.identity = identity;
		this.clearStreamId();
		this.clearCommandId();
		this.clearTimerHandle();
		this.clearRetryCount();
		this.is_closed = false;
	}

	public String getQuery() {
		return this.query;
	}

	public void setQuery (String query) {
		this.query = query;
	}

	public String getItemName() {
		return this.item_name;
	}

	public void setItemName (String item_name) {
		this.item_name = item_name;
	}

	public String getAppName() {
		return this.app_name;
	}

	public void setAppName (String app_name) {
		this.app_name = app_name;
	}

	public String getServiceName() {
		return this.service_name;
	}

	public void setServiceName (String service_name) {
		this.service_name = service_name;
	}

	public AnalyticStreamDispatcher getDispatcher() {
		return this.dispatcher;
	}

	public String getIdentity() {
		return this.identity;
	}

	public Integer getStreamId() {
		return this.stream_id.get();
	}

	public boolean hasStreamId() {
		return this.stream_id.isPresent();
	}

	public void setStreamId (Integer stream_id) {
		this.stream_id = Optional.of (stream_id);
	}

	public void clearStreamId() {
		this.stream_id = Optional.absent();
	}

	public int getCommandId() {
		return this.command_id;
	}

	public boolean hasCommandId() {
		return -1 != this.getCommandId();
	}

	public void setCommandId (int command_id) {
		this.command_id = command_id;
	}

	public void clearCommandId() {
		this.setCommandId (-1);
	}

	public Handle getTimerHandle() {
		return this.timer_handle;
        }
                
	public boolean hasTimerHandle() {
		return null != this.getTimerHandle();
	}
                
	public void setTimerHandle (Handle timer_handle) {
		this.timer_handle = timer_handle;
	}
                
	public void clearTimerHandle() {
		this.setTimerHandle (null);
	}

	public void incrementRetryCount() {
		this.retry_count++;
	}

	public int getRetryCount() {
		return this.retry_count;
	}

	public void clearRetryCount() {
		this.retry_count = 0;
	}

	public boolean isClosed() {
		return this.is_closed;
	}

	public void close() {
		this.is_closed = true;
	}

	@Override
	public String toString() {
		return "{ " +
			  "\"query\":\"" + this.query + "\"" +
			", \"item_name\":\"" + this.item_name + "\"" +
			", \"app_name\":\"" + this.app_name + "\"" +
			", \"service_name\":\"" + this.service_name + "\"" +
			", \"stream_id\":" + this.stream_id +
			", \"command_id\":" + this.command_id +
			", \"retry_count\":" + this.retry_count +
			", \"is_closed\":" + this.is_closed +
			" }";
	}

}

/* eof */
