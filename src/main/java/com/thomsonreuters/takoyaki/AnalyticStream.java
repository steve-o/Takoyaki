/* Analytic stream runtime.
 */

package com.thomsonreuters.Takoyaki;

import java.util.*;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.gson.Gson;
import com.reuters.rfa.common.Handle;
import org.joda.time.Interval;

public class AnalyticStream {
	private String query;

/* Source instruments for this analytic, e.g. MSFT.O */
	private String item_name;

/* App name, e.g. SignalApp */
	private String app_name;

/* Source time interval */
	private Interval interval;

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

	private Table<String, String, String> fids;

	private boolean is_closed;

	public AnalyticStream (AnalyticStreamDispatcher dispatcher, String identity) {
		this.dispatcher = dispatcher;
		this.identity = identity;
		this.clearStreamId();
		this.clearCommandId();
		this.clearTimerHandle();
		this.clearRetryCount();
		this.clearResult();
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

	public Interval getInterval() {
		return this.interval;
	}

	public void setInterval (Interval interval) {
		this.interval = interval;
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

	public void addResult (String datetime, String fid, String value) {
		this.fids.put (datetime, fid, value);
	}

	public Set<String> getResultFids() {
		return this.fids.columnKeySet();
	}

	public Set<String> getResultDateTimes() {
		return this.fids.rowKeySet();
	}

	public List<String> getResultForFid (String fid) {
		final Map<String, String> map = this.fids.column (fid);
		final Set<String> set = this.fids.rowKeySet();
		final List<String> list = new LinkedList<String>();
		for (Iterator it = set.iterator(); it.hasNext();) {
			final String row = (String)it.next();
			if (map.containsKey (row)) {
				list.add (map.get (row));
			} else {
/* prefer null but R tends to fail, so use empty string.
 * ref: http://stackoverflow.com/questions/15793759/convert-r-list-to-dataframe-with-missing-null-elements
 */
				list.add ("\"\"");
			}
		}
		return list;
	}

	public boolean hasResult() {
		return !this.fids.isEmpty();
	}

	public void clearResult() {
		this.fids = TreeBasedTable.create();
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
			", \"interval\":\"" + this.interval + "\"" +
			", \"service_name\":\"" + this.service_name + "\"" +
			", \"stream_id\":" + this.stream_id +
			", \"command_id\":" + this.command_id +
			", \"retry_count\":" + this.retry_count +
			", \"is_closed\":" + this.is_closed +
			" }";
	}

}

/* eof */
