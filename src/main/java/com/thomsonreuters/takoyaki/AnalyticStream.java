/* Analytic stream runtime.
 */

package com.thomsonreuters.Takoyaki;

import java.util.*;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.RowSortedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Interval;

public class AnalyticStream {
        private static Logger LOG = LogManager.getLogger (AnalyticStream.class.getName());

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
	private PendingTask timer_handle;
	private int retry_count;

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

	public PendingTask getTimerHandle() {
		return this.timer_handle;
        }
                
	public boolean hasTimerHandle() {
		return null != this.getTimerHandle();
	}
                
	public void setTimerHandle (PendingTask timer_handle) {
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

	private HashMap<String, StringBuilder> fids;
	private Set<String> all_fids;
	private StringBuilder datetime_builder;
	private int row_count;

	public void putAll (StringBuilder datetime, Map<String, StringBuilder> map) {
		if (this.all_fids.addAll (map.keySet())) {
/* new FID in this map */
			for (final Iterator it = this.all_fids.iterator(); it.hasNext();) {
				final String fid = (String)it.next();
				if (this.fids.containsKey (fid)) {
					if (map.containsKey (fid)) {
						final StringBuilder value = map.get (fid);
						this.fids.get (fid).append (value)
							.append (",");
					} else {
						this.fids.get (fid).append ("null,");
					}
				} else {
					final StringBuilder value = map.get (fid);
					final StringBuilder sb;
					if (row_count > 0)
						sb = new StringBuilder (Strings.repeat ("null,", row_count));
					else
						sb = new StringBuilder();
					sb.append (value)
						.append (",");
					this.fids.put (fid, sb);
				}
			}
		} else {
/* FID list unchanged */
			for (final Iterator it = this.all_fids.iterator(); it.hasNext();) {
				final String fid = (String)it.next();
				if (map.containsKey (fid)) {
					final StringBuilder value = map.get (fid);
					this.fids.get (fid).append (value)
						.append (",");
				} else {
					this.fids.get (fid).append ("null,");
				}
			}
		}
/* datetime managed independently */
		datetime_builder.append (datetime)
			.append (",");
		++row_count;
//LOG.debug ("{}@{}: value={}\nsb={}", "IRGCOND", row_count, map.get ("IRGCOND"), this.fids.get ("IRGCOND").toString());
	}

/* unsorted */
	public Set<String> fidSet() {
		return this.all_fids;
	}

	public StringBuilder joinedDateTimeSet() {
/* TBD: do not call more than once */
		if (this.datetime_builder.length() > 0)
			this.datetime_builder.setLength (this.datetime_builder.length() - 1);
		return this.datetime_builder;
	}

	public StringBuilder joinedValueForFid (String fid) {
		final StringBuilder sb = this.fids.get (fid);
		if (sb.length() > 0)
			sb.setLength (sb.length() - 1);
		return sb;
	}

	public boolean hasResult() {
		return !this.fids.isEmpty();
	}

	public void clearResult() {
		this.fids = Maps.newHashMap();
		this.all_fids = Sets.newHashSet();
		this.datetime_builder = new StringBuilder();
		this.row_count = 0;
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
