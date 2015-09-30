/* Item stream runtime.
 */

package com.thomsonreuters.Takoyaki;

import java.util.Map;
import org.zeromq.ZMQ;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.thomsonreuters.upa.codec.DecodeIterator;
import com.thomsonreuters.upa.codec.Msg;
import com.thomsonreuters.upa.transport.Channel;

public class ItemStream {
/* Fixed name for this stream. */
	private String item_name;

/* Service origin, e.g. IDN_RDF */
	private String service_name;

/* Pseudo-view parameter, an array of field names */
	private ImmutableSortedSet<String> view_by_name;
	private ImmutableSortedSet<Integer> view_by_fid;

/* Subscription handle which is valid from login success to login close. */
	public int token;
/* Stream type, public or private distribution at fan-out point. */
	public boolean is_private_stream;

	private int reference_count;
	private ItemStreamDispatcher dispatcher;
	public Delegate delegate;

/* Performance counters */

/* Delegate implementing RFA like callbacks */
	public interface Delegate {
		public boolean OnMsg (Channel c, DecodeIterator it, Msg msg);
	}

	public ItemStream (Delegate delegate) {
		this.delegate = delegate;
	}

	public ItemStream (ItemStreamDispatcher dispatcher) {
		this.reference_count = 1;
		this.dispatcher = dispatcher;
	}

	public String getItemName() {
		return this.item_name;
	}

	public void setItemName (String item_name) {
		this.item_name = item_name;
	}

	public String getServiceName() {
		return this.service_name;
	}

	public void setServiceName (String service_name) {
		this.service_name = service_name;
	}

	public ImmutableSortedSet<String> getViewByName() {
		return this.view_by_name;
	}

	public void setViewByName (ImmutableSortedSet<String> view) {
		this.view_by_name = view;
	}

	public boolean hasViewByName() {
		return null != this.getViewByName();
	}

	public ImmutableSortedSet<Integer> getViewByFid() {
		return this.view_by_fid;
	}

	public void setViewByFid (ImmutableSortedSet<Integer> view) {
		this.view_by_fid = view;
	}

	public boolean hasViewByFid() {
		return null != this.getViewByFid();
	}

	public int referenceExchangeAdd (int val) {
		final int old = this.reference_count;
		this.reference_count += val;
		return old;
	}

	public int getReferenceCount() {
		return this.reference_count;
	}
	
	public ItemStreamDispatcher getDispatcher() {
		return this.dispatcher;
	}
}

/* eof */
