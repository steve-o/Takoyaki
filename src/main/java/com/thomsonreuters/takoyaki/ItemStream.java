/* Item stream runtime.
 */

package com.thomsonreuters.Takoyaki;

import java.util.Map;
import org.zeromq.ZMQ;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.reuters.rfa.common.Handle;

public class ItemStream {
/* Fixed name for this stream. */
	private String item_name;

/* Service origin, e.g. IDN_RDF */
	private String service_name;

/* Pseudo-view parameter, an array of field names */
	private ImmutableSortedSet<String> view_by_name;
	private ImmutableSortedSet<Integer> view_by_fid;

/* Subscription handle which is valid from login success to login close. */
	private Handle item_handle;
/* Pending subscription handle as part of RSSL batch request */
	private Handle batch_handle;

	private int reference_count;
	private ItemStreamDispatcher dispatcher;

/* Performance counters */

	public ItemStream (ItemStreamDispatcher dispatcher) {
		this.clearItemHandle();
		this.clearBatchHandle();
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

	public Handle getItemHandle() {
		return this.item_handle;
	}

	public boolean hasItemHandle() {
		return null != this.getItemHandle();
	}

	public void setItemHandle (Handle item_handle) {
		this.item_handle = item_handle;
	}

	public void clearItemHandle() {
		this.setItemHandle (null);
	}

	public Handle getBatchHandle() {
		return this.batch_handle;
	}

	public boolean hasBatchHandle() {
		return null != this.getBatchHandle();
	}

	public void setBatchHandle (Handle batch_handle) {
		this.batch_handle = batch_handle;
	}

	public void clearBatchHandle() {
		this.setBatchHandle (null);
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
