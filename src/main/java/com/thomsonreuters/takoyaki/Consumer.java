/* Simple consumer.
 */

package com.thomsonreuters.Takoyaki;

import java.util.*;
import java.net.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.joda.time.DateTime;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.reuters.rfa.common.Client;
import com.reuters.rfa.common.Context;
import com.reuters.rfa.common.Event;
import com.reuters.rfa.common.EventQueue;
import com.reuters.rfa.common.EventSource;
import com.reuters.rfa.common.Handle;
import com.reuters.rfa.dictionary.FidDef;
import com.reuters.rfa.dictionary.FieldDictionary;
import com.reuters.rfa.omm.OMMArray;
import com.reuters.rfa.omm.OMMAttribInfo;
import com.reuters.rfa.omm.OMMData;
import com.reuters.rfa.omm.OMMDataBuffer;
import com.reuters.rfa.omm.OMMElementEntry;
import com.reuters.rfa.omm.OMMElementList;
import com.reuters.rfa.omm.OMMEncoder;
import com.reuters.rfa.omm.OMMEntry;
import com.reuters.rfa.omm.OMMFieldEntry;
import com.reuters.rfa.omm.OMMFieldList;
import com.reuters.rfa.omm.OMMFilterEntry;
import com.reuters.rfa.omm.OMMFilterList;
import com.reuters.rfa.omm.OMMMap;
import com.reuters.rfa.omm.OMMMapEntry;
import com.reuters.rfa.omm.OMMMsg;
import com.reuters.rfa.omm.OMMPool;
import com.reuters.rfa.omm.OMMState;
import com.reuters.rfa.omm.OMMTypes;
import com.reuters.rfa.rdm.RDMInstrument;
import com.reuters.rfa.rdm.RDMMsgTypes;
import com.reuters.rfa.rdm.RDMService;
import com.reuters.rfa.rdm.RDMUser;
import com.reuters.rfa.session.DataDictInfo;
import com.reuters.rfa.session.event.ConnectionEvent;
import com.reuters.rfa.session.Session;
import com.reuters.rfa.session.TimerIntSpec;
import com.reuters.rfa.session.omm.OMMConnectionEvent;
import com.reuters.rfa.session.omm.OMMConnectionIntSpec;
import com.reuters.rfa.session.omm.OMMConsumer;
import com.reuters.rfa.session.omm.OMMErrorIntSpec;
import com.reuters.rfa.session.omm.OMMItemEvent;
import com.reuters.rfa.session.omm.OMMItemIntSpec;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionary;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionaryCache;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionaryRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionaryRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionaryResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectory;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryResponsePayload;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.Service;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLogin;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginResponseAttrib;

public class Consumer implements Client {
	private static Logger LOG = LogManager.getLogger (Consumer.class.getName());

	private SessionConfig config;

/* RFA context. */
	private Rfa rfa;

/* RFA asynchronous event queue. */
	private EventQueue event_queue;

/* RFA session defines one or more connections for horizontal scaling. */
	private Session session;

/* RFA OMM consumer interface. */
	private OMMConsumer omm_consumer;
        private OMMPool omm_pool;
	private OMMEncoder omm_encoder;

	private Set<Integer> field_set;

/* JSON serialisation */
	private Gson gson;
	private StringBuilder sb;

/* Data dictionaries. */
	private RDMDictionaryCache rdm_dictionary;

/* Directory */
	private Map<String, ItemStream> directory;

/* RFA Item event consumer */
	private Handle connection_handle;
	private Handle error_handle;
	private Handle login_handle;
	private Handle directory_handle;

	private class FlaggedHandle {
		private Handle handle;
		private boolean flag;

		public FlaggedHandle (Handle handle) {
			this.handle = handle;
			this.flag = false;
		}

		public Handle getHandle() {
			return this.handle;
		}

		public boolean isFlagged() {
			return this.flag;
		}

		public void setFlag() {
			this.flag = true;
		}
	}

	private Map<String, FlaggedHandle> dictionary_handle;
	private ImmutableMap<String, Integer> appendix_a;

/* Reuters Wire Format versions. */
	private byte rwf_major_version;
	private byte rwf_minor_version;

	private boolean has_view_support;
	private boolean has_batch_support;

	private boolean pending_logon;
	private boolean pending_directory;
	private boolean pending_dictionary;

	private static final boolean UNSUBSCRIBE_ON_SHUTDOWN = false;
	private static final boolean DISABLE_BATCH_SUPPORT = false;
	private static final boolean DISABLE_VIEW_SUPPORT = false;

	private static final int OMM_PAYLOAD_SIZE       = 65535;
	private static final int GC_DELAY_MS		= 15000;

	private static final String RSSL_PROTOCOL       = "rssl";
	private static final String SSLED_PROTOCOL      = "ssled";

	public Consumer (SessionConfig config, Rfa rfa, EventQueue event_queue) {
		this.config = config;
		this.rfa = rfa;
		this.event_queue = event_queue;
		this.rwf_major_version = 0;
		this.rwf_minor_version = 0;
		this.has_view_support = false;
		this.has_batch_support = false;
		this.pending_logon = true;
		this.pending_directory = true;
		this.pending_dictionary = true;
	}

	public void init() throws Exception {
		LOG.trace (this.config);

/* Manual serialisation */
		this.sb = new StringBuilder (512);

/* Null object support */
		this.gson = new GsonBuilder()
				.disableHtmlEscaping()
				.serializeNulls()
				.create();

/* Configuring the session layer package.
 */
		LOG.trace ("Acquiring RFA session.");
		this.session = Session.acquire (this.config.getSessionName());

/* RFA Version Info. The version is only available if an application
 * has acquired a Session (i.e., the Session Layer library is laoded).
 */
		LOG.debug ("RFA: { \"productVersion\": \"{}\" }", Context.getRFAVersionInfo().getProductVersion());

		if (this.config.getProtocol().equalsIgnoreCase (RSSL_PROTOCOL))
		{
/* Initializing an OMM consumer. */
			LOG.trace ("Creating OMM consumer.");
			this.omm_consumer = (OMMConsumer)this.session.createEventSource (EventSource.OMM_CONSUMER,
						this.config.getConsumerName(),
						false /* complete events */);
/* Registering for connection events. */
			LOG.trace ("Registering OMM connection interest.");
			OMMConnectionIntSpec ommConnIntSpec = new OMMConnectionIntSpec();
			this.connection_handle = this.omm_consumer.registerClient (this.event_queue, ommConnIntSpec, this, null);

/* Registering for Events from an OMM Consumer. */
			LOG.trace ("Registering OMM error interest.");
			OMMErrorIntSpec ommErrorIntSpec = new OMMErrorIntSpec();
			this.error_handle = this.omm_consumer.registerClient (this.event_queue, ommErrorIntSpec, this, null);

/* OMM memory management. */
			this.omm_pool = OMMPool.create (OMMPool.SINGLE_THREADED);
			this.omm_encoder = this.omm_pool.acquireEncoder();

			this.rdm_dictionary = new RDMDictionaryCache();

			this.sendLoginRequest();
			this.sendDirectoryRequest();
		}
		else
		{
			throw new Exception ("Unsupported transport protocol \"" + this.config.getProtocol() + "\".");
		}

		this.directory = new LinkedHashMap<String, ItemStream>();
		this.dictionary_handle = new TreeMap<String, FlaggedHandle>();
		this.field_set = Sets.newTreeSet();
	}

	public void clear() {
		if (null != this.rdm_dictionary)
			this.rdm_dictionary = null;
		if (null != this.omm_encoder)
			this.omm_encoder = null;
		if (null != this.omm_pool) {
			LOG.trace ("Closing OMMPool.");
			this.omm_pool.destroy();
			this.omm_pool = null;
		}
		if (null != this.omm_consumer) {
			LOG.trace ("Closing OMMConsumer.");
/* 8.2.11 Shutting Down an Application
 * an application may just destroy Event
 * Source, in which case the closing of the streams is handled by the RFA.
 */
			if (UNSUBSCRIBE_ON_SHUTDOWN) {
/* 9.2.5.3 Batch Close
 * The consumer application
 * builds a List of Handles of the event streams to close and calls OMMConsumer.unregisterClient().
 */
				if (null != this.directory && !this.directory.isEmpty()) {
					List<Handle> item_handles = new ArrayList<Handle> (this.directory.size());
					for (ItemStream item_stream : this.directory.values()) {
						if (item_stream.hasItemHandle())
							item_handles.add (item_stream.getItemHandle());
					}
					this.omm_consumer.unregisterClient (item_handles, null);
					this.directory.clear();
				}
				if (null != this.dictionary_handle && !this.dictionary_handle.isEmpty()) {
					for (FlaggedHandle flagged_handle : this.dictionary_handle.values()) {
						this.omm_consumer.unregisterClient (flagged_handle.getHandle());
					}
					this.dictionary_handle.clear();
				}
				if (null != this.directory_handle) {
					this.omm_consumer.unregisterClient (this.directory_handle);
					this.directory_handle = null;
				}
				if (null != this.login_handle) {
					this.omm_consumer.unregisterClient (this.login_handle);
					this.login_handle = null;
				}
				if (null != this.connection_handle) {
					this.omm_consumer.unregisterClient (this.connection_handle);
					this.connection_handle = null;
				}
			} else {
				if (null != this.directory && !this.directory.isEmpty())
					this.directory.clear();
				if (null != this.dictionary_handle && !this.dictionary_handle.isEmpty())
					this.dictionary_handle.clear();
				if (null != this.directory_handle)
					this.directory_handle = null;
				if (null != this.login_handle)
					this.login_handle = null;
				if (null != this.connection_handle)
					this.connection_handle = null;
			}
			this.omm_consumer.destroy();
			this.omm_consumer = null;
		}
		if (null != this.session) {
			LOG.trace ("Closing RFA Session.");
			this.session.release();
			this.session = null;
		}
	}

	public void batchCreateItemStream (String service_name, String[] item_names, String[] field_names, ItemStream[] item_streams) {
		LOG.trace ("Batch create {} items streams on service \"{}\".", item_names.length, service_name);
		if (this.config.getProtocol().equalsIgnoreCase (RSSL_PROTOCOL))
		{
			final ImmutableSortedSet<String> view_by_name = ImmutableSortedSet.copyOf (field_names);
			if (!this.pending_dictionary && item_names.length > 0) {
				final ImmutableSortedSet<Integer> view_by_fid = this.createViewByFid (this.rdm_dictionary.getFieldDictionary().toNameMap(), view_by_name);
				item_streams[0].setViewByFid (view_by_fid);
			}
			final List<ItemStream> batch = new ArrayList<ItemStream> (item_names.length);
			for (int i = 0; i < item_names.length; ++i) {
				LOG.trace ("{}: {}", (1 + i), item_names[i]);
				item_streams[i].setItemName (item_names[i]);
				item_streams[i].setServiceName (service_name);
				item_streams[i].setViewByName (view_by_name);
/* Construct directory unique key */
				this.sb.setLength (0);
				this.sb .append (service_name)
					.append ('.')
					.append (item_names[i]);
				if (!this.pending_dictionary) {
					if (i > 0) {
						item_streams[i].setViewByFid (item_streams[0].getViewByFid());
					}
					batch.add (item_streams[i]);
				}
				this.directory.put (this.sb.toString(), item_streams[i]);
			}
			if (!batch.isEmpty())
				this.batchSendItemRequest (batch);
			LOG.trace ("Directory size: {}", this.directory.size());
		}
	}

/* Create an item stream for a given symbol name.  The Item Stream maintains
 * the provider state on behalf of the application.
 */
	public void createItemStream (Instrument instrument, ItemStream item_stream) {
		LOG.trace ("Creating item stream for RIC \"{}\" on service \"{}\".", instrument.getName(), instrument.getService());
		item_stream.setItemName (instrument.getName());
		item_stream.setServiceName (instrument.getService());
/* viewType:- RDMUser.View.FIELD_ID_LIST or RDMUser.View.ELEMENT_NAME_LIST */
		final ImmutableSortedSet<String> view_by_name = ImmutableSortedSet.copyOf (instrument.getFields());
		item_stream.setViewByName (view_by_name);

/* Construct directory unique key */
		this.sb.setLength (0);
		this.sb	.append (instrument.getService())
			.append ('.')
			.append (instrument.getName());
		if (!this.pending_dictionary) {
			if (this.config.getProtocol().equalsIgnoreCase (RSSL_PROTOCOL)) {
				item_stream.setViewByFid (this.createViewByFid (this.rdm_dictionary.getFieldDictionary().toNameMap(), item_stream.getViewByName()));
				this.sendItemRequest (item_stream);
			}
		}
		this.directory.put (this.sb.toString(), item_stream);
		LOG.trace ("Directory size: {}", this.directory.size());
	}

	public void destroyItemStream (ItemStream item_stream) {
		LOG.trace ("Destroying item stream for RIC \"{}\" on service \"{}\".", item_stream.getItemName(), item_stream.getServiceName());
/* Construct directory unique key */
		this.sb.setLength (0);
		this.sb	.append (item_stream.getServiceName())
			.append ('.')
			.append (item_stream.getItemName());
		if (this.config.getProtocol().equalsIgnoreCase (RSSL_PROTOCOL)) {
			this.cancelItemRequest (item_stream);
		}
		this.directory.remove (this.sb.toString());
		LOG.trace ("Directory size: {}", this.directory.size());
	}

/* Convert a view by FID name to a view by FID values */
	private ImmutableSortedSet<Integer> createViewByFid (ImmutableMap<String, Integer> appendix_a, ImmutableSortedSet<String> view_by_name) {
		final ArrayList<Integer> fid_list = new ArrayList<Integer> (view_by_name.size());
		for (String name : view_by_name) {
			fid_list.add (appendix_a.get (name));
		}
		final Integer[] fid_array = fid_list.toArray (new Integer [fid_list.size()]);
		return ImmutableSortedSet.copyOf (fid_array);
	}

	private ImmutableSortedSet<Integer> createViewByFid (Map field_dictionary, ImmutableSortedSet<String> view_by_name) {
		final ArrayList<Integer> fid_list = new ArrayList<Integer> (view_by_name.size());
		for (String name : view_by_name) {
			final FidDef fid_def = (FidDef)field_dictionary.get (name);
			fid_list.add (Integer.valueOf (fid_def.getFieldId()));
		}
		final Integer[] fid_array = fid_list.toArray (new Integer [fid_list.size()]);
		return ImmutableSortedSet.copyOf (fid_array);
	}

	public void resubscribe() {
		if (this.config.getProtocol().equalsIgnoreCase (RSSL_PROTOCOL))
		{
			if (null == this.omm_consumer) {
				LOG.warn ("Resubscribe whilst consumer is invalid.");
				return;
			}

			final List<ItemStream> batch = new ArrayList<ItemStream>();
			for (ItemStream item_stream : this.directory.values()) {
				if (!item_stream.hasViewByFid()) {
	                                item_stream.setViewByFid (this.createViewByFid (this.rdm_dictionary.getFieldDictionary().toNameMap(), item_stream.getViewByName()));
				}
				if (!item_stream.hasItemHandle() && !item_stream.hasBatchHandle())
					batch.add (item_stream);
			}
			if (!batch.isEmpty())
				this.batchSendItemRequest (batch);
		}
	}

	private void batchSendItemRequest (List<ItemStream> item_streams) {
		if (this.has_batch_support)
		{
			LOG.trace ("Sending batch item request.");
			final OMMMsg msg = this.omm_pool.acquireMsg();
			msg.setMsgType (OMMMsg.MsgType.REQUEST);
			msg.setMsgModelType (RDMMsgTypes.MARKET_PRICE);
			msg.setAssociatedMetaInfo (this.login_handle);
			msg.setAttribInfo (item_streams.get (0).getServiceName(), null /* a batch */, RDMInstrument.NameType.RIC);
			OMMMsg request_msg = msg;
			final int indication_flags = OMMMsg.Indication.REFRESH | OMMMsg.Indication.NONSTREAMING | OMMMsg.Indication.BATCH_REQ;
			if (this.has_view_support && item_streams.get (0).hasViewByFid()) {
				request_msg.setIndicationFlags (indication_flags | OMMMsg.Indication.VIEW);
				this.omm_encoder.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);
				this.omm_encoder.encodeMsgInit (request_msg, OMMTypes.NO_DATA, OMMTypes.ELEMENT_LIST);
				this.omm_encoder.encodeElementListInit (OMMElementList.HAS_STANDARD_DATA, (short)0, (short)0);
				this.omm_encoder.encodeElementEntryInit (RDMUser.View.ViewType, OMMTypes.UINT);
				this.omm_encoder.encodeUInt (RDMUser.View.FIELD_ID_LIST);
				this.omm_encoder.encodeElementEntryInit (RDMUser.View.ViewData, OMMTypes.ARRAY);
				this.omm_encoder.encodeArrayInit (OMMTypes.INT, 2 /* sizeof (short) */);
				for (Integer fid : item_streams.get (0).getViewByFid()) {
					this.omm_encoder.encodeArrayEntryInit();
					this.omm_encoder.encodeInt (fid);
				}
				this.omm_encoder.encodeAggregateComplete();
			} else {
				request_msg.setIndicationFlags (indication_flags);
				this.omm_encoder.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);
				this.omm_encoder.encodeMsgInit (request_msg, OMMTypes.NO_DATA, OMMTypes.ELEMENT_LIST);
				this.omm_encoder.encodeElementListInit (OMMElementList.HAS_STANDARD_DATA, (short)0, (short)0);
			}
			this.omm_encoder.encodeElementEntryInit (RDMUser.Feature.ItemList, OMMTypes.ARRAY);
			this.omm_encoder.encodeArrayInit (OMMTypes.ASCII_STRING, 0);
			final OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
			for (ItemStream item_stream : item_streams) {
				this.omm_encoder.encodeArrayEntryInit();
				this.omm_encoder.encodeString (item_stream.getItemName(), OMMTypes.ASCII_STRING);
			}
			this.omm_encoder.encodeAggregateComplete();
			this.omm_encoder.encodeAggregateComplete();
			LOG.trace ("Encoded size: {} bytes.", this.omm_encoder.getEncodedSize());
			request_msg = (OMMMsg)this.omm_encoder.acquireEncodedObject();
			ommItemIntSpec.setMsg (request_msg);
/* individual item handle is pending batch response */
			final Handle batch_handle = this.omm_consumer.registerClient (this.event_queue, ommItemIntSpec, this, null);
			for (ItemStream item_stream : item_streams) {
				item_stream.setBatchHandle (batch_handle);
			}
			this.omm_pool.releaseMsg (msg);
		}
		else
		{
			for (ItemStream item_stream : item_streams) {
				this.sendItemRequest (item_stream);
			}
		}
	}

	private void sendItemRequest (ItemStream item_stream) {
		LOG.trace ("Sending item request.");
		final OMMMsg msg = this.omm_pool.acquireMsg();
		msg.setMsgType (OMMMsg.MsgType.REQUEST);
		msg.setMsgModelType (RDMMsgTypes.MARKET_PRICE);
		msg.setAssociatedMetaInfo (this.login_handle);
		msg.setAttribInfo (item_stream.getServiceName(), item_stream.getItemName(), RDMInstrument.NameType.RIC);
/* mutable reference that may be encoded */
		OMMMsg request_msg = msg;
/* No API to get entire indication flag set so use stack until required to forward to API */
		final int indication_flags = OMMMsg.Indication.REFRESH | OMMMsg.Indication.NONSTREAMING;
		if (this.has_view_support && item_stream.hasViewByFid()) {
			request_msg.setIndicationFlags (indication_flags | OMMMsg.Indication.VIEW);
			this.omm_encoder.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);
			this.omm_encoder.encodeMsgInit (request_msg, OMMTypes.NO_DATA, OMMTypes.ELEMENT_LIST);
			this.omm_encoder.encodeElementListInit (OMMElementList.HAS_STANDARD_DATA, (short)0, (short)0);
			this.omm_encoder.encodeElementEntryInit (RDMUser.View.ViewType, OMMTypes.UINT);
			this.omm_encoder.encodeUInt (RDMUser.View.FIELD_ID_LIST);
			this.omm_encoder.encodeElementEntryInit (RDMUser.View.ViewData, OMMTypes.ARRAY);
			this.omm_encoder.encodeArrayInit (OMMTypes.INT, 2 /* sizeof (short) */);
			for (Integer fid : item_stream.getViewByFid()) {
				this.omm_encoder.encodeArrayEntryInit();
				this.omm_encoder.encodeInt (fid);
			}
			this.omm_encoder.encodeAggregateComplete();
			this.omm_encoder.encodeAggregateComplete();
			request_msg = (OMMMsg)this.omm_encoder.acquireEncodedObject();
		} else {
			request_msg.setIndicationFlags (indication_flags);
		}
		LOG.trace ("Registering OMM item interest for MMT_MARKET_PRICE.");
		final OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
		ommItemIntSpec.setMsg (request_msg);
		item_stream.setItemHandle (this.omm_consumer.registerClient (this.event_queue, ommItemIntSpec, this, item_stream));
		this.omm_pool.releaseMsg (msg);
	}

/* 8.2.11.1 Unregistering Interest In OMM Market Information
 * if the event Stream had already been closed by RFA ... the application does not need to not call
 * unregisterClient().
 */
	private void cancelItemRequest (ItemStream item_stream) {
		if (item_stream.hasItemHandle()) {
			LOG.trace ("Cancelling market price request.");
			this.omm_consumer.unregisterClient (item_stream.getItemHandle());
		} else {
			LOG.trace ("Market price request closed by RFA.");
		}
	}

/* Making a Login Request
 * A Login request message is encoded and sent by OMM Consumer and OMM non-
 * interactive provider applications.
 */
	private void sendLoginRequest() throws UnknownHostException {
		LOG.trace ("Sending login request.");
		RDMLoginRequest request = new RDMLoginRequest();
		RDMLoginRequestAttrib attribInfo = new RDMLoginRequestAttrib();

/* RFA/Java only.
 */
		request.setMessageType (RDMLoginRequest.MessageType.REQUEST);
		request.setIndicationMask (EnumSet.of (RDMLoginRequest.IndicationMask.REFRESH));
		attribInfo.setRole (RDMLogin.Role.CONSUMER);

/* DACS username (required).
 */
		attribInfo.setNameType (RDMLogin.NameType.USER_NAME);
		attribInfo.setName (this.config.hasUserName() ?
			this.config.getUserName()
			: System.getProperty ("user.name"));

/* DACS Application Id (optional).
 * e.g. "256"
 */
		if (this.config.hasApplicationId())
			attribInfo.setApplicationId (this.config.getApplicationId());

/* DACS Position name (optional).
 * e.g. "127.0.0.1/net"
 */
		if (this.config.hasPosition()) {
			if (!this.config.getPosition().isEmpty())
				attribInfo.setPosition (this.config.getPosition());
		} else {
			this.sb.setLength (0);
			this.sb .append (InetAddress.getLocalHost().getHostAddress())
				.append ('/')
				.append (InetAddress.getLocalHost().getHostName());
			attribInfo.setPosition (this.sb.toString());
		}

/* Instance Id (optional).
 * e.g. "<Instance Id>"
 */
		if (this.config.hasInstanceId())
			attribInfo.setInstanceId (this.config.getInstanceId());

		request.setAttrib (attribInfo);

		LOG.trace ("Registering OMM item interest for MMT_LOGIN.");
		OMMMsg msg = request.getMsg (this.omm_pool);
		OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
		ommItemIntSpec.setMsg (msg);
		this.login_handle = this.omm_consumer.registerClient (this.event_queue, ommItemIntSpec, this, null);

/* Reset status */
		this.pending_directory = true;
// Maintain current status of dictionary instead of interrupting existing consumers.
//		this.pending_dictionary = true;
	}

/* Make a directory request to see if we can ask for a dictionary.
 */
	private void sendDirectoryRequest() {
		LOG.trace ("Sending directory request.");
		RDMDirectoryRequest request = new RDMDirectoryRequest();
		RDMDirectoryRequestAttrib attribInfo = new RDMDirectoryRequestAttrib();

/* RFA/Java only.
 */
		request.setMessageType (RDMDirectoryRequest.MessageType.REQUEST);
		request.setIndicationMask (EnumSet.of (RDMDirectoryRequest.IndicationMask.REFRESH));

/* Limit to named service */
		if (this.config.hasServiceName())
			attribInfo.setServiceName (this.config.getServiceName());

/* Request INFO and STATE filters for service names and states */
		attribInfo.setFilterMask (EnumSet.of (RDMDirectory.FilterMask.INFO, RDMDirectory.FilterMask.STATE));

		request.setAttrib (attribInfo);

		LOG.trace ("Registering OMM item interest for MMT_DIRECTORY.");
		OMMMsg msg = request.getMsg (this.omm_pool);
		OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
		ommItemIntSpec.setMsg (msg);
		this.directory_handle = this.omm_consumer.registerClient (this.event_queue, ommItemIntSpec, this, null);
	}

/* Make a dictionary request.
 *
 * 5.8.3 Version Check
 * Dictionary version checking can be performed by the client after a refresh
 * (Section 2.2) response message of a Dictionary is received.
 */
	private void sendDictionaryRequest (String service_name, String dictionary_name) {
		LOG.trace ("Sending dictionary request for \"{}\" from service \"{}\".", dictionary_name, service_name);
		RDMDictionaryRequest request = new RDMDictionaryRequest();
		RDMDictionaryRequestAttrib attribInfo = new RDMDictionaryRequestAttrib();

/* RFA/Java only.
 */
		request.setMessageType (RDMDictionaryRequest.MessageType.REQUEST);
		request.setIndicationMask (EnumSet.of (RDMDictionaryRequest.IndicationMask.REFRESH));

// RDMDictionary.Filter.NORMAL=0x7: Provides all information needed for decoding
		attribInfo.setVerbosity (RDMDictionary.Verbosity.NORMAL);
		attribInfo.setDictionaryName (dictionary_name);
		attribInfo.setServiceName (service_name);

		request.setAttrib (attribInfo);

		LOG.trace ("Registering OMM item interest for MMT_DICTIONARY/{}/{}", service_name, dictionary_name);
		OMMMsg msg = request.getMsg (this.omm_pool);
		OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
		ommItemIntSpec.setMsg (msg);
		this.dictionary_handle.put (dictionary_name,
			new FlaggedHandle (this.omm_consumer.registerClient (this.event_queue, ommItemIntSpec, this, dictionary_name /* closure */)));
	}

	@Override
	public void processEvent (Event event) {
		LOG.trace (event);
		switch (event.getType()) {
		case Event.OMM_ITEM_EVENT:
			this.OnOMMItemEvent ((OMMItemEvent)event);
			break;

		case Event.OMM_CONNECTION_EVENT:
			this.OnConnectionEvent ((OMMConnectionEvent)event);
			break;

		default:
			LOG.trace ("Uncaught: {}", event);
			break;
		}
	}

/* Handling Item Events, message types are munged c.f. C++ API.
 */
	private void OnOMMItemEvent (OMMItemEvent event) {
		LOG.trace ("OnOMMItemEvent: {}", event);
		final OMMMsg msg = event.getMsg();

/* Verify event is a response event. */
		switch (msg.getMsgType()) {
		case OMMMsg.MsgType.REFRESH_RESP:
		case OMMMsg.MsgType.UPDATE_RESP:
		case OMMMsg.MsgType.STATUS_RESP:
		case OMMMsg.MsgType.ACK_RESP:
			this.OnRespMsg (msg, event.getHandle(), event.getClosure());
			break;

/* Request message */
		case OMMMsg.MsgType.REQUEST:
/* Generic message */
		case OMMMsg.MsgType.GENERIC:
/* Post message */
		case OMMMsg.MsgType.POST:
		default:
			LOG.trace ("Uncaught: {}", msg);
			break;
		}
	}

	private void OnRespMsg (OMMMsg msg, Handle handle, Object closure) {
		LOG.trace ("OnRespMsg: {}", msg);
		switch (msg.getMsgModelType()) {
		case RDMMsgTypes.LOGIN:
			this.OnLoginResponse (msg);
			break;

		case RDMMsgTypes.DIRECTORY:
			this.OnDirectoryResponse (msg);
			break;

		case RDMMsgTypes.DICTIONARY:
			this.OnDictionaryResponse (msg, handle, closure);
			break;

		case RDMMsgTypes.MARKET_PRICE:
			this.OnMarketPrice (msg, handle, closure);
			break;

		default:
			LOG.trace ("Uncaught: {}", msg);
			break;
		}
	}

	private void OnLoginResponse (OMMMsg msg) {
		LOG.trace ("OnLoginResponse: {}", msg);
/* RFA example helper to dump incoming message. */
//GenericOMMParser.parse (msg);
		final RDMLoginResponse response = new RDMLoginResponse (msg);
		final byte stream_state = response.getRespStatus().getStreamState();
		final byte data_state   = response.getRespStatus().getDataState();

		switch (stream_state) {
		case OMMState.Stream.OPEN:
			switch (data_state) {
			case OMMState.Data.OK:
				this.OnLoginSuccess (response);
				break;

			case OMMState.Data.SUSPECT:
				this.OnLoginSuspect (response);
				break;

			default:
				LOG.trace ("Uncaught data state: {}", response);
				break;
			}
			break;

		case OMMState.Stream.CLOSED:
			this.OnLoginClosed (response);
			break;

		default:
			LOG.trace ("Uncaught stream state: {}", response);
			break;
		}
	}

/* Login Success.
 */
	private void OnLoginSuccess (RDMLoginResponse response) {
		LOG.trace ("OnLoginSuccess: {}", response);
/* defaults */
		this.has_batch_support = false;
		this.has_view_support = false;
/* Determine optional service support */
		if (response.hasAttrib()) {
			final RDMLoginResponseAttrib attrib = response.getAttrib();
			if (!DISABLE_BATCH_SUPPORT)
				this.has_batch_support = attrib.hasBatchSupport();
			if (!DISABLE_VIEW_SUPPORT)
				this.has_view_support = attrib.hasViewSupport();
			if (attrib.hasApplicationName()) {
				LOG.info ("Logged in to application: {}", attrib.getApplicationName());
			}
		}
		LOG.trace ("Unmuting consumer.");
		this.pending_logon = false;
	}

/* Other Login States.
 */
	private void OnLoginSuspect (RDMLoginResponse response) {
		LOG.trace ("OnLoginSuspect: {}", response);
		this.pending_logon = true;
	}

/* Other Login States.
 */
	private void OnLoginClosed (RDMLoginResponse response) {
		LOG.trace ("OnLoginClosed: {}", response);
		this.pending_logon = true;
	}

/* MMT_DIRECTORY domain.  Request RDM dictionaries, RWFFld and RWFEnum, from first available service.
 */
	private void OnDirectoryResponse (OMMMsg msg) {
		LOG.trace ("OnDirectoryResponse: {}", msg);
//GenericOMMParser.parse (msg);

		if (!this.pending_directory)
			return;

/* RFA 7.5.1.L1 raises invalid exception for Elektron Edge directory response due to hard coded capability validation. */
		final RDMDirectoryResponse response = new RDMDirectoryResponse (msg);
		if (!response.hasPayload()) {
			LOG.trace ("Ignoring directory response due to no payload.");
			return;
		}

		final RDMDirectoryResponsePayload payload = response.getPayload();
		if (!payload.hasServiceList()) {
			LOG.trace ("Ignoring directory response due to no service list.");
			return;
		}

/* Find /a/ service to request dictionary from.  It doesn't matter which as the ADS is
 * providing its own dictionary overriding anything from the provider.
 */
		String dictionary_service = null;
		for (Service service : payload.getServiceList()) {
			if (!service.hasServiceName()) {
				LOG.trace ("Ignoring listed service due to empty name.");
				continue;
			}
			if (!service.hasAction()) {
				LOG.trace ("{}: Ignoring service due to no map action {ADD|UPDATE|DELETE}.", service.getServiceName());
				continue;
			}
			if (RDMDirectory.ServiceAction.DELETE == service.getAction()) {
				LOG.trace ("{}: Ignoring service being deleted.", service.getServiceName());
				continue;
			}
			if (!service.hasStateFilter()) {
				LOG.trace ("{}: Ignoring service with no state filter as service may be unavailable.", service.getServiceName());
				continue;
			}
			final Service.StateFilter state_filter = service.getStateFilter();
			if (state_filter.hasServiceUp()) {
				if (state_filter.getServiceUp()) {
					if (state_filter.getAcceptingRequests()) {
						dictionary_service = service.getServiceName();
						break;
					} else {
						LOG.trace ("{}: Ignoring service as directory indicates it is not accepting requests.", service.getServiceName());
						continue;
					}
				} else {
					LOG.trace ("{}: Ignoring service marked as not-up.", service.getServiceName());
					continue;
				}
			} else {
				LOG.trace ("{}: Ignoring service without service state indicator.", service.getServiceName());
				continue;
			}
		}

		if (Strings.isNullOrEmpty (dictionary_service)) {
			LOG.trace ("No service available to accept dictionary requests, waiting for service change in directory update.");
			return;
		}

/* Hard code to RDM dictionary names */
		if (!this.dictionary_handle.containsKey ("RWFFld")) {
			this.sendDictionaryRequest (dictionary_service, "RWFFld");
		}

		if (!this.dictionary_handle.containsKey ("RWFEnum")) {
			this.sendDictionaryRequest (dictionary_service, "RWFEnum");
		}

/* directory received. */
		this.pending_directory = false;
	}

/* MMT_DICTIONARY domain.
 *
 * 5.8.4 Streaming Dictionary
 * Dictionary request can be streaming. Dictionary providers are not allowed to
 * send refresh and update data to consumers.  Instead the provider can
 * advertise a minor Dictionary change by sending a status (Section 2.2)
 * response message with a DataState of Suspect. It is the consumer’s
 * responsibility to reissue the dictionary request.
 */
	private void OnDictionaryResponse (OMMMsg msg, Handle handle, Object closure) {
		LOG.trace ("OnDictionaryResponse: {}", msg);
		final RDMDictionaryResponse response = new RDMDictionaryResponse (msg);
/* Receiving dictionary */
		if (response.hasAttrib()) {
			LOG.trace ("Dictionary {}: {}", response.getMessageType(), response.getAttrib().getDictionaryName());
		}
		if (response.getMessageType() == RDMDictionaryResponse.MessageType.REFRESH_RESP
			&& response.hasPayload() && null != response.getPayload())
		{
			this.rdm_dictionary.load (response.getPayload(), handle);
		}

/* Only know type after it is loaded. */
		final RDMDictionary.DictionaryType dictionary_type = this.rdm_dictionary.getDictionaryType (handle);

/* Received complete dictionary */
		if (response.getMessageType() == RDMDictionaryResponse.MessageType.REFRESH_RESP
			&& response.getIndicationMask().contains (RDMDictionaryResponse.IndicationMask.REFRESH_COMPLETE))
		{
			LOG.trace ("Dictionary complete.");
/* Check dictionary version */
			FieldDictionary field_dictionary = this.rdm_dictionary.getFieldDictionary();
			if (RDMDictionary.DictionaryType.RWFFLD == dictionary_type)
			{
				LOG.trace ("RDM field definitions version: {}", field_dictionary.getFieldProperty ("Version"));
			}
			else if (RDMDictionary.DictionaryType.RWFENUM == dictionary_type)
			{
/* Interesting values like Name, RT_Version, Description, Date are not provided by ADS */
				LOG.trace ("RDM enumerated tables version: {}", field_dictionary.getEnumProperty ("DT_Version"));
			}
/* Notify RFA example helper of dictionary if using to dump message content. */
//			GenericOMMParser.initializeDictionary (field_dictionary);
			this.dictionary_handle.get ((String)closure).setFlag();

/* Check all pending dictionaries */
			int pending_dictionaries = this.dictionary_handle.size();
			for (FlaggedHandle flagged_handle : this.dictionary_handle.values()) {
				if (flagged_handle.isFlagged())
					--pending_dictionaries;
			}
			if (0 == pending_dictionaries) {
				LOG.trace ("All used dictionaries loaded, resuming subscriptions.");
				this.pending_dictionary = false;
				this.resubscribe();
			} else {
				LOG.trace ("Dictionaries pending: {}", pending_dictionaries);
			}
		}
	}

/* MMT_MARKETPRICE domain.
 */
	private void OnMarketPrice (OMMMsg msg, Handle handle, Object closure) {
//		GenericOMMParser.parse (msg);
		LOG.trace ("OnMarketPrice: {}", msg);
/* Capture batch response */
		if (null == closure) {
			if (!msg.has (OMMMsg.HAS_ATTRIB_INFO)) {
				LOG.trace ("Ignoring null closure market price response with no attribinfo.");
				return;
			}
			final OMMAttribInfo attribInfo = msg.getAttribInfo();
			if (attribInfo.has (OMMAttribInfo.HAS_NAME_TYPE)
				&& RDMInstrument.NameType.RIC == attribInfo.getNameType()
				&& attribInfo.has (OMMAttribInfo.HAS_NAME)
				&& attribInfo.has (OMMAttribInfo.HAS_SERVICE_NAME))
			{
				this.sb.setLength (0);
				this.sb .append (attribInfo.getServiceName())
					.append ('.')
					.append (attribInfo.getName());
				if (!this.directory.containsKey (this.sb.toString())) {
					LOG.trace ("Ignoring unknown item \"{}\" on service \"{}\".", attribInfo.getServiceName(), attribInfo.getName());
					return;
				}
				final ItemStream item_stream = this.directory.get (this.sb.toString());
				item_stream.clearBatchHandle();
				item_stream.setItemHandle (handle);
				LOG.trace ("Reparsing response with new item stream.");
				OnMarketPrice (msg, handle, item_stream);
			}
			else
			{
				LOG.trace ("Ignoring null response.");
			}
			return;
		}
		final ItemStream item_stream = (ItemStream)closure;
		if (msg.isFinal()) {
			LOG.trace ("Subscription handle for \"{}\" on service \"{}\" is closed.", item_stream.getItemName(), item_stream.getServiceName());
			item_stream.clearItemHandle();
		}
/* ignore updates messages */
		if (OMMMsg.MsgType.REFRESH_RESP == msg.getMsgType()) {
/* fall through */
		}
		else if (OMMMsg.MsgType.UPDATE_RESP == msg.getMsgType()) {
			LOG.trace ("Ignoring update.");
			if (msg.isFinal()) {
				item_stream.getDispatcher().dispatch (item_stream, "internal error");
				this.destroyItemStream (item_stream);
			}
			return;
		}
		else if (OMMMsg.MsgType.STATUS_RESP == msg.getMsgType()) {
			LOG.trace ("Status: {}", msg);

/* Item stream recovered. */
			if (msg.has (OMMMsg.HAS_STATE)
				&& (OMMState.Stream.OPEN == msg.getState().getStreamState())
				&& (OMMState.Data.OK == msg.getState().getDataState()))
			{
				return;
			}

/* Defer to GSON to escape status text. */
			LogMessage log_msg = new LogMessage (
					"STATUS",
					item_stream.getServiceName(),
					item_stream.getItemName(),
					OMMState.Stream.toString (msg.getState().getStreamState()),
					OMMState.Data.toString (msg.getState().getDataState()),
					OMMState.Code.toString (msg.getState().getCode()),
					msg.getState().getText());
			item_stream.getDispatcher().dispatch (item_stream, this.gson.toJson (log_msg));
			this.destroyItemStream (item_stream);
			return;
		}
		else {
			LOG.trace ("Unhandled OMM message type ({}).", msg.getMsgType());
			if (msg.isFinal()) {
				item_stream.getDispatcher().dispatch (item_stream, "internal error");
				this.destroyItemStream (item_stream);
			}
			return;
		}

		if (OMMTypes.FIELD_LIST != msg.getDataType()) {
			LOG.trace ("Unsupported data type ({}) in OMM event.", msg.getDataType());
			if (msg.isFinal()) {
				item_stream.getDispatcher().dispatch (item_stream, "internal error");
				this.destroyItemStream (item_stream);
			}
			return;
		}

		final OMMFieldList field_list = (OMMFieldList)msg.getPayload();

		if (LOG.isDebugEnabled()) {
			final Iterator<?> it = field_list.iterator();
			while (it.hasNext()) {
				final OMMFieldEntry field_entry = (OMMFieldEntry)it.next();
				final short fid = field_entry.getFieldId();
				final FidDef fid_def = this.rdm_dictionary.getFieldDictionary().getFidDef (fid);
				final OMMData data = field_entry.getData (fid_def.getOMMType());
				LOG.debug (new StringBuilder()
					.append (fid_def.getName())
					.append (": ")
					.append (data.isBlank() ? "null" : data.toString())
					.toString());
			}
		}

		this.sb.setLength (0);
		this.sb .append ('{')
			 .append ("\"recordname\":\"").append (item_stream.getItemName()).append ('\"')
			.append (",\"fields\":{");
/* Use field_set to also count matching FIDs in update to view */
		this.field_set.clear();
		if (item_stream.hasViewByFid()) {
			final ImmutableSortedSet<Integer> view = item_stream.getViewByFid();
			final Iterator<?> it = field_list.iterator();
			while (it.hasNext()) {
				final OMMFieldEntry field_entry = (OMMFieldEntry)it.next();
				if (view.contains (Integer.valueOf (field_entry.getFieldId()))) {
					final FidDef fid_def = this.rdm_dictionary.getFieldDictionary().getFidDef (field_entry.getFieldId());
					final OMMData data = field_entry.getData (fid_def.getOMMType());
					if (!this.field_set.isEmpty()) this.sb.append (',');
					this.sb.append ('\"').append (fid_def.getName()).append ("\":");
					if (data.isBlank()) {
						this.sb.append ("null");
					} else {
						switch (fid_def.getOMMType()) {
/* values that can be represented raw in JSON form */
						case OMMTypes.DOUBLE:
						case OMMTypes.DOUBLE_8:
						case OMMTypes.FLOAT:
						case OMMTypes.FLOAT_4:
						case OMMTypes.INT:
						case OMMTypes.INT_1:
						case OMMTypes.INT_2:
						case OMMTypes.INT_4:
						case OMMTypes.INT_8:
						case OMMTypes.REAL:
						case OMMTypes.REAL_4RB:
						case OMMTypes.REAL_8RB:
						case OMMTypes.UINT:
						case OMMTypes.UINT_1:
						case OMMTypes.UINT_2:
						case OMMTypes.UINT_4:
						case OMMTypes.UINT_8:
							this.sb.append (data.toString());
							break;
						default:
							this.sb.append ('\"').append (data.toString()).append ('\"');
							break;
						}
					}
					this.field_set.add (Integer.valueOf (field_entry.getFieldId()));
					if (view.size() == this.field_set.size()) break;
				}
			}
		}
		this.sb.append ("}}");
/* Ignore updates with no matching fields */
		if (!this.field_set.isEmpty()) {
			item_stream.getDispatcher().dispatch (item_stream, this.sb.toString());
			this.destroyItemStream (item_stream);
		} else {
			LOG.trace ("Field set is empty");
			item_stream.getDispatcher().dispatch (item_stream, "internal error");
			this.destroyItemStream (item_stream);
		}

		final Iterator<?> it = field_list.iterator();
		while (it.hasNext()) {
			final OMMFieldEntry field_entry = (OMMFieldEntry)it.next();
			final short fid = field_entry.getFieldId();
			
		}
	}


// RFA 7.5.1
	private void OnConnectionEvent (OMMConnectionEvent event) {
		LOG.trace ("OnConnectionEvent: {}", event);
		LOG.info ("Connection status {} for {}@{}:{}",
				event.getConnectionStatus().toString(),
				event.getConnectedComponentVersion(), event.getConnectedHostName(), event.getConnectedPort());
	}

	private class LogMessage {
		private final String type;
		private final String service;
		private final String recordname;
		private final String stream;
		private final String data;
		private final String code;
		private final String text;

		public LogMessage (String type, String service, String recordname, String stream, String data, String code, String text) {
			this.type = type;
			this.service = service;
			this.recordname = recordname;
			this.stream = stream;
			this.data = data;
			this.code = code;
			this.text = text;
		}
	}
}

/* eof */
