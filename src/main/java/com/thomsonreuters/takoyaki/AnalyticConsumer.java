/* Private-stream based analytics app consumer.
 */

package com.thomsonreuters.Takoyaki;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.*;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
// Java 8
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.Shorts;
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
import com.reuters.rfa.omm.OMMDateTime;
import com.reuters.rfa.omm.OMMElementEntry;
import com.reuters.rfa.omm.OMMElementList;
import com.reuters.rfa.omm.OMMEncoder;
import com.reuters.rfa.omm.OMMEnum;
import com.reuters.rfa.omm.OMMEntry;
import com.reuters.rfa.omm.OMMFieldEntry;
import com.reuters.rfa.omm.OMMFieldList;
import com.reuters.rfa.omm.OMMFilterEntry;
import com.reuters.rfa.omm.OMMFilterList;
import com.reuters.rfa.omm.OMMIterable;
import com.reuters.rfa.omm.OMMMap;
import com.reuters.rfa.omm.OMMMapEntry;
import com.reuters.rfa.omm.OMMMsg;
import com.reuters.rfa.omm.OMMNumeric;
import com.reuters.rfa.omm.OMMPool;
import com.reuters.rfa.omm.OMMSeries;
import com.reuters.rfa.omm.OMMState;
import com.reuters.rfa.omm.OMMTypes;
import com.reuters.rfa.omm.OMMQos;
import com.reuters.rfa.omm.OMMQosReq;
import com.reuters.rfa.rdm.RDMInstrument;
import com.reuters.rfa.rdm.RDMMsgTypes;
import com.reuters.rfa.rdm.RDMService;
import com.reuters.rfa.session.Session;
import com.reuters.rfa.session.TimerIntSpec;
// RFA 7.5.1
import com.reuters.rfa.session.omm.OMMConnectionEvent;
import com.reuters.rfa.session.omm.OMMConnectionIntSpec;
import com.reuters.rfa.session.omm.OMMConsumer;
import com.reuters.rfa.session.omm.OMMErrorIntSpec;
import com.reuters.rfa.session.omm.OMMHandleItemCmd;
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
//import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.Service;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLogin;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.app.login.AppLogin;
import com.thomsonreuters.rfa.valueadd.domainrep.app.login.AppLoginRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.app.login.AppLoginRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.app.login.AppLoginResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.ResponseStatus;
import com.thomsonreuters.upa.codec.Buffer;
import com.thomsonreuters.upa.codec.Codec;
import com.thomsonreuters.upa.codec.CodecFactory;
import com.thomsonreuters.upa.codec.CodecReturnCodes;
import com.thomsonreuters.upa.codec.DataDictionary;
import com.thomsonreuters.upa.codec.DataStates;
import com.thomsonreuters.upa.codec.DataTypes;
import com.thomsonreuters.upa.codec.DictionaryEntry;
import com.thomsonreuters.upa.codec.DecodeIterator;
import com.thomsonreuters.upa.codec.ElementEntry;
import com.thomsonreuters.upa.codec.ElementList;
import com.thomsonreuters.upa.codec.ElementListFlags;
import com.thomsonreuters.upa.codec.EncodeIterator;
import com.thomsonreuters.upa.codec.FieldEntry;
import com.thomsonreuters.upa.codec.FieldList;
import com.thomsonreuters.upa.codec.FieldListFlags;
import com.thomsonreuters.upa.codec.FilterEntry;
import com.thomsonreuters.upa.codec.FilterEntryActions;
import com.thomsonreuters.upa.codec.FilterList;
import com.thomsonreuters.upa.codec.GenericMsg;
import com.thomsonreuters.upa.codec.GenericMsgFlags;
import com.thomsonreuters.upa.codec.LocalFieldSetDefDb;
import com.thomsonreuters.upa.codec.MapEntryActions;
import com.thomsonreuters.upa.codec.Msg;
import com.thomsonreuters.upa.codec.MsgClasses;
import com.thomsonreuters.upa.codec.MsgKey;
import com.thomsonreuters.upa.codec.MsgKeyFlags;
import com.thomsonreuters.upa.codec.RefreshMsg;
import com.thomsonreuters.upa.codec.RefreshMsgFlags;
import com.thomsonreuters.upa.codec.RequestMsg;
import com.thomsonreuters.upa.codec.RequestMsgFlags;
import com.thomsonreuters.upa.codec.Series;
import com.thomsonreuters.upa.codec.SeriesEntry;
import com.thomsonreuters.upa.codec.SeriesFlags;
import com.thomsonreuters.upa.codec.State;
import com.thomsonreuters.upa.codec.StateCodes;
import com.thomsonreuters.upa.codec.StatusMsg;
import com.thomsonreuters.upa.codec.StatusMsgFlags;
import com.thomsonreuters.upa.codec.StreamStates;
import com.thomsonreuters.upa.codec.UpdateMsg;
import com.thomsonreuters.upa.codec.UpdateMsgFlags;
import com.thomsonreuters.upa.rdm.Dictionary;
import com.thomsonreuters.upa.rdm.Directory;
import com.thomsonreuters.upa.rdm.DomainTypes;
import com.thomsonreuters.upa.rdm.ElementNames;
import com.thomsonreuters.upa.rdm.InstrumentNameTypes;
import com.thomsonreuters.upa.rdm.Login;
import com.thomsonreuters.upa.transport.Channel;
import com.thomsonreuters.upa.transport.ChannelInfo;
import com.thomsonreuters.upa.transport.ChannelState;
import com.thomsonreuters.upa.transport.ComponentInfo;
import com.thomsonreuters.upa.transport.CompressionTypes;
import com.thomsonreuters.upa.transport.ConnectOptions;
import com.thomsonreuters.upa.transport.ConnectionTypes;
import com.thomsonreuters.upa.transport.InProgFlags;
import com.thomsonreuters.upa.transport.InProgInfo;
import com.thomsonreuters.upa.transport.ReadArgs;
import com.thomsonreuters.upa.transport.Transport;
import com.thomsonreuters.upa.transport.TransportBuffer;
import com.thomsonreuters.upa.transport.TransportFactory;
import com.thomsonreuters.upa.transport.TransportReturnCodes;
import com.thomsonreuters.upa.transport.WriteArgs;
import com.thomsonreuters.upa.transport.WriteFlags;
import com.thomsonreuters.upa.transport.WritePriorities;

public class AnalyticConsumer implements ItemStream.Delegate {
	private static Logger LOG = LogManager.getLogger (AnalyticConsumer.class.getName());
	private static final Marker SHOGAKOTTO_MARKER = MarkerManager.getMarker ("SHOGAKOTTO");
	private static final String LINE_SEPARATOR = System.getProperty ("line.separator");

	private SessionConfig config;

/* UPA context. */
	private Upa upa;
/* This flag is set to false when Run should return. */
	private boolean keep_running;

/* Active UPA connection. */
	private Channel connection;
/* unique id per connection. */
	private String prefix;

/* RFA session defines one or more connections for horizontal scaling. */
	private Session session;

/* RFA OMM consumer interface. */
	private OMMConsumer omm_consumer;
	private OMMPool omm_pool;
	private OMMEncoder omm_encoder, omm_encoder2;

	private Set<Integer> field_set;

/* JSON serialisation */
	private Gson gson;
	private StringBuilder sb;

/* Pending messages to flush. */
	int pending_count;
/* Data dictionaries. */
	private DataDictionary rdm_dictionary;
	private BiMap<String, Integer> dictionary_tokens;

/* Watchlist of all items. */
	private List<ItemStream> directory;
	private Map<Integer, ItemStream> tokens;

/* Service name to id map  */
	private ImmutableBiMap<String, Integer> service_map;

/* incrementing unique id for streams */
	int token;
	int directory_token;
	int login_token;	/* should always be 1 */
/* RSSL keepalive state. */
	Instant next_ping;
	Instant next_pong;
	int ping_interval;	/* seconds */

	private ImmutableMap<String, Integer> appendix_a;

	private Instant last_activity;

	private Instant NextPing() {
		return this.next_ping;
	}

	private Instant NextPong() {
		return this.next_pong;
	}

	private void SetNextPing (Instant time) {
		this.next_ping = time;
	}

	private void SetNextPong (Instant time) {
		this.next_pong = time;
	}

	private void IncrementPendingCount() {
		this.pending_count++;
	}

	private void ClearPendingCount() {
		this.pending_count = 0;
	}

	private int GetPendingCount() {
		return this.pending_count;
	}

	private class App implements ItemStream.Delegate {
/* ERROR: modifier 'static' is only allowed in constant variable declarations */
		private Logger LOG = LogManager.getLogger (App.class.getName());

		private OMMConsumer omm_consumer;
		private OMMPool omm_pool;
		private OMMEncoder omm_encoder;
		private OMMEncoder omm_encoder2;
		private String service_name;
		private String app_name;
		private String uuid;
		private String password;
		private List<AnalyticStream> streams;
		private LinkedHashMap<Integer, AnalyticStream> stream_map;
		private int stream_id;
		private boolean pending_connection;	/* to app */
		private final State default_closed_response_status;
		private State closed_response_status;
		private ItemStream private_stream;

		public App (String service_name, String app_name, String uuid, String password) {
			this.app_name = app_name;
			this.uuid = uuid;
			this.password = password;
			this.streams = Lists.newLinkedList();
			this.stream_map = Maps.newLinkedHashMap();
			this.resetStreamId();
			this.private_stream = new ItemStream (this);
			this.private_stream.token = 0;
			this.private_stream.setServiceName (service_name);
			this.setPendingConnection();
// Appears until infrastructure returns new close status to present.
			this.default_closed_response_status = CodecFactory.createState();
			this.default_closed_response_status.code (StateCodes.NO_RESOURCES);
			this.default_closed_response_status.dataState (DataStates.SUSPECT);
			this.default_closed_response_status.streamState (StreamStates.CLOSED);
			final Buffer text = CodecFactory.createBuffer();
			text.data ("No private stream available to process the request.");
			this.default_closed_response_status.text (text);
			this.closed_response_status = this.default_closed_response_status;
		}

		private boolean CreatePrivateStream (Channel c) {
			LOG.trace ("Creating app \"{}\" private stream on service \"{}\".",
				this.app_name, this.private_stream.getServiceName());
			final RequestMsg request = (RequestMsg)CodecFactory.createMsg();
/* Set the message model type. */
//			request.domainType (DomainTypes.SYSTEM);
request.domainType (DomainTypes.HISTORY);
/* Set request type. */
			request.msgClass (MsgClasses.REQUEST);
			request.flags (RequestMsgFlags.STREAMING | RequestMsgFlags.PRIVATE_STREAM);
/* No view thus no payload. */
			request.containerType (DataTypes.NO_DATA);
/* Set the stream token, recycle for closed a stream. */
			request.streamId (this.private_stream.token == 0 ? token : this.private_stream.token);
LOG.debug ("private stream token {}", this.private_stream.token == 0 ? token : this.private_stream.token);

/* In RFA lingo an attribute object */
			request.msgKey().name().data (this.uuid);
			request.msgKey().serviceId (service_map.get (this.private_stream.getServiceName()));
			request.msgKey().flags (MsgKeyFlags.HAS_NAME | MsgKeyFlags.HAS_SERVICE_ID);

request.flags (request.flags() | RequestMsgFlags.HAS_QOS);
request.qos().dynamic (false);
request.qos().rate (com.thomsonreuters.upa.codec.QosRates.TICK_BY_TICK);
request.qos().timeliness (com.thomsonreuters.upa.codec.QosTimeliness.REALTIME);
request.qos().rateInfo (0);
request.qos().timeInfo (0);

/* ASG login credentials */
			request.msgKey().attribContainerType (DataTypes.ELEMENT_LIST);
			request.msgKey().flags (request.msgKey().flags() | MsgKeyFlags.HAS_ATTRIB);

			final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
			final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
			if (null == buf) {
				LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
				return false;
			}
			final EncodeIterator it = CodecFactory.createEncodeIterator();
			it.clear();
			int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
				return false;
			}
			rc = request.encodeInit (it, MAX_MSG_SIZE);
			if (CodecReturnCodes.ENCODE_MSG_KEY_ATTRIB != rc) {
				LOG.error ("RequestMsg.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}

/* Encode attribute object after message instead of before as per RFA. */
			final ElementList element_list = CodecFactory.createElementList();
			final ElementEntry element_entry = CodecFactory.createElementEntry();
			final com.thomsonreuters.upa.codec.Buffer rssl_buffer = CodecFactory.createBuffer();
			element_list.flags (ElementListFlags.HAS_STANDARD_DATA);
			rc = element_list.encodeInit (it, null /* element id dictionary */, 0 /* count of elements */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("RequestMsg.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"flags\": \"HAS_STANDARD_DATA\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
/* user name */
			rssl_buffer.data (this.uuid);
			element_entry.dataType (DataTypes.ASCII_STRING);
			final com.thomsonreuters.upa.codec.Buffer asg_name = CodecFactory.createBuffer();
			asg_name.data ("Name");
			element_entry.name (asg_name);
			rc = element_entry.encode (it, rssl_buffer);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"data\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_buffer);
				return false;
			}
/* password */
			rssl_buffer.data (this.password);
			element_entry.dataType (DataTypes.ASCII_STRING);
			final com.thomsonreuters.upa.codec.Buffer asg_password = CodecFactory.createBuffer();
			asg_password.data ("Password");
			element_entry.name (asg_password);
			rc = element_entry.encode (it, rssl_buffer);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"data\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_buffer);
				return false;
			}
			rc = element_list.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementList.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			rc = request.encodeKeyAttribComplete (it, true /* commit */);
			if (CodecReturnCodes.ENCODE_CONTAINER != rc) {
				LOG.error ("RequestMsg.encodeKeyAttribComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			rc = request.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("RequestMsg.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}

			if (LOG.isDebugEnabled()) {
				final DecodeIterator jt = CodecFactory.createDecodeIterator();
				jt.clear();
				rc = jt.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				} else {
					LOG.debug ("{}", request.decodeToXml (jt));
				}
			}
/* Message validation. */
			if (!request.validateMsg()) {
				LOG.error ("RequestMsg.validateMsg failed.");
				return false;
			}

			if (0 == Submit (c, buf)) {
				return false;
			} else if (0 == this.private_stream.token) {
				this.private_stream.token = token++;
				tokens.put (this.private_stream.token, this.private_stream);
			}
			return true;
		}

		public void createItemStream (AnalyticStream stream) {
			LOG.trace ("Creating analytic stream for query \"{}\" to app \"{}\" on service \"{}\".",
				stream.getQuery(), stream.getAppName(), stream.getServiceName());
			stream.setStreamId (this.acquireStreamId());

			if (!this.pending_connection)
				this.sendItemRequest (connection, stream);
			this.streams.add (stream);
			this.stream_map.put (stream.getStreamId(), stream);

// transient state feedback
			this.registerRetryTimer (stream, retry_timer_ms);
		}

		private void registerRetryTimer (AnalyticStream stream, int retry_timer_ms) {
			final TimerIntSpec timer = new TimerIntSpec();
			timer.setDelay (retry_timer_ms);
//			final Handle timer_handle = this.omm_consumer.registerClient (this.event_queue, timer, this, stream);
//			if (timer_handle.isActive())
//				stream.setTimerHandle (timer_handle);
//			else
//				LOG.error ("Timer handle for query \"{}\" closed on registration.", stream.getQuery());
		}

		public void destroyItemStream (AnalyticStream stream) {
			if (stream.isClosed()) {
				LOG.trace ("Stream already closed, do not submit close request.");
			} else {
// WARNING: no close confirmation from app.
				this.cancelItemRequest (stream);
				stream.close();
			}
			this.removeItemStream (stream);
		}

		public void removeItemStream (AnalyticStream stream) {
			this.streams.remove (stream);
			this.stream_map.remove (stream.getStreamId());
			if (stream.hasTimerHandle()) {
				this.omm_consumer.unregisterClient (stream.getTimerHandle());
				stream.clearTimerHandle();
				stream.clearRetryCount();
			}
		}

		public void resubmit() {
			if (this.pending_connection)
				return;
/* A command id means a pending response, pendingClose() is waiting for confirmation
 * of subscription cancellation by server side app.
 */
			for (AnalyticStream stream : this.streams) {
				if (!stream.hasCommandId() && !stream.isClosed())
					this.sendItemRequest (connection, stream);
			}
		}

		private boolean sendItemRequest (Channel c, AnalyticStream stream) {
			LOG.trace ("Sending analytic query request.");

/* Prepare GenericMsg encapsulation */
			final GenericMsg wrapper = (GenericMsg)CodecFactory.createMsg();
			wrapper.domainType (DomainTypes.HISTORY);
			wrapper.msgClass (MsgClasses.GENERIC);
			wrapper.flags (GenericMsgFlags.MESSAGE_COMPLETE);
			wrapper.containerType (DataTypes.MSG);
			wrapper.streamId (this.private_stream.token);
			final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
			final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
			if (null == buf) {
				LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
				return false;
			}
			final EncodeIterator it = CodecFactory.createEncodeIterator();
			it.clear();
			int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
				return false;
			}
			rc = wrapper.encodeInit (it, MAX_MSG_SIZE);
			if (CodecReturnCodes.ENCODE_CONTAINER != rc) {
				LOG.error ("GenericMsg.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}

			final RequestMsg request = (RequestMsg)CodecFactory.createMsg();
/* Set the message model type. */
			request.domainType (DomainTypes.HISTORY);
/* Set request type. */
			request.msgClass (MsgClasses.REQUEST);
			request.flags (RequestMsgFlags.NONE);
/* No view thus no payload. */
			request.containerType (DataTypes.NO_DATA);
/* Set the stream token. */
			request.streamId (stream.getStreamId());

/* In RFA lingo an attribute object */
			request.msgKey().nameType (InstrumentNameTypes.RIC);
			request.msgKey().name().data (stream.getItemName());
			request.msgKey().flags (MsgKeyFlags.HAS_NAME_TYPE | MsgKeyFlags.HAS_NAME);

/* App request elements */
			request.msgKey().attribContainerType (DataTypes.FIELD_LIST);
			request.msgKey().flags (request.msgKey().flags() | MsgKeyFlags.HAS_ATTRIB);

			rc = request.encodeInit (it, 0 /* max message size */);
			if (CodecReturnCodes.ENCODE_MSG_KEY_ATTRIB != rc) {
				LOG.error ("RequestMsg.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
/* Encode attribute object after message instead of before as per RFA. */
			final FieldList field_list = CodecFactory.createFieldList();
			final FieldEntry field_entry = CodecFactory.createFieldEntry();
			final com.thomsonreuters.upa.codec.Date rssl_date = CodecFactory.createDate();
			final com.thomsonreuters.upa.codec.Time rssl_time = CodecFactory.createTime();
			final com.thomsonreuters.upa.codec.Buffer rssl_buffer = CodecFactory.createBuffer();
			final com.thomsonreuters.upa.codec.Enum rssl_enum = CodecFactory.createEnum();
			final com.thomsonreuters.upa.codec.Int rssl_int = CodecFactory.createInt();
			field_list.flags (FieldListFlags.HAS_STANDARD_DATA | FieldListFlags.HAS_FIELD_LIST_INFO);
			field_list.dictionaryId (1 /* RDMFieldDictionary */);
			field_list.fieldListNum (5 /* record template number */);
			rc = field_list.encodeInit (it, null /* local dictionary */, 0 /* size hint */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldList.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"flags\": \"HAS_STANDARD_DATA|HAS_FIELD_LIST_INFO\", \"dictionaryId\": {}, \"fieldListNum\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					field_list.dictionaryId(), field_list.fieldListNum());
				return false;
			}
// MET_TF_U: 9=TAS, 10=TAQ
			switch (stream.getQuery()) {
			case "days":		rssl_enum.value (4); break;
			case "weeks":		rssl_enum.value (5); break;
			case "months":		rssl_enum.value (6); break;
			case "quarters":	rssl_enum.value (7); break;
			case "years":		rssl_enum.value (8); break;
			case "tas":		rssl_enum.value (9); break;
			case "taq":		rssl_enum.value (10); break;
			default: break;
			}
			field_entry.dataType (DataTypes.ENUM);
			field_entry.fieldId (12794);
			rc = field_entry.encode (it, rssl_enum);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"fieldId\": {}, \"dataType\": \"{}\", \"MET_TF_U\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					field_entry.fieldId(), DataTypes.toString (field_entry.dataType()), rssl_enum);
				return false;
			}
// RQT_S_DATE+RQT_STM_MS
			rssl_date.clear();
			rssl_date.year (stream.getInterval().getStart().getYear());
			rssl_date.month (stream.getInterval().getStart().getMonthOfYear());
			rssl_date.day (stream.getInterval().getStart().getDayOfMonth());
			field_entry.dataType (DataTypes.DATE);
			field_entry.fieldId (9219);
			rc = field_entry.encode (it, rssl_date);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"fieldId\": {}, \"dataType\": \"{}\", \"RQT_S_DATE\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					field_entry.fieldId(), DataTypes.toString (field_entry.dataType()), rssl_date);
				return false;
			}
			rssl_time.clear();
			rssl_time.hour (stream.getInterval().getStart().getHourOfDay());
			rssl_time.minute (stream.getInterval().getStart().getMinuteOfHour());
			rssl_time.second (stream.getInterval().getStart().getSecondOfMinute());
			rssl_time.millisecond (stream.getInterval().getStart().getMillisOfSecond());
			field_entry.dataType (DataTypes.TIME);
			field_entry.fieldId (14225);
			rc = field_entry.encode (it, rssl_time);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"fieldId\": {}, \"dataType\": \"{}\", \"RQT_STM_MS\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					field_entry.fieldId(), DataTypes.toString (field_entry.dataType()), rssl_time);
				return false;
			}
// RQT_E_DATE+RQT_ETM_MS
			rssl_date.clear();
			rssl_date.year (stream.getInterval().getEnd().getYear());
			rssl_date.month (stream.getInterval().getEnd().getMonthOfYear());
			rssl_date.day (stream.getInterval().getEnd().getDayOfMonth());
			field_entry.dataType (DataTypes.DATE);
			field_entry.fieldId (9218);
			rc = field_entry.encode (it, rssl_date);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"fieldId\": {}, \"dataType\": \"{}\", \"RQT_E_DATE\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					field_entry.fieldId(), DataTypes.toString (field_entry.dataType()), rssl_date);
				return false;
			}
			rssl_time.clear();
			rssl_time.hour (stream.getInterval().getEnd().getHourOfDay());
			rssl_time.minute (stream.getInterval().getEnd().getMinuteOfHour());
			rssl_time.second (stream.getInterval().getEnd().getSecondOfMinute());
			rssl_time.millisecond (stream.getInterval().getEnd().getMillisOfSecond());
			field_entry.dataType (DataTypes.TIME);
			field_entry.fieldId (14224);
			rc = field_entry.encode (it, rssl_time);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"fieldId\": {}, \"dataType\": \"{}\", \"RQT_ETM_MS\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					field_entry.fieldId(), DataTypes.toString (field_entry.dataType()), rssl_time);
				return false;
			}
// optional: CORAX_ADJ
			rssl_enum.value (1);
			field_entry.dataType (DataTypes.ENUM);
			field_entry.fieldId (12886);
			rc = field_entry.encode (it, rssl_enum);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"fieldId\": {}, \"dataType\": \"{}\", \"CORAX_ADJ\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					field_entry.fieldId(), DataTypes.toString (field_entry.dataType()), rssl_enum);
				return false;
			}
// optional: MAX_POINTS
			rssl_int.value (100);
			field_entry.dataType (DataTypes.INT);
			field_entry.fieldId (7040);
			rc = field_entry.encode (it, rssl_int);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"fieldId\": {}, \"dataType\": \"{}\", \"MAX_POINTS\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					field_entry.fieldId(), DataTypes.toString (field_entry.dataType()), rssl_int);
				return false;
			}
			rc = field_list.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FieldList.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			rc = request.encodeKeyAttribComplete (it, true /* commit */);
			if (CodecReturnCodes.ENCODE_CONTAINER != rc) {
				LOG.error ("RequestMsg.encodeKeyAttribComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			rc = request.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("RequestMsg.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			rc = wrapper.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("GenericMsg.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
	
/* Message validation. */
			if (!wrapper.validateMsg()) {
				LOG.error ("RequestMsg.validateMsg failed.");
				return false;
			}

			if (0 == Submit (c, buf)) {
				return false;
			} else {
				return true;
			}
		}

		private void cancelItemRequest (AnalyticStream stream) {
/* Require confirmation on close request by app */
			if (!stream.hasCommandId() || stream.isClosed()) {
				LOG.trace ("Ignoring cancel request, analytic stream not open.");
				return;
			}
			LOG.trace ("Sending analytic query close request.");
			OMMMsg msg = this.omm_pool.acquireMsg();
			msg.setStreamId (stream.getStreamId());
			msg.setMsgType (OMMMsg.MsgType.REQUEST);
			msg.setMsgModelType ((short)30 /* RDMMsgTypes.ANALYTICS */);
//			msg.setAssociatedMetaInfo (this.private_stream);
/* RFA 7.6.0.L1 bug translates this to a NOP request which Signals interprets as a close.
 * RsslRequestFlags = 0x20 = RSSL_RQMF_NO_REFRESH
 * Indicates that the user does not require an RsslRefreshMsg for this request
 * - typically used as part of a reissue to change priority, view information,
 *   or pausing/resuming a stream. 
 */
			msg.setIndicationFlags (OMMMsg.Indication.PAUSE_REQ);
			msg.setAttribInfo (null, stream.getItemName(), (short)0x1 /* RIC */);

//			stream.setCommandId (this.sendGenericMsg (msg, this.private_stream, stream));
			this.omm_pool.releaseMsg (msg);
		}

		@Override
		public boolean OnMsg (Channel c, DecodeIterator it, Msg msg) {
			switch (msg.msgClass()) {
/* inside stream messages */
			case MsgClasses.GENERIC:
				return this.OnGenericMsg (c, it, msg);
/* private stream changes */
			default:
				return this.OnSystem (c, it, msg);
			}
		}

/* Raise request timeout */
		private void OnTimerEvent (Event event) {
			LOG.trace ("OnTimerEvent: {}", event);
			final AnalyticStream stream = (AnalyticStream)event.getClosure();
/* timer should be closed by RFA when non-repeating. */
			if (event.isEventStreamClosed()) {
				LOG.trace ("Timer handle for \"{}\" is closed.", stream.getQuery());
			} else if (null != stream.getTimerHandle()) {
				this.omm_consumer.unregisterClient (stream.getTimerHandle());
			}
/* no retry if private stream is not available */
			if (this.pending_connection) {
//				this.OnAnalyticsStatus (this.closed_response_status, stream, HttpURLConnection.HTTP_UNAVAILABLE);
				stream.clearTimerHandle();
			} else if (stream.getRetryCount() >= retry_limit) {
				final State state = CodecFactory.createState();
				state.streamState (StreamStates.OPEN);
				state.dataState (DataStates.SUSPECT);
				final Buffer text = CodecFactory.createBuffer();
				text.data ("Source did not respond.");
				state.text (text);
//				this.OnAnalyticsStatus (state,
//							stream,
//							HttpURLConnection.HTTP_GATEWAY_TIMEOUT);
/* prevent repeated invocation */
				stream.clearTimerHandle();
			} else {
				final State state = CodecFactory.createState();
				state.streamState (StreamStates.OPEN);
				state.dataState (DataStates.SUSPECT);
				final Buffer text = CodecFactory.createBuffer();
				text.data ("Source did not respond.  Retrying.");
				state.text (text);
//				this.OnAnalyticsStatus (state,
//							stream,
//							HttpURLConnection.HTTP_GATEWAY_TIMEOUT);
				stream.incrementRetryCount();
				this.sendItemRequest (connection, stream);
				this.registerRetryTimer (stream, retry_timer_ms);
			}
		}

/* encapsulated stream messages */
		private boolean OnGenericMsg (Channel c, DecodeIterator it, Msg msg) {
			LOG.trace ("OnGenericMsg: {}", msg);
			switch (msg.domainType()) {
			case DomainTypes.HISTORY:
				return this.OnHistory (c, it, msg);

			default:
				LOG.trace ("Uncaught: {}", msg);
				return true;
			}
		}

		private boolean OnHistory (Channel c, DecodeIterator it, Msg msg) {
			LOG.trace ("OnHistory: {}", msg);
			if (DataTypes.MSG != msg.containerType()) {
				LOG.warn ("Unexpected container type in HISTORY response.");
				return false;
			}
			final Msg encapsulated_msg = CodecFactory.createMsg();
			int rc = encapsulated_msg.decode (it);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("Msg.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			switch (encapsulated_msg.msgClass()) {
			case MsgClasses.REFRESH:
				return this.OnHistoryRefresh (c, it, (RefreshMsg)encapsulated_msg);
			case MsgClasses.STATUS:
				return this.OnHistoryStatus (c, it, (StatusMsg)encapsulated_msg);
			default:
				LOG.trace ("Uncaught: {}", encapsulated_msg);
				return true;
			}
		}

		private class LogMessage {
			private final String type;
			private final String service;
			private final String app;
			private final String recordname;  
			private final String query;  
			private final String stream;
			private final String data; 
			private final String code;
			private final String text;

			public LogMessage (String type, String service, String app, String recordname, String query, String stream, String data, String code, String text) {
				this.type = type;
				this.service = service;
				this.app = app;
				this.recordname = recordname;
				this.query = query;
				this.stream = stream;
				this.data = data;
				this.code = code;
				this.text = text;
			}
		}

	private static final boolean TEST_RWF15_TIME_ENCODING	= false;
	private static final boolean USE_RWF15_TIME_ENCODING	= false;

	private OMMMsg CreateTestMsg() {
		omm_encoder.initialize(OMMTypes.MSG, 500);
		OMMMsg msg = omm_pool.acquireMsg();
		msg.setMsgType(OMMMsg.MsgType.UPDATE_RESP);
		msg.setMsgModelType(RDMMsgTypes.MARKET_PRICE);
		msg.setIndicationFlags(OMMMsg.Indication.DO_NOT_CONFLATE);
		msg.setRespTypeNum(RDMInstrument.Update.QUOTE);
		omm_encoder.encodeMsgInit(msg, OMMTypes.NO_DATA, OMMTypes.SERIES);
		omm_encoder.encodeSeriesInit(OMMSeries.HAS_TOTAL_COUNT_HINT, OMMTypes.FIELD_LIST, 1);
		omm_encoder.encodeSeriesEntryInit();
		omm_encoder.encodeFieldListInit(OMMFieldList.HAS_STANDARD_DATA, (short)0, (short)1, (short)0);
		omm_encoder.encodeFieldEntryInit((short)14223, OMMTypes.TIME);
		omm_encoder.encodeTime(23, 59, 58, 123, 999, 512);
// special blank values
//		omm_encoder.encodeTime(255, 255, 255, 65535, 2047, 2047);
		omm_encoder.encodeAggregateComplete();
		omm_encoder.encodeAggregateComplete();
		return (OMMMsg)omm_encoder.getEncodedObject();
	}

/* Elektron Time Series refresh */
		private boolean OnHistoryRefresh (Channel c, DecodeIterator it, RefreshMsg msg) {
			LOG.trace ("OnHistoryResponse: {}", msg);
			final AnalyticStream stream = this.stream_map.get (msg.streamId());
			if (null == stream) {
				LOG.trace ("Ignoring response on stream id {} due to unregistered interest.", msg.streamId());
				return true;
			}
			if (DataTypes.SERIES != msg.containerType()) {
				LOG.trace ("Unsupported data type {} in HISTORY refresh.", msg.containerType());
				stream.getDispatcher().dispatch (stream, HttpURLConnection.HTTP_BAD_GATEWAY, "Unexpected data type.");
				this.destroyItemStream (stream);
				return false;
			}
			final Series series = CodecFactory.createSeries();
			int rc = series.decode (it);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("Series.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			if (DataTypes.FIELD_LIST != series.containerType()) {
				LOG.warn ("Unexpected data type {} in HISTORY refresh series.", series.containerType());
				stream.getDispatcher().dispatch (stream, HttpURLConnection.HTTP_BAD_GATEWAY, "Unexpected data type.");
				this.destroyItemStream (stream);
				return false;
			}
/* response includes a dictionary to decode the series data */
			final LocalFieldSetDefDb local_dictionary;
			if (0 != (series.flags() & SeriesFlags.HAS_SET_DEFS)) {
				LOG.trace ("Response includes local dictionary.");
				local_dictionary = CodecFactory.createLocalFieldSetDefDb();
				rc = local_dictionary.decode (it);
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.error ("LocalFieldSetDefDb.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
					return false;
				}
			} else {
				local_dictionary = null;
			}
			final SeriesEntry series_entry = CodecFactory.createSeriesEntry();
			final FieldList field_list = CodecFactory.createFieldList();
			final FieldEntry field_entry = CodecFactory.createFieldEntry();
			final com.thomsonreuters.upa.codec.Real rssl_real = CodecFactory.createReal();
			final com.thomsonreuters.upa.codec.UInt rssl_uint = CodecFactory.createUInt();
			final com.thomsonreuters.upa.codec.Enum rssl_enum = CodecFactory.createEnum();
			final com.thomsonreuters.upa.codec.Date rssl_date = CodecFactory.createDate();
			final com.thomsonreuters.upa.codec.Time rssl_time = CodecFactory.createTime();
			final com.thomsonreuters.upa.codec.Buffer rssl_buffer = CodecFactory.createBuffer();
			DictionaryEntry dictionary_entry;
			for (;;) {
				rc = series_entry.decode (it);
				if (CodecReturnCodes.END_OF_CONTAINER == rc)
					break;
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.error ("SeriesEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
					return false;
				}
// flatten to dataframe.
// SERIES 
//   SERIES_ENTRY
//     FIELD_LIST
//	 FIELD_ENTRY
				rc = field_list.decode (it, local_dictionary);
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.error ("FieldList.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
					return false;
				}
				ZonedDateTime datetime = ZonedDateTime.ofInstant (Instant.ofEpochSecond (0), ZoneId.of ("UTC"));
				String row = null;
				for (;;) {
					rc = field_entry.decode (it);
					if (CodecReturnCodes.END_OF_CONTAINER == rc)
						break;
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.error ("FieldEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
						return false;
					}
					dictionary_entry = rdm_dictionary.entry (field_entry.fieldId());
					switch (field_entry.fieldId()) {
					case 9217: // ITVL_DATE
						if (DataTypes.DATE == dictionary_entry.rwfType()) {
							final com.thomsonreuters.upa.codec.Date itvl_date = CodecFactory.createDate();
							itvl_date.decode (it);
							datetime = datetime.withYear (itvl_date.year())
										.withMonth (itvl_date.month())
										.withDayOfMonth (itvl_date.day());
							row = '"' + datetime.toInstant().toString() + '"';
						}
						break;
					case 14223: // ITVL_TM_MS
						if (DataTypes.TIME == dictionary_entry.rwfType()) {
							final com.thomsonreuters.upa.codec.Time itvl_tm = CodecFactory.createTime();
							itvl_tm.decode (it);
							datetime = datetime.withHour (itvl_tm.hour())
									.withMinute (itvl_tm.minute())
									.withSecond (itvl_tm.second())
									.withNano ((((itvl_tm.millisecond() * 1000) + itvl_tm.microsecond()) * 1000) + itvl_tm.nanosecond());
/* convert to get standard ISO 8601 Zulu "Z" suffix, otherwise "[UTC]" will apear */
							row = '"' + datetime.toInstant().toString() + '"';
						}
						break;
					default:
						switch (dictionary_entry.rwfType()) {
						case DataTypes.REAL:
							rssl_real.decode (it);
							stream.addResult (row, dictionary_entry.acronym().toString(), rssl_real.toString());
							break;
						case DataTypes.UINT:
							rssl_uint.decode (it);
							stream.addResult (row, dictionary_entry.acronym().toString(), rssl_uint.toString());
							break;
						case DataTypes.ENUM:
							rssl_enum.decode (it);
							stream.addResult (row, dictionary_entry.acronym().toString(), '"' + rdm_dictionary.entryEnumType (dictionary_entry, rssl_enum).display().toString() + '"');
							break;
						case DataTypes.RMTES_STRING:
							rssl_buffer.decode (it);
							stream.addResult (row, dictionary_entry.acronym().toString(), '"' + rssl_buffer.toString() + '"');
							break;
						case DataTypes.DATE:
							rssl_date.decode (it);
							stream.addResult (row, dictionary_entry.acronym().toString(), '"' + rssl_date.toString() + '"');
							break;
						case DataTypes.TIME:
							rssl_time.decode (it);
							stream.addResult (row, dictionary_entry.acronym().toString(), '"' + rssl_time.toString() + '"');
							break;
						default:
							rssl_buffer.decode (it);
							break;
						}
						break;
					}
				}
			}

			if (msg.isFinalMsg()) {
				sb.setLength (0);
				sb.append ('{')
				  .append ("\"recordname\":\"").append (stream.getItemName()).append ('\"')
				  .append (", \"start\":\"").append (stream.getInterval().getStart().toDateTime (DateTimeZone.UTC).toString()).append ('\"')
				  .append (", \"end\":\"").append (stream.getInterval().getEnd().toDateTime (DateTimeZone.UTC).toString()).append ('\"')
				  .append (", \"query\":\"").append (stream.getQuery()).append ('\"')
				  .append (", \"fields\": [\"datetime\"");
				final Set<String> fids = stream.getResultFids();
				for (Iterator jt = fids.iterator(); jt.hasNext();) {
					final String fid = (String)jt.next();
					sb.append (",")
					  .append ("\"")
					  .append (fid)
					  .append ("\"");
				}
				sb.append ("]")
				  .append (", \"timeseries\": [[");
				Joiner.on (",").appendTo (sb, stream.getResultDateTimes());
				sb.append ("]");
				for (Iterator jt = fids.iterator(); jt.hasNext();) {
					final String fid = (String)jt.next();
LOG.info ("array count {} -> {}", fid, stream.getResultForFid (fid).size());
					sb.append (",")
					  .append ("[");
					Joiner.on (",").appendTo (sb, stream.getResultForFid (fid));
					sb.append ("]");
				}
				sb.append ("]")
				  .append ("}");
				LOG.trace ("{}", sb.toString());
//				stream.getDispatcher().dispatch (stream, HttpURLConnection.HTTP_OK, sb.toString());
				this.destroyItemStream (stream);
			}

			return true;
		}

		private boolean OnHistoryStatus (Channel c, DecodeIterator it, StatusMsg msg) {
			LOG.trace ("OnHistoryStatus: {}", msg);
			final AnalyticStream stream = this.stream_map.get (msg.streamId());
			if (null == stream) {
				LOG.trace ("Ignoring response on stream id {} due to unregistered interest.", msg.streamId());
				return true;
			}
			if (msg.isFinalMsg()) {
				LOG.trace ("Query \"{}\" on service/app \"{}/{}\" is closed.",
					stream.getQuery(), stream.getServiceName(), stream.getAppName());
			}
/* auxiliary stream recovered. */
			if (0 != (msg.flags() & StatusMsgFlags.HAS_STATE)
				&& StreamStates.OPEN == msg.state().streamState()
				&& DataStates.OK == msg.state().dataState())
			{
				LOG.trace ("Query \"{}\" on service/app \"{}/{}\" has recovered.",
					stream.getQuery(), stream.getServiceName(), stream.getAppName());
				return true;
			}

/* Defer to GSON to escape status text. */
			LogMessage log_msg = new LogMessage (
				MsgClasses.toString (msg.msgClass()),
				stream.getServiceName(),
				stream.getAppName(),
				stream.getItemName(),
				stream.getQuery(),
				StreamStates.toString (msg.state().streamState()),
				DataStates.toString (msg.state().dataState()),
				StateCodes.toString (msg.state().code()),
				msg.state().text().toString());
			stream.getDispatcher().dispatch (stream, HttpURLConnection.HTTP_UNAVAILABLE, gson.toJson (log_msg));
			this.destroyItemStream (stream);
			return true;
		}

		private boolean OnSystem (Channel c, DecodeIterator it, Msg msg) {
			if (LOG.isDebugEnabled()) {
				LOG.debug ("App response:{}{}", LINE_SEPARATOR, DecodeToXml (msg, c.majorVersion(), c.minorVersion()));
			}

			State state = null;
			switch (msg.msgClass()) {
			case MsgClasses.CLOSE:
				return this.OnAppClosed (c, it, msg);
			case MsgClasses.REFRESH:
				state = ((RefreshMsg)msg).state();
				break;
			case MsgClasses.STATUS:
				state = ((StatusMsg)msg).state();
				break;
			default:
				LOG.warn ("Uncaught: {}", msg);
				return true;
			}

			assert (null != state);

/* extract out stream and data state like RFA */
			switch (state.streamState()) {
			case StreamStates.OPEN:
				switch (state.dataState()) {
				case DataStates.OK:
					return this.OnAppSuccess (c, it, msg);
				case DataStates.SUSPECT:
					return this.OnAppSuspect (c, it, msg);
				case DataStates.NO_CHANGE:
// by-definition, ignore
					return true;
				default:
					LOG.trace ("Uncaught data state: {}", state);
					return true;
				}

/* CLOSED is supposed to be a terminal status like something is not found or entitled.
 * CLOSED_RECOVER is a transient problem that the consumer should attempt recovery such as 
 * out of resources and thus unenable to store the request.
 */
			case StreamStates.CLOSED:
			case StreamStates.CLOSED_RECOVER:
				return this.OnAppClosed (c, it, msg);

			default:
				LOG.trace ("Uncaught stream state: {}", state);
				return true;
			}
		}

		private boolean OnAppSuccess (Channel c, DecodeIterator it, Msg msg) {
			LOG.trace ("OnAppSuccess: {}", msg);
			this.clearPendingConnection();
			LOG.trace ("Resubmitting analytics.");
/* Renumber all managed stream ids */
			this.resetStreamId();
			this.resubmit();
			return true;
		}

/* Transient problem, TREP will attempt to recover automatically */
		private boolean OnAppSuspect (Channel c, DecodeIterator it, Msg msg) {
			LOG.trace ("OnAppSuspect: {}", msg);
			return true;
		}

		private boolean OnAppClosed (Channel c, DecodeIterator it, Msg msg) {
			LOG.trace ("OnAppClosed: {}", msg);
			this.setPendingConnection();
/* Save state for future requests, generate one for message with no state field. */
			switch (msg.msgClass()) {
			case MsgClasses.CLOSE:
				this.closed_response_status = this.default_closed_response_status;
				break;
			case MsgClasses.REFRESH:
				this.closed_response_status = ((RefreshMsg)msg).state();
				break;
			case MsgClasses.STATUS:
				this.closed_response_status = ((StatusMsg)msg).state();
				break;
			default:
				LOG.warn ("Unhandled msgClass.");
				return false;
			}
/* Invalidate all existing identifiers */
			for (AnalyticStream stream : this.streams) {
/* Prevent attempts to send a close request */
				stream.close();
/* Destroy for snapshots */
//				this.OnHistoryStatus (this.closed_response_status,
//							stream,
//							HttpURLConnection.HTTP_UNAVAILABLE);
/* Cleanup */
				this.removeItemStream (stream);
			}
/* Await timer to re-open private stream, cache close message until connected. */
			return true;
		}

		private int acquireStreamId() {
			return this.stream_id++;
		}

		private void resetStreamId() {
			this.stream_id = DEFAULT_STREAM_IDENTIFIER;
			for (AnalyticStream stream : this.streams) {
				final int stream_id = this.acquireStreamId();
				if (stream_id != stream.getStreamId()) {
					this.stream_map.remove (stream.getStreamId());
					stream.setStreamId (stream_id);
					this.stream_map.put (stream_id, stream);
				}
			}
		}

		public boolean isEmpty() {
			return this.streams.isEmpty();
		}

		public int size() {
			return this.streams.size();
		}

		public boolean pendingConnection() {
			return this.pending_connection;
		}

		public void setPendingConnection() {
			this.pending_connection = true;
		}

		public void clearPendingConnection() {
			this.pending_connection = false;
		}

		public boolean hasConnectionHandle() {
			return 0 != this.private_stream.token;
		}

		public void sendConnectionRequest() {
			this.CreatePrivateStream (connection);
		}
	}

	private Map<String, App> apps;

/* Reuters Wire Format versions. */
	private byte rwf_major_version;
	private byte rwf_minor_version;

	private boolean is_muted;
	private boolean pending_directory;
	private boolean pending_dictionary;

	private int retry_timer_ms;
	private int retry_limit;

	private static final boolean UNSUBSCRIBE_ON_SHUTDOWN	= false;
	private static final boolean DO_NOT_CACHE_ZERO_VALUE	= true;
	private static final boolean DO_NOT_CACHE_BLANK_VALUE	= true;

	private static final int MAX_MSG_SIZE			= 4096;
	private static final int OMM_PAYLOAD_SIZE		= 5000;
	private static final int GC_DELAY_MS			= 15000;
	private static final int RESUBSCRIPTION_MS		= 180000;
	private static final int DEFAULT_RETRY_TIMER_MS		= 60000;
	private static final int DEFAULT_RETRY_LIMIT		= 0;
	private static final int DEFAULT_STREAM_IDENTIFIER	= 1;

	private static final String RSSL_PROTOCOL		= "rssl";

	public AnalyticConsumer (SessionConfig config, Upa upa) {
		this.config = config;
		this.upa = upa;
		this.apps = Maps.newLinkedHashMap();
		this.rwf_major_version = 0;
		this.rwf_minor_version = 0;
		this.is_muted = true;
		this.keep_running = true;
		this.pending_directory = true;
		this.pending_dictionary = true;

		this.retry_timer_ms = this.config.hasRetryTimer() ?
						(1000 * Integer.valueOf (this.config.getRetryTimer()))
						: DEFAULT_RETRY_TIMER_MS;
		this.retry_limit = this.config.hasRetryLimit() ?
						Integer.valueOf (this.config.getRetryLimit())
						: DEFAULT_RETRY_LIMIT;
	}

	public boolean Initialize() throws Exception {
		LOG.trace (this.config);

/* Manual serialisation */
		this.sb = new StringBuilder (512);

/* Null object support */
		this.gson = new GsonBuilder()
				.disableHtmlEscaping()
				.serializeNulls()
				.create();

/* RSSL Version Info. */
		if (!this.upa.VerifyVersion()) {
			return false;
		}

		this.directory = new LinkedList<>();
		this.tokens = new LinkedHashMap<>();
		this.dictionary_tokens = HashBiMap.create();
		this.rdm_dictionary = CodecFactory.createDataDictionary();
		return true;
	}

	public void Close() {
	}

/* includes input fds */
	private Selector selector;
	private Set<SelectionKey> out_keys;

	public void Run() {
		LOG.trace ("Run");

// throws IOException for undocumented reasons.
		try {
			this.selector = Selector.open();
LOG.trace ("select -> {}/{}", this.selector.keys().size(), this.selector.selectedKeys().size());
		} catch (IOException e) {
			LOG.catching (e);
			this.keep_running = true;
			return;
		}
		this.out_keys = null;
		final long timeout = 1000 * 100;

		while (true) {
			boolean did_work = DoWork();

			if (!this.keep_running)
				break;

			if (did_work)
				continue;

			try {
				final int rc = this.selector.select (timeout /* milliseconds */);
				if (rc > 0) {
					this.out_keys = this.selector.selectedKeys();
				} else {
					this.out_keys = null;
				}
			} catch (Exception e) {
				LOG.catching (e);
			}
		}

		this.keep_running = true;
	}

	private boolean DoWork() {
		LOG.trace ("DoWork");
		boolean did_work = false;

		this.last_activity = Instant.now();

/* Only check keepalives on timeout */
		if (null == this.out_keys
			&& null != this.connection)
		{
			final Channel c = this.connection;
			LOG.debug ("timeout, state {}", ChannelState.toString (c.state()));
			if (ChannelState.ACTIVE == c.state()) {
				if (this.last_activity.isAfter (this.NextPing())) {
					this.Ping (c);
				}
				if (this.last_activity.isAfter (this.NextPong())) {
					LOG.error ("Pong timeout from peer, aborting connection.");
					this.Abort (c);
				}
			}
			return false;
		}

/* Client connection */
		if (null == this.connection) {
			this.Connect();
			did_work = true;
		}

		if (null != this.connection && null != this.out_keys) {
			final Channel c = this.connection;
			Iterator<SelectionKey> it = this.selector.selectedKeys().iterator();
			while (it.hasNext()) {
				final SelectionKey key = it.next();
				key.attach (Boolean.TRUE);
/* connected */
				if (key.isConnectable()) {
					key.attach (Boolean.FALSE);
					this.OnCanConnectWithoutBlocking (c);
					did_work = true;
				}
/* incoming */
				if (key.isReadable()) {
					key.attach (Boolean.FALSE);
					this.OnCanReadWithoutBlocking (c);
					did_work = true;
				}
/* outgoing */
				if (key.isWritable()) {
					key.attach (Boolean.FALSE);
					this.OnCanWriteWithoutBlocking (c);
					did_work = true;
				}
				if (Boolean.FALSE.equals (key.attachment())) {
					it.remove();
				}
			}
/* Keepalive timeout on active session above connection */
			if (ChannelState.ACTIVE == c.state()) {
				if (this.last_activity.isAfter (this.NextPing())) {
					this.Ping (c);
				}
				if (this.last_activity.isAfter (this.NextPong())) {
					LOG.error ("Pong timeout from peer, aborting connection.");
					this.Abort (c);
				}
			}
/* disconnects */
		}

		return did_work;
	}

	public void Quit() {
		this.keep_running = false;
	}

	private void Connect() {
		final ConnectOptions addr = TransportFactory.createConnectOptions();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		LOG.info ("Initiating new connection.");

/* non-blocking mode to be used with a Selector. */
		addr.blocking (false);
		addr.channelReadLocking (false);
		addr.channelWriteLocking (false);
		addr.unifiedNetworkInfo().address (this.config.getServers()[0]);
		addr.unifiedNetworkInfo().serviceName (this.config.hasDefaultPort() ? this.config.getDefaultPort() : "14002");
		addr.protocolType (Codec.protocolType());
		addr.majorVersion (Codec.majorVersion());
		addr.minorVersion (Codec.minorVersion());
		final Channel c = Transport.connect (addr, rssl_err);
		if (null == c) {
			LOG.error ("Transport.connect: { \"errorId\": {}, \"sysError\": {}, \"text\": \"{}\", \"connectionInfo\": {}, \"protocolType\": {}, \"majorversion\": {}, \"minorVersion\": {} }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
				addr.unifiedNetworkInfo(), addr.protocolType(), addr.majorVersion(), addr.minorVersion());
		} else {
			this.connection = c;
/* Set logger ID */
			this.prefix = Integer.toHexString (c.hashCode());

/* Wait for session */
			try {
				c.selectableChannel().register (this.selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE, Boolean.FALSE);
			} catch (ClosedChannelException e) {
/* leave error handling to Channel wrapper */
				LOG.catching (e);
			}

			LOG.info ("RSSL socket created: { \"connectionType\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {}, \"pingTimeout\": {}, \"protocolType\": {}, \"socketId\": {}, \"state\": \"{}\" }",
				ConnectionTypes.toString (c.connectionType()), c.majorVersion(), c.minorVersion(), c.pingTimeout(), c.protocolType(), c.selectableChannel().hashCode(), ChannelState.toString (c.state()));
		}
	}

	private void OnCanConnectWithoutBlocking (Channel c) {
		LOG.trace ("OnCanConnectWithoutBlocking");
		switch (c.state()) {
		case ChannelState.CLOSED:
			LOG.info ("socket state is closed.");
			this.Abort (c);
			break;
		case ChannelState.INACTIVE:
			LOG.info ("socket state is inactive.");
			break;
		case ChannelState.INITIALIZING:
			LOG.info ("socket state is initializing.");
			this.OnInitializingState (c);
			break;
		default:
			LOG.info ("unhandled socket state.");
			break;
		}
	}

	private void OnCanReadWithoutBlocking (Channel c) {
		LOG.trace ("OnCanReadWithoutBlocking");
		switch (c.state()) {
		case ChannelState.CLOSED:
			LOG.info ("socket state is closed.");
/* Raise internal exception flags to remove socket */
			this.Abort (c);
			break;
		case ChannelState.INACTIVE:
			LOG.info ("socket state is inactive.");
			break;
		case ChannelState.INITIALIZING:
			LOG.info ("socket state is initializing.");
			break;
		case ChannelState.ACTIVE:
			this.OnActiveReadState (c);
			break;
		default:
			LOG.error ("socket state is unknown.");
			break;
		}
	}

	private void OnInitializingState (Channel c) {
		final InProgInfo state = TransportFactory.createInProgInfo();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		final int rc = c.init (state, rssl_err);
		switch (rc) {
		case TransportReturnCodes.CHAN_INIT_IN_PROGRESS:
			if (InProgFlags.SCKT_CHNL_CHANGE == (state.flags() & InProgFlags.SCKT_CHNL_CHANGE)) {
				LOG.info ("RSSL protocol downgrade, reconnected.");
				state.oldSelectableChannel().keyFor (this.selector).cancel();
				try {
					c.selectableChannel().register (this.selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, Boolean.FALSE);
				} catch (ClosedChannelException e) {
					LOG.catching (e);
				}
			} else {
				LOG.info ("RSSL connection in progress.");
			}
			break;
		case TransportReturnCodes.SUCCESS:
			this.OnActiveSession (c);
			try {
				c.selectableChannel().register (this.selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, Boolean.FALSE);
			} catch (ClosedChannelException e) {
				LOG.catching (e);
			}
			break;
		default:
			LOG.error ("Channel.init: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			break;
		}
	}

	private void OnCanWriteWithoutBlocking (Channel c) {
		LOG.trace ("OnCanWriteWithoutBlocking");
		switch (c.state()) {
		case ChannelState.CLOSED:
			LOG.info ("socket state is closed.");
/* Raise internal exception flags to remove socket */
			this.Abort (c);
			break;
		case ChannelState.INACTIVE:
			LOG.info ("socket state is inactive.");
			break;
		case ChannelState.INITIALIZING:
			LOG.info ("socket state is initializing.");
			this.OnInitializingState (c);
			break;
		case ChannelState.ACTIVE:
			this.OnActiveWriteState (c);
			break;
		default:
			LOG.error ("socket state is unknown.");
			break;
		}
	}

	private void OnActiveWriteState (Channel c) {
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		LOG.trace ("rsslFlush");
		final int rc = c.flush (rssl_err);
		if (TransportReturnCodes.SUCCESS == rc) {
			final SelectionKey key = c.selectableChannel().keyFor (selector);
			key.interestOps (key.interestOps() & ~SelectionKey.OP_WRITE);
			this.ClearPendingCount();
			this.SetNextPing (this.last_activity.plusSeconds (this.ping_interval));
		} else if (rc > 0) {
			LOG.info ("{} bytes pending.", rc);
		} else {
			LOG.error ("Channel.flush: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}
	}

	private void Abort (Channel c) {
		c.selectableChannel().keyFor (selector).cancel();
		try {
			c.selectableChannel().close();
		} catch (IOException e) {
			LOG.catching (e);
		}
	}

	private void Close (Channel c) {
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		LOG.info ("Closing RSSL connection.");
		final int rc = c.close (rssl_err);
		if (TransportReturnCodes.SUCCESS != rc) {
			LOG.warn ("Channel.close: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}
	}

	private boolean OnActiveSession (Channel c) {
		final ChannelInfo info = TransportFactory.createChannelInfo();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		this.last_activity = Instant.now();

/* Relog negotiated state. */
		LOG.info ("RSSL negotiated state: { \"connectionType\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {}, \"pingTimeout\": {}, \"protocolType\": \"{}\", \"socketId\": {}, \"state\": \"{}\" }",
			ConnectionTypes.toString (c.connectionType()), c.majorVersion(), c.minorVersion(), c.pingTimeout(), c.protocolType(), c.selectableChannel(), ChannelState.toString (c.state()));

/* Store negotiated Reuters Wire Format version information. */
		final int rc = c.info (info, rssl_err);
		if (TransportReturnCodes.SUCCESS != rc) {
			LOG.error ("Channel.info: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return false;
		}

/* Log connected infrastructure. */
		final StringBuilder components = new StringBuilder ("[ ");
		final Iterator<ComponentInfo> it = info.componentInfo().iterator();
		while (it.hasNext()) {
			final ComponentInfo component = it.next();
			components.append ("{ ")
				.append ("\"componentVersion\": \"").append (component.componentVersion()).append ("\"")
				.append (" }");
			if (it.hasNext())
				components.append (", ");
		}
		components.append (" ]");

		LOG.info ("channelInfo: { \"clientToServerPings\": {}, \"componentInfo\": {}, \"compressionThreshold\": {}, \"compressionType\": \"{}\", \"guaranteedOutputBuffers\": {}, \"maxFragmentSize\": {}, \"maxOutputBuffers\": {}, \"numInputBuffers\": {}, \"pingTimeout\": {}, \"priorityFlushStrategy\": \"{}\", \"serverToClientPings\": \"{}\", \"sysRecvBufSize\": {}, \"sysSendBufSize\": {} }",
			info.clientToServerPings(), components.toString(), info.compressionThreshold(), info.compressionType(), CompressionTypes.toString (info.compressionType()), info.guaranteedOutputBuffers(), info.maxFragmentSize(), info.maxOutputBuffers(), info.numInputBuffers(), info.pingTimeout(), info.priorityFlushStrategy(), info.serverToClientPings(), info.sysRecvBufSize(), info.sysSendBufSize());
/* First token aka stream id */
		this.token = 1;
		this.dictionary_tokens.clear();
/* Derive expected RSSL ping interval from negotiated timeout. */
		this.ping_interval = c.pingTimeout() / 3;
/* Schedule first RSSL ping. */
		this.SetNextPing (this.last_activity.plusSeconds (this.ping_interval));
/* Treat connect as first RSSL pong. */
		this.SetNextPong (this.last_activity.plusSeconds (c.pingTimeout()));
/* Reset RDM data dictionary and wait to request from upstream. */
		return this.SendLoginRequest (c);
	}

/* Create an item stream for a given symbol name.  The Item Stream maintains
 * the provider state on behalf of the application.
 */
	private void createItemStream (Instrument instrument, ItemStream item_stream) {
/* Construct directory unique key */
		this.sb.setLength (0);
		this.sb	.append (instrument.getService())
			.append ('.')
			.append (instrument.getName());
		this.createItemStream (instrument, item_stream, this.sb.toString());
	}

	private void createItemStream (Instrument instrument, ItemStream item_stream, String key) {
		LOG.trace ("Creating item stream for RIC \"{}\" on service \"{}\".", instrument.getName(), instrument.getService());
		item_stream.setItemName (instrument.getName());
		item_stream.setServiceName (instrument.getService());

		if (!this.pending_dictionary) {
			this.sendItemRequest (this.connection, item_stream);
		}
		this.directory.add (item_stream);
		LOG.trace ("Directory size: {}", this.directory.size());
	}

	private void destroyItemStream (ItemStream item_stream) {
/* Construct directory unique key */
		this.sb.setLength (0);
		this.sb .append (item_stream.getServiceName())
			.append ('.')
			.append (item_stream.getItemName());
		this.destroyItemStream (item_stream, this.sb.toString());
	}

	private void destroyItemStream (ItemStream item_stream, String key) {
		LOG.trace ("Destroying item stream for RIC \"{}\" on service \"{}\".", item_stream.getItemName(), item_stream.getServiceName());
//		this.cancelItemRequest (item_stream);
		this.directory.remove (key);
		LOG.trace ("Directory size: {}", this.directory.size());
	}

/* no native support by provider, so emulate functionality */
	public void batchCreateAnalyticStream (Analytic[] analytics, AnalyticStream[] streams) {
		for (int i = 0; i < analytics.length; ++i) {
			this.createAnalyticStream (analytics[i], streams[i]);
		}
	}

	public void createAnalyticStream (Analytic analytic, AnalyticStream stream) {
		stream.setQuery (analytic.getQuery());
		stream.setItemName (analytic.getItem());
		stream.setAppName (analytic.getApp());
		stream.setServiceName (analytic.getService());
		stream.setInterval (analytic.getInterval());
/* lazy app private stream creation */
		App app = this.apps.get (analytic.getApp());
		if (null == app) {
			app = new App ( analytic.getService(),
					analytic.getApp(),
					this.config.hasUuid() ? this.config.getUuid() : "",
					this.config.hasPassword() ? this.config.getPassword() : "");
			this.apps.put (analytic.getApp(), app);
			if (!this.is_muted) {
				app.sendConnectionRequest();
			}
		}
/* TBD: no stream de-duplication */
		app.createItemStream (stream);
		LOG.trace ("App \"{}\" stream count: {}", analytic.getApp(), app.size());
	}

	public void destroyAnalyticStream (AnalyticStream analytic_stream) {
		App app = this.apps.get (analytic_stream.getAppName());
		app.destroyItemStream (analytic_stream);
/* TBD: remove app */
		LOG.trace ("App \"{}\" stream count: {}", analytic_stream.getAppName(), app.size());
	}

/* Convert a view by FID name to a view by FID values */
	private ImmutableSortedSet<Integer> createViewByFid (ImmutableSortedSet<String> view_by_name) {
		final ArrayList<Integer> fid_list = new ArrayList<Integer> (view_by_name.size());
		for (String name : view_by_name) {
			final Integer fid = this.appendix_a.get (name);
			if (null == fid) {
				LOG.error ("Field \"{}\" not described in appendix_a dictionary.", name);
			} else {
				fid_list.add (fid);
			}
		}
		final Integer[] fid_array = fid_list.toArray (new Integer [fid_list.size()]);
		return ImmutableSortedSet.copyOf (fid_array);
	}

/* Convert a set of FID names to psuedo ripple field names */
	private ImmutableMap<Integer, String> createRippleFieldDictionary (ImmutableSortedSet<String> view_by_name) {
		Map<Integer, String> map = Maps.newHashMap();
		for (String name : view_by_name) {
			final Integer fid = this.appendix_a.get (name);
			if (null == fid) {
				LOG.warn ("Field \"{}\" not described in appendix_a dictionary.", name);
			} else {
				this.sb.setLength (0);
				this.sb.append (name)
					.append ("_PRV");
				map.put (fid, this.sb.toString());
			}
		}
		return ImmutableMap.copyOf (map);
	}

	public boolean Resubscribe (Channel c) {
		LOG.debug ("Resubscribe");
		if (this.is_muted) {
			LOG.debug ("Cancelling item resubscription due to pending session.");
			return true;
		}

/* private streams for apps */
		for (App app : this.apps.values()) {
			if (!app.hasConnectionHandle()) {
				app.sendConnectionRequest();
			}
		}

/* individual item streams */
		for (ItemStream item_stream : this.directory) {
			if (-1 == item_stream.token) {
				this.sendItemRequest (c, item_stream);
			}
		}

		return true;
	}

	public void Resubscribe () {
/* Cannot decode responses so do not allow wire subscriptions until dictionary is present */
		if (this.pending_dictionary)
			return;
		if (null == this.omm_consumer) {
			LOG.warn ("Resubscribe whilst consumer is invalid.");
			return;
		}

/* retry app streams */
		for (App app : this.apps.values()) {
			if (!app.hasConnectionHandle()) {
				app.sendConnectionRequest();
			}
		}

/* item streams */
		for (ItemStream item_stream : this.directory) {
			if (-1 == item_stream.token) {
				this.sendItemRequest (this.connection, item_stream);
			}
		}
	}

	private void OnActiveReadState (Channel c) {
		final ReadArgs read_args = TransportFactory.createReadArgs();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		final TransportBuffer buf = c.read (read_args, rssl_err);
		final int rc = read_args.readRetVal();
		if (rc > 0) {
			LOG.info ("Channel.read: { \"pendingBytes\": {}, \"bytesRead\": {}, \"uncompressedBytesRead\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rc,
				read_args.bytesRead(), read_args.uncompressedBytesRead(), rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		} else {
			LOG.info ("Channel.read: { \"returnCode\": {}, \"enumeration\": \"{}\", \"bytesRead\": {}, \"uncompressedBytesRead\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rc, TransportReturnCodes.toString (rc),
				read_args.bytesRead(), read_args.uncompressedBytesRead(), rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}

		if (TransportReturnCodes.CONGESTION_DETECTED == rc
			|| TransportReturnCodes.SLOW_READER == rc
			|| TransportReturnCodes.PACKET_GAP_DETECTED == rc)
		{
			if (ChannelState.CLOSED != c.state()) {
				LOG.warn ("Channel.read: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			}
		}
		else if (TransportReturnCodes.READ_FD_CHANGE == rc)
		{
			LOG.info ("RSSL reconnected.");
			c.oldSelectableChannel().keyFor (this.selector).cancel();
			try {
				c.selectableChannel().register (this.selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, Boolean.FALSE);
			} catch (ClosedChannelException e) {
				LOG.catching (e);
			}
		}
		else if (TransportReturnCodes.READ_PING == rc)
		{
			this.SetNextPong (this.last_activity.plusSeconds (c.pingTimeout()));
			LOG.info ("RSSL pong.");
		}
		else if (TransportReturnCodes.FAILURE == rc)
		{
			LOG.error ("Channel.read: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}
		else 
		{
			if (null != buf) {
				this.OnMsg (c, buf);
/* Received data equivalent to a heartbeat pong. */
				this.SetNextPong (this.last_activity.plusSeconds (c.pingTimeout()));
			}
			if (rc > 0)
			{
/* pending buffer needs flushing out before IO notification can resume */
				final SelectionKey key = c.selectableChannel().keyFor (selector);
				key.attach (Boolean.TRUE);
			}
		}
	}

	private boolean sendItemRequest (Channel c, ItemStream item_stream) {
		LOG.trace ("Sending market price request.");
		final RequestMsg request = (RequestMsg)CodecFactory.createMsg();
/* Set the message model type. */
		request.domainType (DomainTypes.MARKET_PRICE);
/* Set request type. */
		request.msgClass (MsgClasses.REQUEST);
		request.flags (RequestMsgFlags.STREAMING);
/* No view thus no payload. */
		request.containerType (DataTypes.NO_DATA);
/* Set the stream token. */
		request.streamId (this.token);

/* In RFA lingo an attribute object */
		request.msgKey().nameType (InstrumentNameTypes.RIC);
		request.msgKey().name().data (item_stream.getItemName());
		request.msgKey().serviceId (this.service_map.get (item_stream.getServiceName()));
		request.msgKey().flags (MsgKeyFlags.HAS_NAME_TYPE | MsgKeyFlags.HAS_NAME | MsgKeyFlags.HAS_SERVICE_ID);

		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
		if (null == buf) {
			LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
				MAX_MSG_SIZE);
			return false;
		}
		final EncodeIterator it = CodecFactory.createEncodeIterator();
		it.clear();
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
				c.majorVersion(), c.minorVersion());
			return false;
		}
		rc = request.encode (it);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("RequestMsg.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

/* Message validation. */
		if (!request.validateMsg()) {
			LOG.error ("RequestMsg.validateMsg failed.");
			return false;
		}

		if (0 == this.Submit (c, buf)) {
			return false;
		} else {
			this.tokens.put (item_stream.token = this.token++, item_stream);
			return true;
		}
	}

	private boolean OnMsg (Channel c, TransportBuffer buf) {
		final DecodeIterator it = CodecFactory.createDecodeIterator();
		it.clear();
		final Msg msg = CodecFactory.createMsg();

/* Prepare codec */
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

/* Decode data buffer into RSSL message */
		rc = msg.decode (it);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("Msg.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		} else {
			if (LOG.isDebugEnabled()) {
/* Pass through RSSL validation and report exceptions */
				if (!msg.validateMsg()) {
					LOG.warn ("Msg.ValidateMsg failed.");
					this.Abort (c);
					return false;
				} else {
					LOG.debug ("Msg.ValidateMsg success.");
				}
/*				final DecodeIterator jt = CodecFactory.createDecodeIterator();
				jt.clear();
				rc = jt.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.error ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
					return false;
				}
				LOG.debug ("{}", msg.decodeToXml (jt)); */
			}
			if (!this.OnMsg (c, it, msg))
				this.Abort (c);
			return true;
		}
	}

	private String DecodeToXml (Msg msg, int major_version, int minor_version) {
		final DecodeIterator it = CodecFactory.createDecodeIterator();
		it.clear();
		final int rc = it.setBufferAndRWFVersion (msg.encodedMsgBuffer(), major_version, minor_version);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return "";
		} else {
			return msg.decodeToXml (it);
		}
	}

/* Returns true if message processed successfully, returns false to abort the connection.
 */
	@Override
	public boolean OnMsg (Channel c, DecodeIterator it, Msg msg) {
		switch (msg.domainType()) {
		case DomainTypes.LOGIN:
			return this.OnLoginResponse (c, it, msg);
		case DomainTypes.SOURCE:
			return this.OnDirectory (c, it, msg);
		case DomainTypes.DICTIONARY:
			return this.OnDictionary (c, it, msg);
		case DomainTypes.HISTORY:
		case DomainTypes.ANALYTICS:
		case DomainTypes.SYSTEM:
			return this.OnSystem (c, it, msg);
		default:
			LOG.warn ("Uncaught message: {}", this.DecodeToXml (msg, c.majorVersion(), c.minorVersion()));
			return true;
		}
	}

	private boolean OnLoginResponse (Channel c, DecodeIterator it, Msg msg) {
		State state = null;

		switch (msg.msgClass()) {
		case MsgClasses.CLOSE:
			return this.OnLoginClosed (c, it, msg);

		case MsgClasses.REFRESH:
			state = ((RefreshMsg)msg).state();
			break;

		case MsgClasses.STATUS:
			state = ((StatusMsg)msg).state();
			break;

		default:
			LOG.warn ("Uncaught: {}", msg);
			return true;
		}

		assert (null != state);

/* extract out stream and data state like RFA */
		switch (state.streamState()) {
		case StreamStates.OPEN:
			switch (state.dataState()) {
			case DataStates.OK:
				return this.OnLoginSuccess (c, it, msg);
			case DataStates.SUSPECT:
				return this.OnLoginSuspect (c, it, msg);
			case DataStates.NO_CHANGE:
// by-definition, ignore
				return true;
			default:
				LOG.warn ("Uncaught data state: {}", msg);
				return true;
			}

		case StreamStates.CLOSED:
			return this.OnLoginClosed (c, it, msg);

		default:
			LOG.warn ("Uncaught stream state: {}", msg);
			return true;
		}
	}

	private boolean OnDirectory (Channel c, DecodeIterator it, Msg msg) {
		switch (msg.msgClass()) {
		case MsgClasses.REFRESH:
			return this.OnDirectoryRefresh (c, it, (RefreshMsg)msg);
		case MsgClasses.UPDATE:
			return this.OnDirectoryUpdate (c, it, (UpdateMsg)msg);
		default:
			LOG.warn ("Uncaught directory response message type: {}", msg);
			return false;
		}
	}

	static final String FIELD_DICTIONARY_NAME = "RWFFld";
	static final String ENUM_TYPE_DICTIONARY_NAME = "RWFEnum";

	private boolean OnDirectoryRefresh (Channel c, DecodeIterator it, RefreshMsg response) {
		LOG.debug ("OnDirectoryRefresh");
		if (DataTypes.MAP != response.containerType()) {
			LOG.warn ("Directory refresh container type unexpected.");
			return false;
		}
		final com.thomsonreuters.upa.codec.Map map = CodecFactory.createMap();
		int rc = map.decode (it);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.warn ("Map.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		if (DataTypes.FILTER_LIST != map.containerType()) {
			LOG.warn ("Map container type unexpected.");
			return false;
		}
		if (DataTypes.UINT != map.keyPrimitiveType()) {
			LOG.warn ("Map key primitive type unexpected.");
			return false;
		}
		final com.thomsonreuters.upa.codec.MapEntry map_entry = CodecFactory.createMapEntry();
		final com.thomsonreuters.upa.codec.UInt rssl_uint = CodecFactory.createUInt();
		final FilterList filter_list = CodecFactory.createFilterList();
		final FilterEntry filter_entry = CodecFactory.createFilterEntry();
		final ElementList element_list = CodecFactory.createElementList();
		final ElementEntry element_entry = CodecFactory.createElementEntry();
		final ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();
		for (;;) {
			rc = map_entry.decode (it, rssl_uint);
			if (CodecReturnCodes.END_OF_CONTAINER == rc)
				break;
			if (CodecReturnCodes.BLANK_DATA == rc)
				continue;
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.warn ("MapEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc));
				return false;
			}
			final int service_id = (int)rssl_uint.toLong();
/* refresh should never include a DELETE but the example code handles this case. */
			if (MapEntryActions.DELETE != map_entry.action()) {
				rc = filter_list.decode (it);
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.warn ("FilterList.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc));
					return false;
				}
				for (;;) {
					rc = filter_entry.decode (it);
					if (CodecReturnCodes.END_OF_CONTAINER == rc)
						break;
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.warn ("FilterEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc));
						return false;
					}
/* absolute minimum of service name and id */
					if (Directory.ServiceFilterIds.INFO != filter_entry.id())
						continue;
					rc = element_list.decode (it, null /* local definitions */);
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.warn ("ElementList.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc));
						return false;
					}
					for (;;) {
						rc = element_entry.decode (it);
						if (CodecReturnCodes.END_OF_CONTAINER == rc)
							break;
						if (CodecReturnCodes.SUCCESS != rc) {
							LOG.warn ("ElementEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
								rc, CodecReturnCodes.toString (rc));
							return false;
						}
						if (element_entry.name().equals (ElementNames.NAME)) {
							final Buffer rssl_buffer = element_entry.encodedData();
							builder.put (rssl_buffer.toString(), service_id);
							LOG.trace ("Service {} applied to map with action {}.", service_id, filter_entry.action());
						}
					}
				}
			}
		}
		this.service_map = builder.build();

/* Request dictionary on first directory message. */
		if (0 == this.rdm_dictionary.enumTableCount()
			&& 0 == this.rdm_dictionary.numberOfEntries())
		{
			if (this.service_map.isEmpty()) {
				LOG.warn ("Upstream provider has no configured services, unable to request a dictionary.");
				return true;
			}
			final int service_id = this.service_map.values().iterator().next();
/* Hard code to RDM dictionary for TREP deployment. */
			if (!this.SendDictionaryRequest (c, service_id, FIELD_DICTIONARY_NAME))
				return false;
			if (!this.SendDictionaryRequest (c, service_id, ENUM_TYPE_DICTIONARY_NAME))
				return false;
		}

		return this.Resubscribe (c);
	}

	private boolean OnDirectoryUpdate (Channel c, DecodeIterator it, UpdateMsg response) {
		LOG.debug ("OnDirectoryUpdate");
		if (DataTypes.MAP != response.containerType()) {
			LOG.warn ("Directory update container type unexpected.");
			return false;
		}
		final com.thomsonreuters.upa.codec.Map map = CodecFactory.createMap();
		int rc = map.decode (it);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.warn ("Map.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		if (DataTypes.FILTER_LIST != map.containerType()) {
			LOG.warn ("Map container type unexpected.");
			return false;
		}
		if (DataTypes.UINT != map.keyPrimitiveType()) {
			LOG.warn ("Map key primitive type unexpected.");
			return false;
		}
		final com.thomsonreuters.upa.codec.MapEntry map_entry = CodecFactory.createMapEntry();
		final com.thomsonreuters.upa.codec.UInt rssl_uint = CodecFactory.createUInt();
		final FilterList filter_list = CodecFactory.createFilterList();
		final FilterEntry filter_entry = CodecFactory.createFilterEntry();
		final ElementList element_list = CodecFactory.createElementList();
		final ElementEntry element_entry = CodecFactory.createElementEntry();
		final TreeMap<String,Integer> new_map = new TreeMap<>();
		new_map.putAll (this.service_map);
		for (;;) {
			rc = map_entry.decode (it, rssl_uint);
			if (CodecReturnCodes.END_OF_CONTAINER == rc)
				break;
			if (CodecReturnCodes.BLANK_DATA == rc)
				continue;
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.warn ("Ma@Entry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc));
				return false;
			}
			final int service_id = (int)rssl_uint.toLong();
			switch (map_entry.action()) {
			case MapEntryActions.DELETE:
				LOG.trace ("Removing service id {}", service_id);
				new_map.remove (this.service_map.inverse().get (service_id));
				break;

			case MapEntryActions.ADD:
			case MapEntryActions.UPDATE:
				rc = filter_list.decode (it);
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.warn ("FilterList.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc));
					return false;
				}
				for (;;) {
					rc = filter_entry.decode (it);
					if (CodecReturnCodes.END_OF_CONTAINER == rc)
						break;
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.warn ("FilterEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc));
						return false;
					}
/* absolute minimum of service name and id */
					if (Directory.ServiceFilterIds.INFO != filter_entry.id())
						continue;
					rc = element_list.decode (it, null /* local definitions */);
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.warn ("ElementList.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc));
						return false;
					}
					for (;;) {
						rc = element_entry.decode (it);
						if (CodecReturnCodes.END_OF_CONTAINER == rc)
							break;
						if (CodecReturnCodes.SUCCESS != rc) {
							LOG.warn ("ElementEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
								rc, CodecReturnCodes.toString (rc));
							return false;
						}
						if (element_entry.name().equals (ElementNames.NAME)) {
							final Buffer rssl_buffer = element_entry.encodedData();
							switch (filter_entry.action()) {
							case FilterEntryActions.SET:
							case FilterEntryActions.UPDATE:
								LOG.trace ("Service {} applied to map with action {}.", service_id, filter_entry.action());
								new_map.put (rssl_buffer.toString(), service_id);
								break;
							case FilterEntryActions.CLEAR:
								LOG.trace ("Removing service {} due to CLEAR filter entry action.", service_id);
								new_map.remove (this.service_map.inverse().get (service_id));
								break;
							default:
								LOG.warn ("Unexpected filter entry action.");
								return false;
							}
						}
					}
				}
			}
		}
		this.service_map = ImmutableBiMap.copyOf (new_map);
		return this.Resubscribe (c);
	}

	private boolean OnDictionary (Channel c, DecodeIterator it, Msg msg) {
		LOG.debug ("OnDictionary");

		if (MsgClasses.REFRESH != msg.msgClass()) {
/* Status can show a new dictionary but is not implemented in TREP-RT infrastructure, so ignore. */
/* Close should only happen when the infrastructure is in shutdown, defer to closed MMT_LOGIN. */
			LOG.warn ("Uncaught dictionary response message type: {}", msg);
			return true;
		}

		return this.OnDictionaryRefresh (c, it, (RefreshMsg)msg);
	}

/* thunk to app object for private stream processing. */
	private boolean OnSystem (Channel c, DecodeIterator it, Msg msg) {
		final int token = msg.streamId();
		LOG.trace ("token {}", token);
		final ItemStream stream = this.tokens.get (token);
		if (null == stream) {
			LOG.error ("SYSTEM domain message received on unregistered token.");
			return false;
		}
		return stream.delegate.OnMsg (c, it, msg);
	}

/* Replace any existing RDM dictionary upon a dictionary refresh message.
 */
	private boolean OnDictionaryRefresh (Channel c, DecodeIterator it, RefreshMsg response) {
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		int rc;

		LOG.debug ("OnDictionaryRefresh");
		if (0 == (response.flags() & RefreshMsgFlags.HAS_MSG_KEY)) {
			LOG.warn ("Dictionary refresh messages should contain a msgKey component, rejecting.");
			return false;
		}
		final MsgKey msg_key = response.msgKey();

		switch (this.dictionary_tokens.inverse().get (response.streamId())) {
		case FIELD_DICTIONARY_NAME:
			rc = this.rdm_dictionary.decodeFieldDictionary (it, Dictionary.VerbosityValues.NORMAL, rssl_err);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.info ("DataDictionary.decodeFieldDictionary: { \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
				return false;
			}
			break;

		case ENUM_TYPE_DICTIONARY_NAME:
			rc = this.rdm_dictionary.decodeEnumTypeDictionary (it, Dictionary.VerbosityValues.NORMAL, rssl_err);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.info ("DataDictionary.decodeEnumTypeDictionary: { \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
				return false;
			}
			break;

/* Ignore unused dictionaries */
		default:
			return true;
		}

		if (0 != (response.flags() & RefreshMsgFlags.REFRESH_COMPLETE)
			&& 0 != this.rdm_dictionary.enumTableCount()
			&& 0 != this.rdm_dictionary.numberOfEntries())
		{
			LOG.info ("Dictionary reception complete.");
/* Permit new subscriptions. */
			this.is_muted = false;
			return this.Resubscribe (c);
		}
		return true;
	}

	private boolean OnLoginSuccess (Channel c, DecodeIterator it, Msg msg) {
		LOG.debug ("OnLoginSuccess");
/* Log upstream application name, only presented in refresh messages. */
		switch (msg.msgClass()) {
		case MsgClasses.REFRESH:
			if (0 != (msg.flags() & RefreshMsgFlags.HAS_MSG_KEY)) {
				final MsgKey msg_key = msg.msgKey();
				if (0 != (msg_key.flags() & MsgKeyFlags.HAS_ATTRIB)) {
					int rc = msg.decodeKeyAttrib (it, msg_key);
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.warn ("Msg.decodeKeyAttrib: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc));
						return false;
					}
					final ElementList element_list = CodecFactory.createElementList();
					rc = element_list.decode (it, null /* local definitions */);
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.warn ("ElementList.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc));
						return false;
					}
					final ElementEntry element_entry = CodecFactory.createElementEntry();
					for (;;) {
						rc = element_entry.decode (it);
						if (CodecReturnCodes.END_OF_CONTAINER == rc)
							break;
						if (CodecReturnCodes.SUCCESS != rc) {
							LOG.warn ("ElementEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
								rc, CodecReturnCodes.toString (rc));
							return false;
						}
						if (element_entry.name().equals (ElementNames.APPNAME)) {
							if (DataTypes.ASCII_STRING != element_entry.dataType()) {
								LOG.warn ("Element entry APPNAME not expected data type ASCII_STRING.");
								return false;
							}
							final Buffer buffer = element_entry.encodedData();
							LOG.info ("applicationName: \"{}\"", buffer.toString());
							break;
						}
					}
				}
			}
		default:
			break;
		}
/* A new connection to TREP infrastructure, request dictionary to discover available services. */
		return this.SendDirectoryRequest (c);
	}

	private boolean OnLoginSuspect (Channel c, DecodeIterator it, Msg msg) {
		LOG.debug ("OnLoginSuspect");
		this.is_muted = true;
		return true;
	}

	private boolean OnLoginClosed (Channel c, DecodeIterator it, Msg msg) {
		LOG.debug ("OnLoginClosed");
		this.is_muted = true;
		return true;
	}

	private int Submit (Channel c, TransportBuffer buf) {
		final WriteArgs write_args = TransportFactory.createWriteArgs();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		int rc;

		write_args.priority (WritePriorities.LOW);	/* flushing priority */
/* direct write on clear socket, enqueue when writes are pending */
		boolean should_write_direct = (0 == (c.selectableChannel().keyFor (this.selector).interestOps() & SelectionKey.OP_WRITE));
		write_args.flags (should_write_direct ? WriteFlags.DIRECT_SOCKET_WRITE : WriteFlags.NO_FLAGS);

		do {
			rc = c.write (buf, write_args, rssl_err);
			if (rc > 0) {
				LOG.info ("Channel.write: { \"pendingBytes\": {}, \"bytesWritten\": {}, \"uncompressedBytesWritten\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
					rc,
					write_args.bytesWritten(), write_args.uncompressedBytesWritten(), rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			} else {
				LOG.info ("Channel.write: { \"returnCode\": {}, \"enumeration\": \"{}\", \"bytesWritten\": {}, \"uncompressedBytesWritten\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
					rc, TransportReturnCodes.toString (rc),
					write_args.bytesWritten(), write_args.uncompressedBytesWritten(), rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			}

			if (rc > 0) {
				this.IncrementPendingCount();
			}
			if (rc > 0 
/* attempted to flush data to the connection but was blocked. */
				|| TransportReturnCodes.WRITE_FLUSH_FAILED == rc
/* empty buffer pool: spin wait until buffer is available. */
				|| TransportReturnCodes.NO_BUFFERS == rc)
			{
/* pending output */
				try {
					c.selectableChannel().keyFor (this.selector).interestOps (SelectionKey.OP_READ | SelectionKey.OP_WRITE);
				} catch (Exception e) {
					LOG.catching (e);
				}
				return -1;
			}
/* fragmenting the buffer and needs to be called again with the same buffer. */
		} while (TransportReturnCodes.WRITE_CALL_AGAIN == rc);
/* sent, no flush required. */
		if (TransportReturnCodes.SUCCESS != rc) {
			LOG.info ("Channel.write: { \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return 0;
		}
/* Sent data equivalent to a ping. */
		this.SetNextPing (this.last_activity.plusSeconds (this.ping_interval));
		return 1;
	}

	private boolean Ping (Channel c) {
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		final int rc = c.ping (rssl_err);
		if (rc > 0) {
			LOG.info ("Channel.ping: { \"pendingBytes\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rc,
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		} else {
			LOG.info ("Channel.ping: { \"returnCode\": {}, \"enumeration\": \"{}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rc, TransportReturnCodes.toString (rc),
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}
		if (TransportReturnCodes.WRITE_FLUSH_FAILED == rc
			|| TransportReturnCodes.NO_BUFFERS == rc
			|| rc > 0)
		{
			this.Abort (c);
			LOG.error ("Channel.ping: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return false;
		} else if (TransportReturnCodes.SUCCESS == rc) {	/* sent, no flush required. */
/* Advance ping expiration only on success. */
			this.SetNextPing (this.last_activity.plusSeconds (this.ping_interval));
			return true;
		} else {
			LOG.error ("Channel.ping: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return false;
		}
	}

/* Making a Login Request
 * A Login request message is encoded and sent by OMM Consumer and OMM non-
 * interactive provider applications.
 */
	private boolean SendLoginRequest (Channel c) {
		LOG.trace ("Sending login request.");
		final RequestMsg request = (RequestMsg)CodecFactory.createMsg();
/* Set the message model type. */
		request.domainType (DomainTypes.LOGIN);
/* Set request type. */
		request.msgClass (MsgClasses.REQUEST);
		request.flags (RequestMsgFlags.STREAMING);
/* No payload. */
		request.containerType (DataTypes.NO_DATA);
/* Set the login token. */
		request.streamId (this.login_token = this.token++);
LOG.debug ("login token {}", this.login_token);

/* DACS username (required). */
		request.msgKey().nameType (Login.UserIdTypes.NAME);
		request.msgKey().name().data (this.config.hasUserName() ?
						this.config.getUserName()
						: System.getProperty ("user.name"));
		request.msgKey().flags (MsgKeyFlags.HAS_NAME_TYPE | MsgKeyFlags.HAS_NAME);

/* Login Request Elements */
		request.msgKey().attribContainerType (DataTypes.ELEMENT_LIST);
		request.msgKey().flags (request.msgKey().flags() | MsgKeyFlags.HAS_ATTRIB);

		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
		if (null == buf) {
			LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
			return false;
		}
		final EncodeIterator it = CodecFactory.createEncodeIterator();
		it.clear();
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
			return false;
		}
		rc = request.encodeInit (it, MAX_MSG_SIZE);
		if (CodecReturnCodes.ENCODE_MSG_KEY_ATTRIB != rc) {
			LOG.error ("RequestMsg.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"dataMaxSize\": {} }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc), MAX_MSG_SIZE);
			return false;
		}

/* Encode attribute object after message instead of before as per RFA. */
		final ElementList element_list = CodecFactory.createElementList();
		final ElementEntry element_entry = CodecFactory.createElementEntry();
		final com.thomsonreuters.upa.codec.Buffer rssl_buffer = CodecFactory.createBuffer();
		final com.thomsonreuters.upa.codec.UInt rssl_uint = CodecFactory.createUInt();
		element_list.flags (ElementListFlags.HAS_STANDARD_DATA);
		rc = element_list.encodeInit (it, null /* element id dictionary */, 0 /* count of elements */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("RequestMsg.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"flags\": \"HAS_STANDARD_DATA\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
/* Do not permit stale data, item stream should be closed. */
		final int disallow_suspect_data = 0;
		rssl_uint.value (disallow_suspect_data);
		element_entry.dataType (DataTypes.UINT);
		element_entry.name (ElementNames.ALLOW_SUSPECT_DATA);
		rc = element_entry.encode (it, rssl_uint);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"allowSuspectData\": {} }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
				element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_uint);
			return false;
		}
/* DACS Application Id (optional).
 * e.g. "256"
 */
		if (this.config.hasApplicationId()) {
			rssl_buffer.data (this.config.getApplicationId());
			element_entry.dataType (DataTypes.ASCII_STRING);
			element_entry.name (ElementNames.APPID);
			rc = element_entry.encode (it, rssl_buffer);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"applicationId\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_buffer);
				return false;
			}
		}
/* Instance Id (optional).
 * e.g. "<Instance Id>"
 */
		if (this.config.hasInstanceId()) {
			rssl_buffer.data (this.config.getInstanceId());
			element_entry.dataType (DataTypes.ASCII_STRING);
			element_entry.name (ElementNames.INST_ID);
			rc = element_entry.encode (it, rssl_buffer);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"instanceId\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_buffer);
				return false;
			}
		}
/* DACS Position name (optional).
 * e.g. "127.0.0.1/net"
 */
		String position = null;
		if (this.config.hasPosition()) {
			if (!this.config.getPosition().isEmpty())
				position = this.config.getPosition();
			else
				position = "";
		} else {
			this.sb.setLength (0);
			try {
				this.sb .append (InetAddress.getLocalHost().getHostAddress())
					.append ('/')
					.append (InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e) {
				LOG.catching (e);
				return false;
			}
			position = this.sb.toString();
		}
		rssl_buffer.data (position);
		element_entry.dataType (DataTypes.ASCII_STRING);
		element_entry.name (ElementNames.POSITION);
		rc = element_entry.encode (it, rssl_buffer);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"position\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
				element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_buffer);
			return false;
		}
		rc = element_list.encodeComplete (it, true /* commit */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("ElementList.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		rc = request.encodeKeyAttribComplete (it, true /* commit */);
		if (CodecReturnCodes.ENCODE_CONTAINER != rc) {
			LOG.error ("RequestMsg.encodeKeyAttribComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		rc = request.encodeComplete (it, true /* commit */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("RequestMsg.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

/* Message validation. */
		if (!request.validateMsg()) {
			LOG.error ("RequestMsg.validateMsg failed.");
			return false;
		}

		if (0 == this.Submit (c, buf)) {
			return false;
		} else {
/* Reset status */
			this.pending_directory = true;
// Maintain current status of dictionary instead of interrupting existing consumers.
//			this.pending_dictionary = true;
			return true;
		}
	}

/* Make a directory request to see if we can ask for a dictionary.
 */
	private boolean SendDirectoryRequest (Channel c) {
		LOG.trace ("Sending directory request.");
		final RequestMsg request = (RequestMsg)CodecFactory.createMsg();
/* Set the message model type. */
		request.domainType (DomainTypes.SOURCE);
/* Set request type. */        
		request.msgClass (MsgClasses.REQUEST);
		request.flags (RequestMsgFlags.STREAMING);
/* No payload. */
		request.containerType (DataTypes.NO_DATA);
/* Set the directory token. */
		request.streamId (this.token);	/* login + 1 */
LOG.debug ("directory token {}", this.token);

/* In RFA lingo an attribute object, TBD: group, load filters. */
		request.msgKey().filter (Directory.ServiceFilterFlags.INFO	// service names
					| Directory.ServiceFilterFlags.STATE);	// up or down
		request.msgKey().flags (MsgKeyFlags.HAS_FILTER);

		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
		if (null == buf) {
			LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
			return false;
		}
		final EncodeIterator it = CodecFactory.createEncodeIterator();
		it.clear();
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
			return false;
		}
		rc = request.encode (it);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("RequestMsg.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

/* Message validation. */
		if (!request.validateMsg()) {
			LOG.error ("RequestMsg.validateMsg failed.");
			return false;
		}

		if (0 == this.Submit (c, buf)) {
			return false;
		} else {
/* advance token counter only on success, re-use token on failure. */
			this.directory_token = this.token++;
			return true;
		}
	}

/* Make a dictionary request.
 *
 * 5.8.3 Version Check
 * Dictionary version checking can be performed by the client after a refresh
 * (Section 2.2) response message of a Dictionary is received.
 */
	private boolean SendDictionaryRequest (Channel c, int service_id, String dictionary_name) {
		LOG.trace ("Sending dictionary request for \"{}\" from service #{}.", dictionary_name, service_id);
		final RequestMsg request = (RequestMsg)CodecFactory.createMsg();
/* Set the message model type. */
		request.domainType (DomainTypes.DICTIONARY);
/* Set request type. */
		request.msgClass (MsgClasses.REQUEST);
		request.flags (RequestMsgFlags.NONE);
/* No payload. */
		request.containerType (DataTypes.NO_DATA);
/* Set the dictionary token. */
		request.streamId (this.token);
LOG.debug ("dictionary token {}", this.token);

/* In RFA lingo an attribute object. */
		request.msgKey().serviceId (service_id);
		final Buffer rssl_buf = CodecFactory.createBuffer();
		rssl_buf.data (dictionary_name);
		request.msgKey().name (rssl_buf);
// RDMDictionary.Filter.NORMAL=0x7: Provides all information needed for decoding
		request.msgKey().filter (Dictionary.VerbosityValues.NORMAL);
		request.msgKey().flags (MsgKeyFlags.HAS_SERVICE_ID | MsgKeyFlags.HAS_NAME | MsgKeyFlags.HAS_FILTER);

		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
		if (null == buf) {
			LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
			return false;
		}
		final EncodeIterator it = CodecFactory.createEncodeIterator();
		it.clear();
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
			return false;
		}
		rc = request.encode (it);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("RequestMsg.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

/* Message validation. */
		if (!request.validateMsg()) {
			LOG.error ("RequestMsg.validateMsg failed.");
			return false;
		}

		if (0 == this.Submit (c, buf)) {
			return false;
		} else {
/* re-use token on failure. */
			this.dictionary_tokens.put (dictionary_name, this.token++);
			return true;
		}
	}

	public void processEvent (Event event) {
		LOG.trace (event);
		switch (event.getType()) {
		case Event.OMM_ITEM_EVENT:
			this.OnOMMItemEvent (this.connection, (OMMItemEvent)event);
			break;

// RFA 7.5.1
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
	private void OnOMMItemEvent (Channel c, OMMItemEvent event) {
		LOG.trace ("OnOMMItemEvent: {}", event);
		final OMMMsg msg = event.getMsg();

/* Verify event is a response event. */
		switch (msg.getMsgType()) {
		case OMMMsg.MsgType.REFRESH_RESP:
		case OMMMsg.MsgType.UPDATE_RESP:
		case OMMMsg.MsgType.STATUS_RESP:
		case OMMMsg.MsgType.ACK_RESP:
			this.OnRespMsg (c, msg, event.getHandle(), event.getClosure());
			break;

/* Generic message */
		case OMMMsg.MsgType.GENERIC:
/* Request message */
		case OMMMsg.MsgType.REQUEST:
/* Post message */
		case OMMMsg.MsgType.POST:
		default:
			LOG.trace ("Uncaught: {}", msg);
			break;
		}
	}

	private void OnRespMsg (Channel c, OMMMsg msg, Handle handle, Object closure) {
		LOG.trace ("OnRespMsg: {}", msg);
		switch (msg.getMsgModelType()) {
		case RDMMsgTypes.LOGIN:
			this.OnLoginResponse (c, msg);
			break;

		case RDMMsgTypes.DIRECTORY:
			this.OnDirectoryResponse (c, msg);
			break;

		case RDMMsgTypes.DICTIONARY:
			this.OnDictionaryResponse (c, msg, handle, closure);
			break;

		case RDMMsgTypes.MARKET_PRICE:
			this.OnMarketPrice (c, msg);
			break;

		default:
			LOG.trace ("Uncaught: {}", msg);
			break;
		}
	}

	private void OnLoginResponse (Channel c, OMMMsg msg) {
		LOG.trace ("OnLoginResponse: {}", msg);
		if (LOG.isDebugEnabled()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream (baos);
			GenericOMMParser.parseMsg (msg, ps);
			LOG.debug ("Login response:{}{}", LINE_SEPARATOR, baos.toString());
		}
		final RDMLoginResponse response = new RDMLoginResponse (msg);
		final byte stream_state = response.getRespStatus().getStreamState();
		final byte data_state	= response.getRespStatus().getDataState();

		switch (stream_state) {
		case OMMState.Stream.OPEN:
			switch (data_state) {
			case OMMState.Data.OK:
				this.OnLoginSuccess (c, response);
				break;

			case OMMState.Data.SUSPECT:
				this.OnLoginSuspect (c, response);
				break;

			default:
				LOG.trace ("Uncaught data state: {}", response);
				break;
			}
			break;

		case OMMState.Stream.CLOSED:
			this.OnLoginClosed (c, response);
			break;

		default:
			LOG.trace ("Uncaught stream state: {}", response);
			break;
		}
	}

/* Login Success.
 */
	private void OnLoginSuccess (Channel c, RDMLoginResponse response) {
		LOG.trace ("OnLoginSuccess: {}", response);
		LOG.trace ("Unmuting consumer.");
		this.is_muted = false;
		if (!this.pending_dictionary)
			this.Resubscribe();
	}

/* Other Login States.
 */
	private void OnLoginSuspect (Channel c, RDMLoginResponse response) {
		LOG.trace ("OnLoginSuspect: ResponseStatus: {}", response.getRespStatus());
		this.is_muted = true;
	}

/* Login Closed.
 */
	private void OnLoginClosed (Channel c, RDMLoginResponse response) {
		LOG.trace ("OnLoginClosed: {}", response);
		this.is_muted = true;
	}

/* MMT_DIRECTORY domain.  Request RDM dictionaries, RWFFld and RWFEnum, from first available service.
 */
	private void OnDirectoryResponse (Channel c, OMMMsg msg) {
		LOG.trace ("OnDirectoryResponse: {}", msg);
		if (LOG.isDebugEnabled()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream (baos);
			GenericOMMParser.parseMsg (msg, ps);
			LOG.debug ("Directory response:{}{}", LINE_SEPARATOR, baos.toString());
		}

// We only desire a single directory response with UP status to request dictionaries, ignore all other updates */
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
		int dictionary_service_id = 0;
/*		for (Service service : payload.getServiceList()) {
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
		} */

		if (Strings.isNullOrEmpty (dictionary_service)) {
			LOG.trace ("No service available to accept dictionary requests, waiting for service change in directory update.");
			return;
		}

/* Hard code to RDM dictionary names */
//		if (!this.dictionary_handle.containsKey ("RWFFld")) {
/* Local file override */
			if (!this.config.hasFieldDictionary()) {
				this.SendDictionaryRequest (c, dictionary_service_id, "RWFFld");
			} else {
final FieldDictionary field_dictionary = null;
//				final FieldDictionary field_dictionary = this.rdm_dictionary.getFieldDictionary();
				FieldDictionary.readRDMFieldDictionary (field_dictionary, this.config.getFieldDictionary());
/* Additional meta-data only from file dictionaries */
				LOG.trace ("RDM field dictionary file \"{}\": { " +
						  "\"Desc\": \"{}\"" +
						", \"Version\": \"{}\"" +
						", \"Build\": \"{}\"" +
						", \"Date\": \"{}\"" +
						" }",
						this.config.getFieldDictionary(),
						field_dictionary.getFieldProperty ("Desc"),
						field_dictionary.getFieldProperty ("Version"),
						field_dictionary.getFieldProperty ("Build"),
						field_dictionary.getFieldProperty ("Date"));
			}
//		}

//		if (!this.dictionary_handle.containsKey ("RWFEnum")) {
			if (!this.config.hasEnumDictionary()) {
				this.SendDictionaryRequest (c, dictionary_service_id, "RWFEnum");
			} else {
final FieldDictionary field_dictionary = null;
//				final FieldDictionary field_dictionary = this.rdm_dictionary.getFieldDictionary();
				FieldDictionary.readEnumTypeDef (field_dictionary, this.config.getEnumDictionary());
				LOG.trace ("RDM enumerated tables file \"{}\": { " +
						  "\"Desc\": \"{}\"" +
						", \"RT_Version\": \"{}\"" +
						", \"Build_RDMD\": \"{}\"" +
						", \"DT_Version\": \"{}\"" +
						", \"Date\": \"{}\"" +
						" }",
						this.config.getEnumDictionary(),
						field_dictionary.getEnumProperty ("Desc"),
						field_dictionary.getEnumProperty ("RT_Version"),
						field_dictionary.getEnumProperty ("Build_RDMD"),
						field_dictionary.getEnumProperty ("DT_Version"),
						field_dictionary.getEnumProperty ("Date"));
			}
//		}

//		if (0 == this.dictionary_handle.size()) {
			if (LOG.isDebugEnabled()) {
//				GenericOMMParser.initializeDictionary (this.rdm_dictionary.getFieldDictionary());
			}
			LOG.trace ("All dictionaries loaded, resuming subscriptions.");
			this.pending_dictionary = false;
			this.Resubscribe();
//		}

/* Directory received and processed, ignore all future updates. */
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
	private void OnDictionaryResponse (Channel c, OMMMsg msg, Handle handle, Object closure) {
		LOG.trace ("OnDictionaryResponse: {}", msg);
		final RDMDictionaryResponse response = new RDMDictionaryResponse (msg);
/* Receiving dictionary */
		if (response.hasAttrib()) {
			LOG.trace ("Dictionary {}: {}", response.getMessageType(), response.getAttrib().getDictionaryName());
		}
		if (response.getMessageType() == RDMDictionaryResponse.MessageType.REFRESH_RESP
			&& response.hasPayload() && null != response.getPayload())
		{
//			this.rdm_dictionary.load (response.getPayload(), handle);
		}

/* Only know type after it is loaded. */
final RDMDictionary.DictionaryType dictionary_type = null;
//		final RDMDictionary.DictionaryType dictionary_type = this.rdm_dictionary.getDictionaryType (handle);

/* Received complete dictionary */
		if (response.getMessageType() == RDMDictionaryResponse.MessageType.REFRESH_RESP
			&& response.getIndicationMask().contains (RDMDictionaryResponse.IndicationMask.REFRESH_COMPLETE))
		{
			LOG.trace ("Dictionary complete.");
/* Check dictionary version */
FieldDictionary field_dictionary = null;
//			FieldDictionary field_dictionary = this.rdm_dictionary.getFieldDictionary();
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
			if (LOG.isDebugEnabled()) {
				GenericOMMParser.initializeDictionary (field_dictionary);
			}
//			this.dictionary_handle.get ((String)closure).setFlag();

/* Check all pending dictionaries */
//			int pending_dictionaries = this.dictionary_handle.size();
//			for (FlaggedHandle flagged_handle : this.dictionary_handle.values()) {
//				if (flagged_handle.isFlagged())
//					--pending_dictionaries;
//			}
//			if (0 == pending_dictionaries) {
//				LOG.trace ("All used dictionaries loaded, resuming subscriptions.");
//				this.pending_dictionary = false;
//				this.Resubscribe();
//			} else {
//				LOG.trace ("Dictionaries pending: {}", pending_dictionaries);
//			}
		}
	}

// RFA 7.5.1
	private void OnConnectionEvent (OMMConnectionEvent event) {
		LOG.trace ("OnConnectionEvent: {}", event);
		LOG.info ("Connection status {} for {}@{}:{}",
				event.getConnectionStatus().toString(),
				event.getConnectedComponentVersion(), event.getConnectedHostName(), event.getConnectedPort());
	}

/* MMT_MARKETPRICE domain.
 */
	private void OnMarketPrice (Channel c, OMMMsg msg) {
	}

}

/* eof */
