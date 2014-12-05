/* Private-stream based analytics app consumer.
 */

package com.thomsonreuters.Takoyaki;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.joda.time.DateTime;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Floats;
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
import com.reuters.rfa.omm.OMMNumeric;
import com.reuters.rfa.omm.OMMPool;
import com.reuters.rfa.omm.OMMState;
import com.reuters.rfa.omm.OMMTypes;
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
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.Service;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLogin;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.app.login.AppLogin;
import com.thomsonreuters.rfa.valueadd.domainrep.app.login.AppLoginRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.app.login.AppLoginRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.app.login.AppLoginResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.ResponseStatus;

public class AnalyticConsumer implements Client {
	private static Logger LOG = LogManager.getLogger (Consumer.class.getName());
	private static final Marker SHOGAKOTTO_MARKER = MarkerManager.getMarker ("SHOGAKOTTO");

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
	private OMMEncoder omm_encoder, omm_encoder2;

	private Set<Integer> field_set;

/* JSON serialisation */
	private Gson gson;
	private StringBuilder sb;

/* Data dictionaries. */
	private RDMDictionaryCache rdm_dictionary;

/* Directory */
	private Map<String, ItemStream> directory;

/* RFA Item event consumer */
	private Handle error_handle;
	private Handle login_handle;
	private Handle directory_handle;

/* Resubscription management via timer */
	private Handle resubscription_handle;
	private SubscriptionManager subscription_manager;

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

	private class App implements Client {
/* ERROR: modifier 'static' is only allowed in constant variable declarations */
		private Logger LOG = LogManager.getLogger (App.class.getName());

		private EventQueue event_queue;
		private OMMConsumer omm_consumer;
		private OMMPool omm_pool;
		private OMMEncoder omm_encoder;
		private OMMEncoder omm_encoder2;
		private Handle login_handle;		/* to infrastructure */
		private String service_name;
		private String app_name;
		private String uuid;
		private List<AnalyticStream> streams;
		private LinkedHashMap<Integer, AnalyticStream> stream_map;
		private int stream_id;
		private boolean pending_connection;	/* to app */
		private ResponseStatus closed_response_status;
		private Handle private_stream;

		public App (EventQueue event_queue, OMMConsumer omm_consumer, OMMPool omm_pool, OMMEncoder omm_encoder, OMMEncoder omm_encoder2, Handle login_handle, String service_name, String app_name, String uuid) {
			this.event_queue = event_queue;
			this.omm_consumer = omm_consumer;
			this.omm_pool = omm_pool;
			this.omm_encoder = omm_encoder;
			this.omm_encoder2 = omm_encoder2;
			this.login_handle = login_handle;
			this.service_name = service_name;
			this.app_name = app_name;
			this.uuid = uuid;
			this.streams = Lists.newLinkedList();
			this.stream_map = Maps.newLinkedHashMap();
			this.resetStreamId();
			this.setPendingConnection();
// Appears until infrastructure returns new close status to present.
			this.closed_response_status = new ResponseStatus (OMMState.Stream.CLOSED, OMMState.Data.SUSPECT, OMMState.Code.NO_RESOURCES, "No service private stream available to process the request.");
		}

		private void createPrivateStream() {
			LOG.trace ("Creating app \"{}\" private stream on service \"{}\".",
				this.app_name, this.service_name);
			OMMMsg msg = this.omm_pool.acquireMsg();
			msg.setMsgType (OMMMsg.MsgType.REQUEST);
			msg.setMsgModelType ((short)30 /* RDMMsgTypes.ANALYTICS */);
			msg.setAssociatedMetaInfo (this.login_handle);
			msg.setIndicationFlags (OMMMsg.Indication.REFRESH | OMMMsg.Indication.PRIVATE_STREAM);
			msg.setAttribInfo (this.service_name, this.app_name, AppLogin.NameType.getValue (AppLogin.NameType.APP));
			OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
			if (!this.uuid.isEmpty()) {
/* Authorization for the app */
				this.omm_encoder.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);
				this.omm_encoder.encodeMsgInit (msg, OMMTypes.ELEMENT_LIST, OMMTypes.NO_DATA);
				this.omm_encoder.encodeElementListInit (OMMElementList.HAS_STANDARD_DATA, (short)0, (short)0);
				this.omm_encoder.encodeElementEntryInit ("UUID", OMMTypes.ASCII_STRING);
				this.omm_encoder.encodeString (this.uuid, OMMTypes.ASCII_STRING);
				this.omm_encoder.encodeAggregateComplete();
				ommItemIntSpec.setMsg ((OMMMsg)this.omm_encoder.getEncodedObject());
			} else {
				ommItemIntSpec.setMsg (msg);
			}
GenericOMMParser.parse (msg);
			this.private_stream = this.omm_consumer.registerClient (this.event_queue, ommItemIntSpec, this, null);
			this.omm_pool.releaseMsg (msg);
		}

		public void createItemStream (AnalyticStream stream) {
			LOG.trace ("Creating analytic stream for query \"{}\" to app \"{}\" on service \"{}\".",
				stream.getQuery(), stream.getAppName(), stream.getServiceName());
			stream.setStreamId (this.acquireStreamId());

			if (!this.pending_connection)
				this.sendItemRequest (stream);
			this.streams.add (stream);
			this.stream_map.put (stream.getStreamId(), stream);

// transient state feedback
			this.registerRetryTimer (stream, retry_timer_ms);
		}

		private void registerRetryTimer (AnalyticStream stream, int retry_timer_ms) {
			final TimerIntSpec timer = new TimerIntSpec();
			timer.setDelay (retry_timer_ms);
			final Handle timer_handle = this.omm_consumer.registerClient (this.event_queue, timer, this, stream);
			if (timer_handle.isActive())
				stream.setTimerHandle (timer_handle);
			else
				LOG.error ("Timer handle for query \"{}\" closed on registration.", stream.getQuery());
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
					this.sendItemRequest (stream);
			}
		}

		private void sendItemRequest (AnalyticStream stream) {
			LOG.trace ("Sending analytic query request.");
			OMMMsg msg = this.omm_pool.acquireMsg();
			msg.setStreamId (stream.getStreamId());
			msg.setMsgType (OMMMsg.MsgType.REQUEST);
			msg.setMsgModelType ((short)30 /* RDMMsgTypes.ANALYTICS */);
			msg.setAssociatedMetaInfo (this.private_stream);
// TBD: SignalsApp does not support snapshot requests.
			if (stream.getAppName().equals ("SignalApp")) {
				msg.setIndicationFlags (OMMMsg.Indication.REFRESH | OMMMsg.Indication.PRIVATE_STREAM);
				msg.setAttribInfo (null, stream.getItemName(), (short)0x1 /* RIC */);
			} else {
				msg.setIndicationFlags (OMMMsg.Indication.REFRESH | OMMMsg.Indication.NONSTREAMING | OMMMsg.Indication.PRIVATE_STREAM);
				sb.setLength (0);
				sb.append ("/").append (stream.getItemName());
				msg.setAttribInfo (null, sb.toString(), (short)0x1 /* RIC */);
			}

/* OMMAttribInfo.Attrib as an OMMElementList */
			this.omm_encoder.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);
			this.omm_encoder.encodeMsgInit (msg, OMMTypes.ELEMENT_LIST, OMMTypes.NO_DATA);
			this.omm_encoder.encodeElementListInit (OMMElementList.HAS_STANDARD_DATA, (short)0, (short)0);
			this.omm_encoder.encodeElementEntryInit ("Query", OMMTypes.ASCII_STRING);
			this.omm_encoder.encodeString (stream.getQuery(), OMMTypes.ASCII_STRING);
			this.omm_encoder.encodeAggregateComplete();

			stream.setCommandId (this.sendGenericMsg ((OMMMsg)this.omm_encoder.getEncodedObject(), this.private_stream, stream));
			this.omm_pool.releaseMsg (msg);
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
			msg.setAssociatedMetaInfo (this.private_stream);
/* RFA 7.6.0.L1 bug translates this to a NOP request which Signals interprets as a close.
 * RsslRequestFlags = 0x20 = RSSL_RQMF_NO_REFRESH
 * Indicates that the user does not require an RsslRefreshMsg for this request
 * - typically used as part of a reissue to change priority, view information,
 *   or pausing/resuming a stream. 
 */
			msg.setIndicationFlags (OMMMsg.Indication.PAUSE_REQ);
			msg.setAttribInfo (null, stream.getItemName(), (short)0x1 /* RIC */);

			stream.setCommandId (this.sendGenericMsg (msg, this.private_stream, stream));
			this.omm_pool.releaseMsg (msg);
		}

		private int sendGenericMsg (OMMMsg encapsulated_msg, Handle stream_handle, java.lang.Object closure) {
			LOG.trace ("Sending generic message request.");
			OMMMsg msg = this.omm_pool.acquireMsg();
			msg.setMsgType (OMMMsg.MsgType.GENERIC);
			msg.setMsgModelType ((short)30 /* RDMMsgTypes.ANALYTICS */);
			msg.setAssociatedMetaInfo (stream_handle);
			msg.setIndicationFlags (OMMMsg.Indication.GENERIC_COMPLETE);

/* Encapsulate provided message */
			this.omm_encoder2.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);
			this.omm_encoder2.encodeMsgInit (msg, OMMTypes.NO_DATA, OMMTypes.MSG);
			this.omm_encoder2.encodeMsg (encapsulated_msg);

			OMMHandleItemCmd cmd = new OMMHandleItemCmd();
			cmd.setMsg ((OMMMsg)this.omm_encoder2.getEncodedObject());
GenericOMMParser.parse (cmd.getMsg());
			cmd.setHandle (stream_handle);
			final int command_id = this.omm_consumer.submit (cmd, closure);
			this.omm_pool.releaseMsg (msg);
			return command_id;
		}

		@Override
		public void processEvent (Event event) {
			LOG.trace (event);
			switch (event.getType()) {
			case Event.OMM_ITEM_EVENT:
				this.OnOMMItemEvent ((OMMItemEvent)event);
				break;

			case Event.TIMER_EVENT:
				this.OnTimerEvent (event);
				break;

			default:
				LOG.trace ("Uncaught: {}", event);
				break;
			}
		}

		private void OnOMMItemEvent (OMMItemEvent event) {
			LOG.trace ("OnOMMItemEvent: {}", event);
			final OMMMsg msg = event.getMsg();

			switch (msg.getMsgType()) {
			case OMMMsg.MsgType.REFRESH_RESP:
			case OMMMsg.MsgType.UPDATE_RESP:
			case OMMMsg.MsgType.STATUS_RESP:
			case OMMMsg.MsgType.ACK_RESP:
				this.OnRespMsg (msg, event.getHandle(), event.getClosure());
				break;

/* inside stream messages */
			case OMMMsg.MsgType.GENERIC:
				this.OnGenericMsg (msg, event.getHandle(), event.getClosure());
				break;
			
			default:
				LOG.trace ("Uncaught: {}", msg);
				break;
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
				this.OnAnalyticsStatus (this.closed_response_status, stream);
				stream.clearTimerHandle();
			} else if (stream.getRetryCount() >= retry_limit) {
				this.OnAnalyticsStatus (new ResponseStatus (OMMState.Stream.OPEN, OMMState.Data.SUSPECT, OMMState.Code.NONE, "Source did not respond."),
							stream);
/* prevent repeated invocation */
				stream.clearTimerHandle();
			} else {
				this.OnAnalyticsStatus (new ResponseStatus (OMMState.Stream.OPEN, OMMState.Data.SUSPECT, OMMState.Code.NONE, "Source did not respond.  Retrying."),
							stream);
				stream.incrementRetryCount();
				this.sendItemRequest (stream);
				this.registerRetryTimer (stream, retry_timer_ms);
			}
		}

		private void OnRespMsg (OMMMsg msg, Handle handle, Object closure) {
			LOG.trace ("OnRespMsg: {}", msg);
			switch (msg.getMsgModelType()) {
			case 30 /* RDMMsgTypes.ANALYTICS */:
				this.OnAppResponse (msg, handle, closure);
				break;

			default:
				LOG.trace ("Uncaught: {}", msg);
				break;
			}
		}

		private void OnGenericMsg (OMMMsg msg, Handle handle, Object closure) {
GenericOMMParser.parse (msg);
			LOG.trace ("OnGenericMsg: {}", msg);
/* Forward all MMT_ANALYTICS encapsulated messages */
			switch (msg.getMsgModelType()) {
			case 30 /* RDMMsgTypes.ANALYTICS */:
				if (msg.getDataType() == OMMTypes.MSG) {
					this.OnAnalyticsMsg ((OMMMsg)msg.getPayload(), handle, closure);
					break;
				}

			default:
				LOG.trace ("Uncaught: {}", msg);
				break;
			}
		}

		private void OnAnalyticsMsg (OMMMsg msg, Handle handle, Object closure) {
			LOG.trace ("OnAnalyticsMsg: {}", msg);

			switch (msg.getMsgType()) {
			case OMMMsg.MsgType.REFRESH_RESP:
			case OMMMsg.MsgType.UPDATE_RESP:
			case OMMMsg.MsgType.STATUS_RESP:
			case OMMMsg.MsgType.ACK_RESP:
				this.OnAnalyticsRespMsg (msg, handle, closure);
				break;

			default:
				LOG.trace ("Uncaught: {}", msg);
				break;
			}
		}

		private void OnAnalyticsRespMsg (OMMMsg msg, Handle handle, Object closure) {
			LOG.trace ("OnAnalyticsRespMsg: {}", msg);
			switch (msg.getMsgModelType()) {
			case 30 /* RDMMsgTypes.ANALYTICS */:
				this.OnAnalyticsResponse (msg, handle, closure);
				break;

			default:
				LOG.trace ("Uncaught: {}", msg);
				break;
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

		private void OnAnalyticsStatus (ResponseStatus response_status, AnalyticStream stream) {
/* Defer to GSON to escape status text. */
			LogMessage log_msg = new LogMessage (
				"STATUS",
				stream.getServiceName(),
				stream.getAppName(),
				stream.getItemName(),
				stream.getQuery(),
				OMMState.Stream.toString (response_status.getStreamState()),
				OMMState.Data.toString (response_status.getDataState()),
				OMMState.Code.toString (response_status.getCode()),
				response_status.getText());
			stream.getDispatcher().dispatch (stream, gson.toJson (log_msg));
			this.destroyItemStream (stream);
		}

/* example response:
 * MESSAGE
 *   Msg Type: MsgType.STATUS_RESP
 *   Msg Model Type: Unknown Msg Model Type: 30
 *   Indication Flags: PRIVATE_STREAM
 *   Hint Flags: HAS_STATE
 *   State: CLOSED, SUSPECT, ERROR,  " bidPrice: 97.42 bidSize: 400 bidtime: 2014-11-20T19:00:00.000Z  askPrice: 97.44 askSize: 100 asktime: 2014-11-20T19:00:00.000Z  tradePrice: 97.42 tradeSize: 52 tradetime: 2014-11-20T18:59:46.000Z "
 *   Payload: None
 */
		private final Pattern TECHANALYSIS_PATTERN = Pattern.compile ("(\\S+):\\s(\\S*)\\s");
		private boolean OnTechAnalysisResponse (OMMMsg msg, AnalyticStream stream) {
			LOG.trace ("OnTechAnalysisResponse: {}", msg);
			if (!(msg.has (OMMMsg.HAS_STATE)
				&& (OMMState.Stream.CLOSED == msg.getState().getStreamState())
				&& (OMMState.Data.SUSPECT == msg.getState().getDataState())
				&& (OMMState.Code.ERROR == msg.getState().getCode())))
			{
				return false;
			}

			final String text = msg.getState().getText();
			if (text.isEmpty()
				|| !(text.startsWith (" bidPrice: ")			/* taqfromdatetime */
				     || text.startsWith (" tradePrice: ")
				     || text.startsWith ("201")))			/* tradespreadperformance */
			{
				return false;
			}

			sb.setLength (0);
			sb.append ('{')
			  .append ("\"recordname\":\"").append (stream.getItemName()).append ('\"')
			  .append (", \"query\":\"").append (stream.getQuery()).append ('\"');
			if (text.startsWith ("201"))
			{
				sb.append (',')
				  .append ("\"timeseries\": [[");
				List<String> times = Lists.newLinkedList(), values = Lists.newLinkedList();
				final Splitter newline = Splitter.on ('\n').omitEmptyStrings(), comma = Splitter.on (',');
				for (String entry : newline.split (text)) {
					Iterator<String> it = comma.split (entry).iterator();
					times.add (it.next());
					values.add (it.next());
				}
				Joiner.on (",").appendTo (sb, Iterables.transform (times, new Function<String, String>() {
					public String apply (String arg0) {
						return "\"" + arg0 + "\"";
					}}));
				sb.append ("],[");
				Joiner.on (",").appendTo (sb, values.iterator());
				sb.append ("]]");
			}
			else
			{
				final Matcher matcher = TECHANALYSIS_PATTERN.matcher (text);
				while (matcher.find()) {
					final String name = matcher.group (1);
					sb.append (',')
					  .append ('\"').append (name).append ("\":");
					final String value = matcher.group (2);
					if (null == Floats.tryParse (value)) {
						sb.append ('\"').append (value).append ('\"');
					} else {
						sb.append (value);
					}
				}
			}
			sb.append ("}");
			stream.getDispatcher().dispatch (stream, sb.toString());
			this.destroyItemStream (stream);
			return true;
		}

		private void OnAnalyticsResponse (OMMMsg msg, Handle handle, Object closure) {
			LOG.trace ("OnAnalyticsResponse: {}", msg);
/* Closures do not work as expected, implement stream id map */
			final AnalyticStream stream = this.stream_map.get (msg.getStreamId());
			if (null == stream) {
				LOG.trace ("Ignoring response on stream id {} due to unregistered interest.", msg.getStreamId());
				return;
			}
/* Clear request timeout */
			if (stream.hasTimerHandle()) {
				final Handle timer_handle = stream.getTimerHandle();
				this.omm_consumer.unregisterClient (timer_handle);
				stream.clearTimerHandle();
				stream.clearRetryCount();
			}
			if (msg.isFinal()) {
				LOG.trace ("Command id for query \"{}\" on service/app \"{}/{}\" is closed.",
					stream.getQuery(), stream.getServiceName(), stream.getAppName());
				stream.clearCommandId();
			}
			if (OMMMsg.MsgType.REFRESH_RESP == msg.getMsgType()) {
/* fall through */
			}
			else if (OMMMsg.MsgType.UPDATE_RESP == msg.getMsgType()) {
				LOG.trace ("Ignoring update.");
				if (msg.isFinal()) {
					stream.getDispatcher().dispatch (stream, "internal error");
					this.destroyItemStream (stream);
				}
				return;
			}
			else if (OMMMsg.MsgType.STATUS_RESP == msg.getMsgType()) {
				LOG.trace ("Status: {}", msg);

/* Analytic stream recovered. */
				if (msg.has (OMMMsg.HAS_STATE)
					&& (OMMState.Stream.OPEN == msg.getState().getStreamState())
					&& (OMMState.Data.OK == msg.getState().getDataState()))
				{
					return;
				}

/* Hook for TechAnalysis responses */
				if (stream.getAppName().equals ("TechAnalysis")
					&& this.OnTechAnalysisResponse (msg, stream))
				{
					return;
				}

				this.OnAnalyticsStatus (new ResponseStatus (msg.getState()),
							stream);
				return;
			}
			else {
				LOG.trace ("Unhandled OMM message type ({}).", msg.getMsgType());
				if (msg.isFinal()) {
					stream.getDispatcher().dispatch (stream, "internal error");
					this.destroyItemStream (stream);
				}
				return;
			}

			if (OMMTypes.FIELD_LIST != msg.getDataType()) {
				LOG.trace ("Unsupported data type ({}) in OMM event.", msg.getDataType());
				if (msg.isFinal()) {
					stream.getDispatcher().dispatch (stream, "internal error");
					this.destroyItemStream (stream);
				}
				return;
			}

	                final OMMFieldList field_list = (OMMFieldList)msg.getPayload();

			if (LOG.isDebugEnabled()) {
				final Iterator<?> it = field_list.iterator();
				while (it.hasNext()) {
					final OMMFieldEntry field_entry = (OMMFieldEntry)it.next();
					final short fid = field_entry.getFieldId();
					final FidDef fid_def = rdm_dictionary.getFieldDictionary().getFidDef (fid);
					final OMMData data = field_entry.getData (fid_def.getOMMType());
					LOG.debug (new StringBuilder()
						.append (fid_def.getName())
						.append (": ")
						.append (data.isBlank() ? "null" : data.toString())
						.toString());
				}
			}

			sb.setLength (0);
			sb.append ('{')
			  .append ("\"recordname\":\"").append (stream.getItemName()).append ('\"')
			  .append (", \"query\":\"").append (stream.getQuery()).append ('\"');
			if (!field_list.isBlank()) {
				final Iterator<?> it = field_list.iterator();
				while (it.hasNext()) {
					final OMMFieldEntry field_entry = (OMMFieldEntry)it.next();
					final short fid = field_entry.getFieldId();
					final FidDef fid_def = rdm_dictionary.getFieldDictionary().getFidDef (fid);
					final OMMData data = field_entry.getData (fid_def.getOMMType());
					sb.append (',')
					  .append ('\"').append (fid_def.getName()).append ("\":");
					if (data.isBlank()) {
						sb.append ("null");
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
							sb.append (data.toString());
							break;
						default:
							sb.append ('\"').append (data.toString()).append ('\"');
							break;
                                                }
					}
				}
			}
			sb.append ("}");
			stream.getDispatcher().dispatch (stream, sb.toString());
			this.destroyItemStream (stream);
		}

		private void OnAppResponse (OMMMsg msg, Handle handle, Object closure) {
GenericOMMParser.parse (msg);
			final AppLoginResponse response = new AppLoginResponse (msg);
			final byte stream_state = response.getRespStatus().getStreamState();
			final byte data_state   = response.getRespStatus().getDataState();

			switch (stream_state) {
			case OMMState.Stream.OPEN:
				switch (data_state) {
				case OMMState.Data.OK:
					this.OnAppSuccess (response);
					break;

				case OMMState.Data.SUSPECT:
					this.OnAppSuspect (response);
					break;

				default:
					LOG.trace ("Uncaught data state: {}", response.getRespStatus());
					break;
				}
				break;

/* CLOSED is supposed to be a terminal status like something is not found or entitled.
 * CLOSED_RECOVER is a transient problem that the consumer should attempt recovery such as 
 * out of resources and thus unenable to store the request.
 */
			case OMMState.Stream.CLOSED:
			case OMMState.Stream.CLOSED_RECOVER:
				this.OnAppClosed (response);
				break;

			default:
				LOG.trace ("Uncaught stream state: {}", response.getRespStatus());
				break;
			}
		}

		private void OnAppSuccess (AppLoginResponse response) {
			LOG.trace ("OnAppSuccess: {}", response);
			this.clearPendingConnection();
			LOG.trace ("Resubmitting analytics.");
/* Renumber all managed stream ids */
			this.resetStreamId();
			this.resubmit();
		}

/* Transient problem, TREP will attempt to recover automatically */
		private void OnAppSuspect (AppLoginResponse response) {
			LOG.trace ("OnAppSuspect: {}", response);
		}

		private void OnAppClosed (AppLoginResponse response) {
			LOG.trace ("OnAppClosed: {}", response);
			this.setPendingConnection();
/* Invalidate all existing identifiers */
			for (AnalyticStream stream : this.streams) {
/* Prevent attempts to send a close request */
				stream.close();
/* Destroy for snapshots */
				this.OnAnalyticsStatus (response.getRespStatus(),
							stream);
/* Cleanup */
				this.removeItemStream (stream);
			}
/* Await timer to re-open private stream, cache close message until connected. */
			this.closed_response_status = response.getRespStatus();
			this.private_stream = null;
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
			return null != this.private_stream;
		}

		public void sendConnectionRequest() {
			this.createPrivateStream();
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

	private static final int OMM_PAYLOAD_SIZE	     	= 5000;
	private static final int GC_DELAY_MS			= 15000;
	private static final int RESUBSCRIPTION_MS		= 180000;
	private static final int DEFAULT_RETRY_TIMER_MS		= 60000;
	private static final int DEFAULT_RETRY_LIMIT		= 0;
	private static final int DEFAULT_STREAM_IDENTIFIER	= 1;

	private static final String RSSL_PROTOCOL      	 	= "rssl";

	public AnalyticConsumer (SessionConfig config, Rfa rfa, EventQueue event_queue) {
		this.config = config;
		this.rfa = rfa;
		this.event_queue = event_queue;
		this.apps = Maps.newLinkedHashMap();
		this.rwf_major_version = 0;
		this.rwf_minor_version = 0;
		this.is_muted = true;
		this.pending_directory = true;
		this.pending_dictionary = true;

		this.retry_timer_ms = this.config.hasRetryTimer() ?
						(1000 * Integer.valueOf (this.config.getRetryTimer()))
						: DEFAULT_RETRY_TIMER_MS;
		this.retry_limit = this.config.hasRetryLimit() ?
						Integer.valueOf (this.config.getRetryLimit())
						: DEFAULT_RETRY_LIMIT;
	}

	private class SubscriptionManager implements Client {
		private final AnalyticConsumer consumer;

		public SubscriptionManager (AnalyticConsumer consumer) {
			this.consumer = consumer;
		}

		@Override
		public void processEvent (Event event) {
			LOG.trace (event);
			switch (event.getType()) {
			case Event.TIMER_EVENT:
				this.OnTimerEvent (event);
				break;

			default:
				LOG.trace ("Uncaught: {}", event);
				break;
			}
		}

/* All requests are throttled per The Session Layer Package Configuration thus
 * no need to perform additional pacing at the application layer.  Default is
 * to permit 200 outstanding requests at a time.  See throttleEnabled, and
 * throttleType for interval based request batching.
 */
		private void OnTimerEvent (Event event) {
			LOG.trace ("Resubscription event ...");
			if (null != this.consumer) {
				this.consumer.resubscribe();
			}
		}
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

/* Registering for Events from an OMM Consumer. */
			LOG.trace ("Registering OMM error interest.");
			OMMErrorIntSpec ommErrorIntSpec = new OMMErrorIntSpec();
			this.error_handle = this.omm_consumer.registerClient (this.event_queue, ommErrorIntSpec, this, null);

/* OMM memory management. */
			this.omm_pool = OMMPool.create (OMMPool.SINGLE_THREADED);
			this.omm_encoder = this.omm_pool.acquireEncoder();
			this.omm_encoder2 = this.omm_pool.acquireEncoder();

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

/* Resubsription manager */
		if (RESUBSCRIPTION_MS > 0) {
			final TimerIntSpec timer = new TimerIntSpec();
			timer.setDelay (RESUBSCRIPTION_MS);
			timer.setRepeating (true);
			this.subscription_manager = new SubscriptionManager (this);
			this.resubscription_handle = this.omm_consumer.registerClient (this.event_queue, timer, this.subscription_manager, null);
		}
	}

	public void clear() {
		if (null != this.resubscription_handle) {
			this.resubscription_handle = null;
		}
		if (null != this.rdm_dictionary)
			this.rdm_dictionary = null;
		if (null != this.omm_encoder)
			this.omm_encoder = null;
		if (null != this.omm_encoder2)
			this.omm_encoder2 = null;
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
			} else {
				if (null != this.directory && !this.directory.isEmpty())
					this.directory.clear();
				if (null != this.dictionary_handle && !this.dictionary_handle.isEmpty())
					this.dictionary_handle.clear();
				if (null != this.directory_handle)
					this.directory_handle = null;
				if (null != this.login_handle)
					this.login_handle = null;
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
			this.sendItemRequest (item_stream);
		}
		this.directory.put (key, item_stream);
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
		this.cancelItemRequest (item_stream);
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
/* lazy app private stream creation */
		App app = this.apps.get (analytic.getApp());
		if (null == app) {
			app = new App (this.event_queue,
					this.omm_consumer,
					this.omm_pool,
					this.omm_encoder, this.omm_encoder2,
					this.login_handle,
					analytic.getService(),
					analytic.getApp(),
					this.config.hasUuid() ? this.config.getUuid() : "");
			this.apps.put (analytic.getApp(), app);
			app.sendConnectionRequest();
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

	public void resubscribe() {
		LOG.trace ("resubscribe");
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
		for (ItemStream item_stream : this.directory.values()) {
			if (!item_stream.hasItemHandle()) {
				this.sendItemRequest (item_stream);
			}
		}
	}

	private void sendItemRequest (ItemStream item_stream) {
		LOG.trace ("Sending market price request.");
		OMMMsg msg = this.omm_pool.acquireMsg();
		msg.setMsgType (OMMMsg.MsgType.REQUEST);
		msg.setMsgModelType (RDMMsgTypes.MARKET_PRICE);
		msg.setAssociatedMetaInfo (this.login_handle);
		msg.setIndicationFlags (OMMMsg.Indication.REFRESH);
		msg.setAttribInfo (item_stream.getServiceName(), item_stream.getItemName(), RDMInstrument.NameType.RIC);

		LOG.trace ("Registering OMM item interest for MMT_MARKET_PRICE/{}/{}", item_stream.getServiceName(), item_stream.getItemName());
		OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
		ommItemIntSpec.setMsg (msg);
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

		LOG.trace ("Registering OMM item interest for MMT_LOGIN");
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

		LOG.trace ("Registering OMM item interest for MMT_DIRECTORY");
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
		attribInfo.setServiceName (service_name);
		attribInfo.setDictionaryName (dictionary_name);

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
			this.OnMarketPrice (msg);
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
		LOG.trace ("Unmuting consumer.");
		this.is_muted = false;
		if (!this.pending_dictionary)
			this.resubscribe();
	}

/* Other Login States.
 */
	private void OnLoginSuspect (RDMLoginResponse response) {
		LOG.trace ("OnLoginSuspect: ResponseStatus: {}", response.getRespStatus());
		this.is_muted = true;
	}

/* Login Closed.
 */
	private void OnLoginClosed (RDMLoginResponse response) {
		LOG.trace ("OnLoginClosed: {}", response);
		this.is_muted = true;
	}

/* MMT_DIRECTORY domain.  Request RDM dictionaries, RWFFld and RWFEnum, from first available service.
 */
	private void OnDirectoryResponse (OMMMsg msg) {
		LOG.trace ("OnDirectoryResponse: {}", msg);
GenericOMMParser.parse (msg);

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
/* Local file override */
			if (!this.config.hasFieldDictionary()) {
				this.sendDictionaryRequest (dictionary_service, "RWFFld");
			} else {
				final FieldDictionary field_dictionary = this.rdm_dictionary.getFieldDictionary();
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
		}

		if (!this.dictionary_handle.containsKey ("RWFEnum")) {
			if (!this.config.hasEnumDictionary()) {
				this.sendDictionaryRequest (dictionary_service, "RWFEnum");
			} else {
				final FieldDictionary field_dictionary = this.rdm_dictionary.getFieldDictionary();
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
		}

		if (0 == this.dictionary_handle.size()) {
			GenericOMMParser.initializeDictionary (this.rdm_dictionary.getFieldDictionary());
			LOG.trace ("All dictionaries loaded, resuming subscriptions.");
			this.pending_dictionary = false;
			this.resubscribe();
		}

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
			GenericOMMParser.initializeDictionary (field_dictionary);
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

// RFA 7.5.1
	private void OnConnectionEvent (OMMConnectionEvent event) {
		LOG.trace ("OnConnectionEvent: {}", event);
		LOG.info ("Connection status {} for {}@{}:{}",
				event.getConnectionStatus().toString(),
				event.getConnectedComponentVersion(), event.getConnectedHostName(), event.getConnectedPort());
	}

/* MMT_MARKETPRICE domain.
 */
	private void OnMarketPrice (OMMMsg msg) {
GenericOMMParser.parse (msg);
	}

}

/* eof */
