/* Takoyaki Snapshot Gateway.
 */

package com.thomsonreuters.Takoyaki;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.zeromq.ZMQ;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedInteger;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import com.reuters.rfa.common.Context;
import com.reuters.rfa.common.DeactivatedException;
import com.reuters.rfa.common.Dispatchable;
import com.reuters.rfa.common.DispatchException;
import com.reuters.rfa.common.DispatchableNotificationClient;
import com.reuters.rfa.common.EventQueue;
import com.reuters.rfa.common.Handle;
import com.reuters.rfa.config.ConfigDb;
import com.reuters.rfa.session.Session;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class Takoyaki implements AnalyticStreamDispatcher {

/* Application configuration. */
	private Config config;

/* RFA context. */
	private Rfa rfa;

/* RFA asynchronous event queue. */
	private EventQueue event_queue;

/* RFA consumer */
	private AnalyticConsumer consumer;

/* ZeroMQ context */
	private ZMQ.Context zmq_context;
	private ZMQ.Socket abort_sock;
	private ZMQ.Socket dispatcher;

/* HTTP server */
 	private HttpServer http_server;
	private MyHandler http_handler;
	private HttpContext http_context;

	private static Logger LOG = LogManager.getLogger (Takoyaki.class.getName());
	private static Logger RFA_LOG = LogManager.getLogger ("com.reuters.rfa");

	private static final String RSSL_PROTOCOL		= "rssl";
	private static final String SSLED_PROTOCOL		= "ssled";

	private static final String SERVER_LIST_PARAM		= "server-list";
	private static final String APPLICATION_ID_PARAM	= "application-id";
	private static final String INSTANCE_ID_PARAM		= "instance-id";
	private static final String POSITION_PARAM		= "position";
	private static final String DICTIONARY_PARAM		= "dictionary";
	private static final String RETRY_TIMER_PARAM		= "retry-timer";
	private static final String RETRY_LIMIT_PARAM		= "retry-limit";
	private static final String UUID_PARAM			= "uuid";

	private static final String SIGNAL_PARAM		= "signal";
	private static final String TECHANALYSIS_PARAM		= "analytic";
	private static final String DATETIME_PARAM		= "datetime";
	private static final String TIME_INTERVAL_PARAM		= "interval";
	private static final String SNAPBY_PARAM		= "snapby";
	private static final String LAGTYPE_PARAM		= "lagtype";
	private static final String LAG_PARAM			= "lag";

	private static final String SESSION_OPTION		= "session";
	private static final String HELP_OPTION			= "help";
	private static final String VERSION_OPTION		= "version";

	private static final String SESSION_NAME		= "Session";
	private static final String CONNECTION_NAME		= "Connection";
	private static final String CONSUMER_NAME		= "Consumer";

	private static final String[] DEFAULT_FIELDS = { "OPEN_PRC", "HIGH_1", "LOW_1", "HST_CLOSE", "ACVOL_1", "NUM_MOVES" };

	private static Options buildOptions() {
		Options opts = new Options();

		Option help = OptionBuilder.withLongOpt (HELP_OPTION)
					.withDescription ("print this message")
					.create ("h");
		opts.addOption (help);

		Option version = OptionBuilder.withLongOpt (VERSION_OPTION)
					.withDescription ("print version information and exit")
					.create();
		opts.addOption (version);

		Option session = OptionBuilder.hasArg()
					.isRequired()
					.withArgName ("uri")
					.withDescription ("TREP-RT session declaration")
					.withLongOpt (SESSION_OPTION)
					.create();
		opts.addOption (session);

		return opts;
	}

	private static void printHelp (Options options) {
		new HelpFormatter().printHelp ("Takoyaki", options);
	}

	private static List<String> splitHeaderValues (List<String> values) {
		if (values == null) {
			return null;
		}
		List<String> parsed = new ArrayList<String> (values.size());
		for (String value : values) {
			String[] parts = value.split (",", -1);
			for (String part : parts) {
				parsed.add (part.trim());
			}
		}
		return Collections.unmodifiableList (parsed);
	}

	private static void enableCompressionIfSupported (HttpExchange ex) throws IOException {
		Collection<String> encodings = splitHeaderValues (ex.getRequestHeaders().get ("Accept-Encoding"));
		if (encodings == null) {
			return;
		}
		if (encodings.contains ("gzip")) {
			ex.getResponseHeaders().set ("Content-Encoding", "gzip");
			final OutputStream os = ex.getResponseBody();
			ex.setStreams (null, new AbstractLazyOutputStream() {
				@Override
				protected OutputStream retrieveOs() throws IOException {
					return new GZIPOutputStream (os);
				}
			});
		}
	}

	private static Map<String, List<String>> parseQueryParameters (String queryString, Charset charset) {
		if (queryString == null || queryString.isEmpty()) {
			return Collections.emptyMap();
		}
		Map<String, List<String>> parsedParams = new TreeMap<String, List<String>>();
		for (String param : queryString.split ("&")) {
			String[] parts = param.split ("=", 2);
			String key = parts[0];
			String value = parts.length == 2 ? parts[1] : "";
			try {
				key = URLDecoder.decode (key, charset.name());
				value = URLDecoder.decode (value, charset.name());
			} catch (UnsupportedEncodingException e) {
				throw new AssertionError (e);
			}
			List<String> values = parsedParams.get (key);
			if (values == null) {
				values = new LinkedList<String>();
				parsedParams.put (key, values);
			}
			values.add (value);
		}

		for (Map.Entry<String, List<String>> me : parsedParams.entrySet()) {
			me.setValue (Collections.unmodifiableList (me.getValue()));
		}
		return Collections.unmodifiableMap (parsedParams);
	}

	private static String getParameterValue (Map<String, List<String>> queryParameters, String name) {
		List<String> values = getParameterValues (queryParameters, name);
		return values == null ? null : values.get (0);
	}

	private static List<String> getParameterValues (Map<String, List<String>> queryParameters, String name) {
		if (queryParameters == null) {
			throw new UnsupportedOperationException ("Parameter decoding only supported for GET requests");
		}
		List<String> values = queryParameters.get (name);
		return values == null ? null : Collections.unmodifiableList (values);
	}

	private void init (CommandLine line, Options options) throws Exception {
		if (line.hasOption (HELP_OPTION)) {
			printHelp (options);
			return;
		}

/* Configuration. */
		this.config = new Config();

		if (line.hasOption (SESSION_OPTION)) {
			final String session = line.getOptionValue (SESSION_OPTION);
			List<SessionConfig> session_configs = new ArrayList<SessionConfig>();
			if (!Strings.isNullOrEmpty (session)) {
				LOG.debug ("Session declaration: {}", session);
				final URI parsed = new URI (session);
/* For each key-value pair, i.e. ?a=x&b=y&c=z -> (a,x) (b,y) (c,z) */
				final Map<String, List<String>> query = parseQueryParameters (parsed.getQuery(), Charset.forName ("UTF-8"));

/* Extract out required parameters */
				final String protocol = parsed.getScheme();
				final String server_list = getParameterValue (query, SERVER_LIST_PARAM);
				String[] servers = { parsed.getHost() };
/* Override host in URL with server-list query parameter */
				if (!Strings.isNullOrEmpty (server_list)) {
					servers = Iterables.toArray (Splitter.on (',')
							.trimResults()
							.omitEmptyStrings()
							.split (server_list), String.class);
				}

/* Minimum parameters to construct session configuration */
				SessionConfig session_config = new SessionConfig (SESSION_NAME, CONNECTION_NAME, CONSUMER_NAME, protocol, servers);

/* Optional session parameters: */
				if (!Strings.isNullOrEmpty (parsed.getUserInfo()))
					session_config.setUserName (parsed.getUserInfo());
/* -1 if the port is undefined */
				if (-1 != parsed.getPort()) 
					session_config.setDefaultPort (Integer.toString (parsed.getPort()));
/* Catch default URL of host/ as empty */
				if (!Strings.isNullOrEmpty (parsed.getPath())
					&& parsed.getPath().length() > 1)
				{
					session_config.setServiceName (new File (parsed.getPath()).getName());
				}
				if (query.containsKey (APPLICATION_ID_PARAM))
					session_config.setApplicationId (getParameterValue (query, APPLICATION_ID_PARAM));
				if (query.containsKey (INSTANCE_ID_PARAM))
					session_config.setInstanceId (getParameterValue (query, INSTANCE_ID_PARAM));
				if (query.containsKey (POSITION_PARAM))
					session_config.setPosition (getParameterValue (query, POSITION_PARAM));
				if (query.containsKey (DICTIONARY_PARAM)) {
					Iterable<String> iterable = Splitter.on (',')
									.trimResults()
									.limit (2)
									.split (getParameterValue (query, DICTIONARY_PARAM));
					Iterator<String> it = iterable.iterator();
					if (it.hasNext())
						session_config.setFieldDictionary (it.next());
					if (it.hasNext())
						session_config.setEnumDictionary (it.next());
				}
				if (query.containsKey (RETRY_TIMER_PARAM))
					session_config.setRetryTimer (getParameterValue (query, RETRY_TIMER_PARAM));
				if (query.containsKey (RETRY_LIMIT_PARAM))
					session_config.setRetryLimit (getParameterValue (query, RETRY_LIMIT_PARAM));
				if (query.containsKey (UUID_PARAM))
					session_config.setUuid (getParameterValue (query, UUID_PARAM));

				LOG.debug ("Session evaluation: {}", session_config.toString());
				session_configs.add (session_config);
			}
			if (!session_configs.isEmpty()) {
				final SessionConfig[] array = session_configs.toArray (new SessionConfig[session_configs.size()]);
				this.config.setSessions (array);
			}
		}

		LOG.debug (this.config.toString());

/* ZeroMQ Context. */
		this.zmq_context = ZMQ.context (1);
		this.abort_sock = this.zmq_context.socket (ZMQ.DEALER);
		this.dispatcher = this.zmq_context.socket (ZMQ.ROUTER);
		this.dispatcher.bind ("inproc://rfa");
		this.abort_sock.connect ("inproc://rfa");

/* RFA Logging. */
// Remove existing handlers attached to j.u.l root logger
		SLF4JBridgeHandler.removeHandlersForRootLogger();
// add SLF4JBridgeHandler to j.u.l's root logger
		SLF4JBridgeHandler.install();

		if (RFA_LOG.isDebugEnabled()) {
			java.util.logging.Logger rfa_logger = java.util.logging.Logger.getLogger ("com.reuters.rfa");
			rfa_logger.setLevel (java.util.logging.Level.FINE);
		}

/* RFA Context. */
		this.rfa = new Rfa (this.config);
		this.rfa.init();

/* RFA asynchronous event queue. */
		this.event_queue = EventQueue.create (this.config.getEventQueueName());

/* RFA consumer */
		this.consumer = new AnalyticConsumer (this.config.getSession(),
					this.rfa,
					this.event_queue);
		this.consumer.init();

/* HTTP server */
		this.http_server = HttpServer.create (new InetSocketAddress (8000), 0);
		this.http_handler = new MyHandler (this.zmq_context);
		this.http_context = this.http_server.createContext ("/", this.http_handler);
		this.http_server.setExecutor (java.util.concurrent.Executors.newSingleThreadExecutor());
	}

	private class MyHandler implements HttpHandler {
		private ZMQ.Socket sock;

		public MyHandler (ZMQ.Context zmq_context) {
			this.sock = zmq_context.socket (ZMQ.DEALER);
			Random rand = new Random (System.currentTimeMillis());
			String identity = String.format ("%04X-%04X", rand.nextInt(), rand.nextInt());
			this.sock.setIdentity (identity.getBytes());
			this.sock.connect ("inproc://rfa");
		}

		public void reset() {
			if (null != this.sock) {
				this.sock.close();
				this.sock = null;
			}
		}

		@Override
		public void handle (HttpExchange exchange) throws IOException {
			if (exchange.getRequestMethod().equals("GET")) {
				final URI request = exchange.getRequestURI();
				final String path = request.getPath();
				if (path.equalsIgnoreCase ("/favicon.ico")) {
					exchange.getResponseHeaders().set ("Cache-Control", "public, max-age=691200");
					exchange.sendResponseHeaders (HttpURLConnection.HTTP_NOT_FOUND, 0);
				} else {
					this.sock.sendMore ("");
					this.sock.sendMore ("http");
					this.sock.send (request.toASCIIString());
					this.sock.recvStr();	//  Envelope delimiter
					final String response = this.sock.recvStr();
					enableCompressionIfSupported (exchange);
					exchange.getResponseHeaders().set ("Content-Type", "application/json");
					exchange.sendResponseHeaders (HttpURLConnection.HTTP_OK, 0);
					final OutputStream os = exchange.getResponseBody();
					os.write (response.getBytes());
					os.flush();
					os.close();
				}
			} else {
				exchange.sendResponseHeaders (HttpURLConnection.HTTP_BAD_METHOD, 0);
			}
			exchange.close();
		}
	}

/* LOG4J2 logging is terminated by an installed shutdown hook.  This hook can
 * disabled by adding shutdownHook="disable" to the <Configuration> stanza.
 */
	private class ShutdownThread extends Thread {
		private Takoyaki app;
		private org.apache.logging.log4j.core.LoggerContext context;
		public ShutdownThread (Takoyaki app) {
			this.app = app;
/* Capture on startup as we cannot capture on shutdown as it would try to reinit:
 *   WARN Unable to register shutdown hook due to JVM state
 */
			this.context = (org.apache.logging.log4j.core.LoggerContext)LogManager.getContext();
		}
		@Override
		public void run() {
			if (null != this.app
				&& null != this.app.event_queue
				&& this.app.event_queue.isActive())
			{
				LOG.trace ("Deactivating event queue ...");
				this.app.event_queue.deactivate();
				LOG.trace ("Notifying mainloop ... ");
				this.app.abort_sock.sendMore ("");
				this.app.abort_sock.send ("abort");
				try {
					LOG.trace ("Waiting for mainloop shutdown ...");
					while (!this.app.is_shutdown) {
						Thread.sleep (100);
					}
					LOG.trace ("Shutdown complete.");
				} catch (InterruptedException e) {}
			}
/* LOG4J2-318 to manually shutdown.
 */
			if (context.isStarted()
				&& !context.getConfiguration().isShutdownHookEnabled())
			{
				LOG.trace ("Shutdown log4j2.");
				context.stop();
			}
		}
	}

	private void run (CommandLine line, Options options) throws Exception {
		this.init (line, options);
		Thread shutdown_hook = new ShutdownThread (this);
		Runtime.getRuntime().addShutdownHook (shutdown_hook);
		LOG.trace ("Shutdown hook installed.");
		this.mainloop();
		LOG.trace ("Shutdown in progress.");
/* Cannot remove hook if shutdown is in progress. */
//		Runtime.getRuntime().removeShutdownHook (shutdown_hook);
//		LOG.trace ("Removed shutdown hook.");
		this.clear();
		this.is_shutdown = true;
	}

	public volatile boolean is_shutdown = false;

	private class RfaDispatcher implements DispatchableNotificationClient {
		private ZMQ.Socket sock;

		public RfaDispatcher (ZMQ.Context zmq_context) {
			this.sock = zmq_context.socket (ZMQ.DEALER);
			this.sock.connect ("inproc://rfa");
		}

		public void reset() {
			if (null != this.sock) {
				this.sock.close();
				this.sock = null;
			}
		}

		@Override
		public void notify (Dispatchable dispSource, java.lang.Object closure) {
			this.sock.sendMore ("");
			this.sock.send ("");
		}
	}

	private void drainqueue() {
		LOG.trace ("Draining event queue.");
		int count = 0;
		try {
			while (this.event_queue.dispatch (Dispatchable.NO_WAIT) > 0) { ++count; }
			LOG.trace ("Queue contained {} events.", count);
		} catch (DeactivatedException e) {
/* ignore on empty queue */
			if (count > 0) LOG.catching (e);
		} catch (Exception e) {
			LOG.catching (e);
		}
	}

	private void mainloop() {
		RfaDispatcher dispatcher = new RfaDispatcher (this.zmq_context);
		this.event_queue.registerNotificationClient (dispatcher, null);
		try {
/* drain queue of pending events before client registration */
			this.drainqueue();
			this.http_server.start();
			LOG.info ("Listening on http://{}/", this.http_server.getAddress());
/* on demand edge triggered dispatch */
			while (this.event_queue.isActive()) {
				LOG.trace ("Waiting ...");
				final String identity = this.dispatcher.recvStr();
				this.dispatcher.recv (0);		// envelope delimiter
				String msg = this.dispatcher.recvStr();	// response
				LOG.trace ("recv: {}", msg);
				switch (msg) {
				case "http":
					this.dispatcher.sendMore (identity);
					this.dispatcher.sendMore ("");
					try {
						final URI request = new URI (this.dispatcher.recvStr());
						this.handler (request);
					} catch (Exception e) {
						this.dispatcher.send (Throwables.getStackTraceAsString (e));
					}
					break;
				default:
					if (this.event_queue.isActive())
						this.event_queue.dispatch (Dispatchable.NO_WAIT);
					break;
				}
			}
		} catch (DispatchException e) {
			LOG.error ("DispatchException: {}", Throwables.getStackTraceAsString (e));
		} catch (Throwable t) {
			LOG.catching (t);
		} finally {
			this.http_server.stop (0 /* seconds */);
			if (!this.event_queue.isActive()) this.event_queue.deactivate();
			this.drainqueue();
		}
		LOG.trace ("Mainloop deactivated.");
		this.event_queue.unregisterNotificationClient (dispatcher);
		dispatcher.reset();
	}

	final static Duration ONE_SECOND = Duration.standardSeconds (1);
	final static Duration ONE_DAY = Duration.standardDays (1);
	final DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser();

	private class Multipass implements AnalyticStreamDispatcher {
		private final ImmutableSet<AnalyticStream> requests;
		private final ZMQ.Socket dispatcher;
		private final Map<String, String> responses;

		public Multipass (ImmutableSet<AnalyticStream> requests, ZMQ.Socket dispatcher) {
			this.requests = requests;
			this.dispatcher = dispatcher;
			this.responses = Maps.newTreeMap();
		}

/* Format the final HTTP result, adjust per special snowflake requirements. */
		@Override
		public void dispatch (AnalyticStream stream, String stream_response) {
			this.responses.put (stream.getItemName(), stream_response);
			if (this.responses.size() != this.requests.size()) {
/* pending complete result set */
				return;
			}
			else if (1 == this.requests.size()) {
				this.dispatcher.send (stream_response);
			}
			else {
				final StringBuilder sb = new StringBuilder();
				sb.append ("[");
				final Joiner joiner = Joiner.on (",\n");
				joiner.appendTo (sb, this.responses.values());
				sb.append ("]");
				this.dispatcher.send (sb.toString());
			}
		}
	}

	@Override
	public void dispatch (AnalyticStream stream, String response) {
		this.multipass.dispatch (stream, response);
	}

	public Multipass multipass;

// http://takoyaki/MSFT.O?signal=MMA(21,Close())
// http://takoyaki/MSFT.O,GOOG.O?signal=MMA(21,Close())
// http://takoyaki/SBUX.O?analytic=taqfromdatetime&datetime=2014-11-20T19:00:00.000Z
// --> #type=taqfromdatetime datetime=2014-11-20T19:00:00.000Z
// http://takoyaki/SBUX.O?analytic=tradeperformancespread&interval=2014-11-24T17:05:15.444Z/PT2M&returnformat=perunit
// --> #type=tradeperformancespread startdatetime=2014-11-24T17:05:15.444Z enddatetime=2014-11-24T17:07:15.444Z returnmode=historical returnformat=perunit

// supported parameters:
//   snapby  - [ time | trade | quote ]
//   lagtype - [ trade | quote | second | minute | hour | volume ]
//   lag     - <integer>
//   returnformat - [ perunit | decimal | percent | basispoints ]

	private void handler (URI request) {
		LOG.info ("GET: {}", request.toASCIIString());
		final Map<String, List<String>> query = parseQueryParameters (request.getQuery(), Charset.forName ("UTF-8"));
		Optional<String> signal = Optional.absent();
		Optional<String> techanalysis = Optional.absent(), datetime = Optional.absent(), snapby = Optional.absent(), lagtype = Optional.absent(), lag = Optional.absent();
		String[] items = {};
/* Validate each parameter */
		if (query.containsKey (SIGNAL_PARAM)) {
			signal = Optional.of (getParameterValue (query, SIGNAL_PARAM));
		}
		if (query.containsKey (TECHANALYSIS_PARAM)) {
			techanalysis = Optional.of (getParameterValue (query, TECHANALYSIS_PARAM));
			if (query.containsKey (DATETIME_PARAM)) {
				datetime = Optional.of (getParameterValue (query, DATETIME_PARAM));
			}
			if (query.containsKey (SNAPBY_PARAM)) {
				snapby = Optional.of (getParameterValue (query, SNAPBY_PARAM));
			}
			if (query.containsKey (LAG_PARAM)) {
				lag = Optional.of (getParameterValue (query, LAG_PARAM));
				if (query.containsKey (LAGTYPE_PARAM)) {
					lagtype = Optional.of (getParameterValue (query, LAGTYPE_PARAM));
				}
			}
		}
		if (!Strings.isNullOrEmpty (request.getPath())
			&& request.getPath().length() > 1)
		{
			items = Iterables.toArray (Splitter.on (',')
					.trimResults()
					.omitEmptyStrings()
					.split (new File (request.getPath()).getName()), String.class);
		}
		if ((!signal.isPresent() && !techanalysis.isPresent())
			|| (0 == items.length))
		{
			this.dispatcher.send ("invalid request");
			return;
		}
/* Build up batch request */
		final Analytic[] analytics = new Analytic[ items.length ];
		final AnalyticStream[] streams = new AnalyticStream[ items.length ];
		if (signal.isPresent())
		{
			LOG.trace ("signal: {}", signal.get());
			for (int i = 0; i < streams.length; ++i) {
				LOG.trace ("item[{}]: {}", i, items[i]);
				analytics[i] = new Analytic ("ECP_SAP",
							"SignalApp",
							signal.get(),
							items[i]);
				streams[i] = new AnalyticStream (this);
			}
		}
		else
		{
			StringBuilder sb = new StringBuilder();
			sb.append ("#type=")
			  .append (techanalysis.get());
			if (datetime.isPresent()) {
				sb.append (" datetime=")
				  .append (datetime.get());
			}
			if (snapby.isPresent()) {
				sb.append (" snapby=")
				  .append (snapby.get());
			}
			if (lag.isPresent()) {
				if (lagtype.isPresent()) {
// override content
					if (lagtype.get().equals ("duration")) {
						final Duration duration = Period.parse (lag.get()).toStandardDuration();
						lagtype = Optional.of ("second");
						lag = Optional.of (Long.toString (duration.getStandardSeconds()));
					}
					sb.append (" lagtype=")
					  .append (lagtype.get());
				}
				sb.append (" lag=")
				  .append (lag.get());
			}
			final String analytic = sb.toString();
			LOG.trace ("techanalysis: {}", analytic);
			for (int i = 0; i < streams.length; ++i) {
				LOG.trace ("item[{}]: {}", i, items[i]);
				analytics[i] = new Analytic ("ECP_SAP",
							"TechAnalysis",
							analytic,
							items[i]);
				streams[i] = new AnalyticStream (this);
			}
		}
		this.consumer.batchCreateAnalyticStream (analytics, streams);
		this.multipass = new Multipass (ImmutableSet.copyOf (streams), this.dispatcher);
	}

	private void clear() {
		if (null != this.http_context) {
			this.http_server.removeContext (this.http_context);
			this.http_context = null;
		}
		if (null != this.http_handler) {
			this.http_handler.reset();
			this.http_handler = null;
		}
		if (null != this.http_server) {
			this.http_server = null;
		}

/* Prevent new events being generated whilst shutting down. */
		if (null != this.event_queue && this.event_queue.isActive()) {
			LOG.trace ("Deactivating EventQueue.");
			this.event_queue.deactivate();
/* notify mainloop */
			this.abort_sock.sendMore ("");
			this.abort_sock.send ("abort");
			this.drainqueue();
		}

		if (null != this.consumer) {
			LOG.trace ("Closing Consumer.");
			this.consumer.clear();
			this.consumer = null;
		}

		if (null != this.event_queue) {
			LOG.trace ("Closing EventQueue.");
			this.event_queue.destroy();
			this.event_queue = null;
		}

		if (null != this.rfa) {
			LOG.trace ("Closing RFA.");
			this.rfa.clear();
			this.rfa = null;
		}

		if (null != this.abort_sock) {
			LOG.trace ("Closing ZeroMQ abort socket.");
			this.abort_sock.close();
			this.abort_sock = null;
		}

		if (null != this.dispatcher) {
			LOG.trace ("Closing ZeroMQ dispatcher socket.");
			this.dispatcher.close();
			this.dispatcher = null;
		}

		if (null != this.zmq_context) {
			LOG.trace ("Closing ZeroMQ context.");
			this.zmq_context.term();
			this.zmq_context = null;
		}
	}

	public static void main (String[] args) throws Exception {
		final Options options = Takoyaki.buildOptions();
		final CommandLine line = new PosixParser().parse (options, args);
		Takoyaki app = new Takoyaki();
		app.run (line, options);
	}
}

/* eof */
