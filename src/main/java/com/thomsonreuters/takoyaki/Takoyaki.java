/* Takoyaki Snapshot Gateway.
 */

package com.thomsonreuters.Takoyaki;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.nio.channels.SelectableChannel;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.time.*;
import java.time.format.*;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.joda.time.Interval;
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
import com.google.common.io.CharStreams;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.UnsignedInteger;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class Takoyaki implements AnalyticStreamDispatcher, AnalyticConsumer.Delegate {

/* Application configuration. */
	private Config config;

/* UPA context. */
	private Upa upa;

/* RFA consumer */
	private AnalyticConsumer analytic_consumer;

/* ZeroMQ context */
	private ZMQ.Context zmq_context;
	private ZMQ.Socket abort_sock;
	private ZMQ.Socket dispatcher;

/* HTTP server */
 	private HttpServer http_server;
	private MyHandler http_handler;
	private HttpContext http_context;

	private static Logger LOG = LogManager.getLogger (Takoyaki.class.getName());

	private static final String RSSL_PROTOCOL		= "rssl";

	private static final String SERVER_LIST_PARAM		= "server-list";
	private static final String APPLICATION_ID_PARAM	= "application-id";
	private static final String INSTANCE_ID_PARAM		= "instance-id";
	private static final String POSITION_PARAM		= "position";
	private static final String DICTIONARY_PARAM		= "dictionary";
	private static final String RETRY_TIMER_PARAM		= "retry-timer";
	private static final String RETRY_LIMIT_PARAM		= "retry-limit";
	private static final String UUID_PARAM			= "uuid";
	private static final String PASSWORD_PARAM		= "password";

	private static final String SIGNAL_PARAM		= "signal";
	private static final String DATETIME_PARAM		= "datetime";
	private static final String TIMEINTERVAL_PARAM		= "interval";
	private static final String SNAPBY_PARAM		= "snapby";
	private static final String LAGTYPE_PARAM		= "lagtype";
	private static final String LAG_PARAM			= "lag";
	private static final String RETURNFORMAT_PARAM		= "returnformat";

	private static final String LISTEN_OPTION		= "listen";
	private static final String SESSION_OPTION		= "session";
	private static final String HELP_OPTION			= "help";
	private static final String VERSION_OPTION		= "version";

	private static final String SESSION_NAME		= "Session";
	private static final String CONNECTION_NAME		= "Connection";
	private static final String CONSUMER_NAME		= "Consumer";

	private static final String[] DEFAULT_FIELDS = { "OPEN_PRC", "HIGH_1", "LOW_1", "HST_CLOSE", "ACVOL_1", "NUM_MOVES" };
	private static final int DEFAULT_PORT			= 8000;

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

		Option listen = OptionBuilder.hasArg()
					.withArgName ("hostname")
					.withDescription ("HTTP hostname")
					.withLongOpt (LISTEN_OPTION)
					.create();
		opts.addOption (listen);

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

	private boolean Initialize (CommandLine line, Options options) throws Exception {
		if (line.hasOption (HELP_OPTION)) {
			printHelp (options);
			return true;
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
				if (query.containsKey (PASSWORD_PARAM))
					session_config.setPassword (getParameterValue (query, PASSWORD_PARAM));

				LOG.debug ("Session evaluation: {}", session_config.toString());
				session_configs.add (session_config);
			}
			if (!session_configs.isEmpty()) {
				final SessionConfig[] array = session_configs.toArray (new SessionConfig[session_configs.size()]);
				this.config.setSessions (array);
			}
		}

		if (line.hasOption (LISTEN_OPTION)) {
			final HostAndPort hp = HostAndPort.fromString (line.getOptionValue (LISTEN_OPTION)).withDefaultPort (DEFAULT_PORT);
			this.config.setHostAndPort (hp);
		} else {
			final HostAndPort hp = HostAndPort.fromParts (InetAddress.getLocalHost().getHostName(), DEFAULT_PORT);
			this.config.setHostAndPort (hp);
		}

		LOG.debug (this.config.toString());

/* ZeroMQ Context. */
		this.zmq_context = ZMQ.context (1);
		this.abort_sock = this.zmq_context.socket (ZMQ.DEALER);
		this.dispatcher = this.zmq_context.socket (ZMQ.ROUTER);
		this.dispatcher.bind ("inproc://upa");
		this.abort_sock.connect ("inproc://upa");

		final SelectableChannel request_channel = this.dispatcher.getFD();

/* UPA Context. */
		this.upa = new Upa (this.config);
		if (!this.upa.Initialize()) {
			return false;
		}

/* UPA consumer */
		this.analytic_consumer = new AnalyticConsumer (this.config.getSession(), this.upa, this, request_channel);
		if (!this.analytic_consumer.Initialize()) {
			return false;
		}

/* HTTP server */
		this.http_server = HttpServer.create (new InetSocketAddress (this.config.getHostAndPort().getPort()), 0);
		this.http_handler = new MyHandler (this.zmq_context);
		this.http_context = this.http_server.createContext ("/", this.http_handler);

		this.multipass = Maps.newHashMap();

/* Single thread useful for debugging */
		this.http_server.setExecutor (java.util.concurrent.Executors.newSingleThreadExecutor());
/* Default sensible multi-threaded option */
//		this.http_server.setExecutor (java.util.concurrent.Executors.newCachedThreadPool());

		return true;
	}

/* dealer socket ready, pop request and start RSSL request mechanism */
	@Override
	public boolean OnRead() {
		LOG.trace ("OnRead");
		int zmq_events = this.dispatcher.getEvents();
		if (0 == (zmq_events & ZMQ.Poller.POLLIN))
			return false;
		final String identity = this.dispatcher.recvStr();
		LOG.trace ("dispatch from \"{}\"", identity);
		this.dispatcher.recv (0);		// envelope delimiter
		try {
			final URI request = new URI (this.dispatcher.recvStr());
			this.handler (request, identity);
		} catch (Exception e) {
			LOG.trace ("500 Internal Error.");
			this.dispatcher.sendMore (identity);
			this.dispatcher.sendMore ("");
			this.dispatcher.sendMore (Integer.toString (HttpURLConnection.HTTP_INTERNAL_ERROR));
			this.dispatcher.send (Throwables.getStackTraceAsString (e));
		}
/* re-read events due to edge triggering */
		zmq_events = this.dispatcher.getEvents();
		if (0 != (zmq_events & ZMQ.Poller.POLLIN))
			return true;
		return false;
	}

	private class MyHandler implements HttpHandler {
		private ZMQ.Context context;
		private Random rand;

		public MyHandler (ZMQ.Context zmq_context) {
			LOG.trace ("New HttpHandler context.");
			this.context = zmq_context;
			this.rand = new Random (System.currentTimeMillis());
		}

		public void reset() {
			this.context = null;
		}

		@Override
		public void handle (HttpExchange exchange) throws IOException {
			exchange.getResponseHeaders().set ("Access-Control-Allow-Origin", "*");
			exchange.getResponseHeaders().set ("Access-Control-Allow-Headers", "Content-Type");
			exchange.getResponseHeaders().set ("Access-Control-Allow-Methods", "GET");
			if (exchange.getRequestMethod().equals("GET")) {
				final URI request = exchange.getRequestURI();
				final String path = request.getPath();
				if (path.equalsIgnoreCase ("/favicon.ico")) {
					LOG.trace ("404 Not Found.");
					exchange.getResponseHeaders().set ("Cache-Control", "public, max-age=691200");
					exchange.sendResponseHeaders (HttpURLConnection.HTTP_NOT_FOUND, 0);
				} else if (path.equalsIgnoreCase ("/api-docs")) {
					enableCompressionIfSupported (exchange);
					exchange.getResponseHeaders().set ("Cache-Control", "public, max-age=691200");
					exchange.getResponseHeaders().set ("Content-Type", "application/json");
					exchange.sendResponseHeaders (HttpURLConnection.HTTP_OK, 0);
					final InputStream in = getClass().getResourceAsStream ("/swagger.json");
					final OutputStream os = exchange.getResponseBody();
					final String template = CharStreams.toString (new BufferedReader (new InputStreamReader (in)));
					in.close();
					os.write (template.replace ("${HOST}", config.getHostAndPort().toString()).getBytes());
					os.flush();
					os.close();
				} else {
					final ZMQ.Socket sock = this.context.socket (ZMQ.REQ);
					final String identity = String.format ("%04X-%04X", this.rand.nextInt(), this.rand.nextInt());
					try {
						sock.setIdentity (identity.getBytes());
						sock.connect ("inproc://upa");
LOG.trace ("{}: send http/{}", identity, request.toASCIIString());
						sock.send (request.toASCIIString());
LOG.trace ("{}: block on recvStr()", identity);
						final int response_code = Integer.parseInt (sock.recvStr());
LOG.trace ("{}: response HTTP/{}", identity, response_code);
						final String response = sock.recvStr();
						enableCompressionIfSupported (exchange);
						exchange.getResponseHeaders().set ("Content-Type", "application/json");
						exchange.sendResponseHeaders (response_code, 0);
						final OutputStream os = exchange.getResponseBody();
						os.write (response.getBytes());
						os.flush();
						os.close();
					} catch (Throwable t) {
						LOG.trace ("{}: 500 Internal Error: {}", identity, t.getMessage());
						exchange.sendResponseHeaders (HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
					} finally {
						sock.close();
					}
				}
			} else {
				LOG.trace ("405 Bad Method.");
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
			setName ("shutdown");
//			if (null != this.app)
			if (false)
			{
				LOG.trace ("Deactivating event queue ...");
				LOG.trace ("Notifying mainloop ... ");
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
		if (this.Initialize (line, options)) {
			Thread shutdown_hook = new ShutdownThread (this);
			Runtime.getRuntime().addShutdownHook (shutdown_hook);
			LOG.trace ("Shutdown hook installed.");
			this.mainloop();
			LOG.trace ("Shutdown in progress.");
/* Cannot remove hook if shutdown is in progress. */
//			Runtime.getRuntime().removeShutdownHook (shutdown_hook);
//			LOG.trace ("Removed shutdown hook.");
		}
		this.clear();
		this.is_shutdown = true;
	}

	public volatile boolean is_shutdown = false;

	private void drainqueue() {
		LOG.trace ("Draining event queue.");
		int count = 0;
		try {
//			while (this.event_queue.dispatch (Dispatchable.NO_WAIT) > 0) { ++count; }
			LOG.trace ("Queue contained {} events.", count);
//		} catch (DeactivatedException e) {
/* ignore on empty queue */
//			if (count > 0) LOG.catching (e);
		} catch (Exception e) {
			LOG.catching (e);
		}
	}

	private void mainloop() {
		this.http_server.start();
		LOG.info ("Listening on http://{}/", this.http_server.getAddress());
		LOG.trace ("Waiting ...");
		this.analytic_consumer.Run();
		this.http_server.stop (0 /* seconds */);
		LOG.trace ("Mainloop deactivated.");
	}

	final static Duration ONE_SECOND = Duration.ofSeconds (1);
	final static Duration ONE_DAY = Duration.ofDays (1);
	final DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

	private class Multipass implements AnalyticStreamDispatcher {
		private final ImmutableSet<AnalyticStream> requests;
		private final ZMQ.Socket dispatcher;
		private final String identity;
		private final Map<String, String> responses;

		public Multipass (ImmutableSet<AnalyticStream> requests, ZMQ.Socket dispatcher, String identity) {
			this.requests = requests;
			this.dispatcher = dispatcher;
			this.identity = identity;
			this.responses = Maps.newTreeMap();
		}

/* Format the final HTTP result, adjust per special snowflake requirements. */
		@Override
		public void dispatch (AnalyticStream stream, int response_code, String stream_response) {
			this.responses.put (stream.getItemName(), stream_response);
			if (this.responses.size() != this.requests.size()) {
/* pending complete result set */
				return;
			}
			else if (1 == this.requests.size()) {
LOG.trace ("http: send response {} to {}", response_code, this.identity);
				this.dispatcher.sendMore (this.identity);
				this.dispatcher.sendMore ("");
				this.dispatcher.sendMore (Integer.toString (response_code));
				this.dispatcher.send (stream_response);
			}
			else {
				final StringBuilder sb = new StringBuilder();
				sb.append ("[");
				final Joiner joiner = Joiner.on (",\n");
				joiner.appendTo (sb, this.responses.values());
				sb.append ("]");
				this.dispatcher.sendMore (this.identity);
				this.dispatcher.sendMore ("");
				this.dispatcher.sendMore (Integer.toString (response_code));
				this.dispatcher.send (sb.toString());
			}
/* complete, remove pass */
			multipass.remove (this.identity);
		}
	}

	@Override
	public void dispatch (AnalyticStream stream, int response_code, String response) {
		this.multipass.get (stream.getIdentity()).dispatch (stream, response_code, response);
	}

	public Map<String, Multipass> multipass;

// http://takoyaki/SBUX.O
// http://takoyaki/MSFT.O?signal=MMA(21,Close())
// http://takoyaki/MSFT.O,GOOG.O?signal=MMA(21,Close())
// http://takoyaki/SBUX.O/taqfromdatetime?datetime=2014-11-20T19:00:00.000Z
// --> #type=taqfromdatetime datetime=2014-11-20T19:00:00.000Z
// http://takoyaki/SBUX.O/tradeperformancespread?interval=2014-11-24T17:05:15.444Z/PT2M&returnformat=perunit
// --> #type=tradeperformancespread startdatetime=2014-11-24T17:05:15.444Z enddatetime=2014-11-24T17:07:15.444Z returnmode=historical returnformat=perunit

// http://takoyaki/SBUX.O/tas?interval=2014-11-24T17:05:15.444Z/PT2M
// http://takoyaki/SBUX.O/taq?interval=2014-11-24T17:05:15.444Z/PT2M

// supported parameters:
//   snapby       - [ time | trade | quote ]
//   lagtype      - [ trade | quote | second | minute | hour | volume | duration ]
//   lag          - [ signed integer | iso 8601 duration ]
//   interval     - < iso 8601 interval >
//   returnformat - [ perunit | decimal | percent | basispoints ]

	private void handler (URI request, String identity) {
		LOG.info ("GET: {}", request.toASCIIString());
		final Map<String, List<String>> query = parseQueryParameters (request.getQuery(), Charset.forName ("UTF-8"));
		Optional<String> signal = Optional.absent();
		Optional<String> techanalysis = Optional.absent(), datetime = Optional.absent(), snapby = Optional.absent(), lagtype = Optional.absent(), lag = Optional.absent();
		Optional<String> timeinterval = Optional.absent(), returnformat = Optional.absent();
		String[] items = {};
/* Validate each parameter */
		if (query.containsKey (SIGNAL_PARAM))
		{
/* Signals app */
			signal = Optional.of (getParameterValue (query, SIGNAL_PARAM));
/* ticker symbols */
			if (!Strings.isNullOrEmpty (request.getPath())
				&& request.getPath().length() > 1)
			{
				items = Iterables.toArray (Splitter.on (',')
						.trimResults()
						.omitEmptyStrings()
						.split (new File (request.getPath()).getName()), String.class);
			}
		}
		else if (!Strings.isNullOrEmpty (request.getPath())
				&& request.getPath().length() > 1)
		{
/* TechAnalysis app */
			File path = new File (request.getPath());
			items = Iterables.toArray (Splitter.on (',')
					.trimResults()
					.omitEmptyStrings()
					.split (path.getParentFile().getName()), String.class);
			techanalysis = Optional.of (path.getName());
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
			if (query.containsKey (TIMEINTERVAL_PARAM)) {
				timeinterval = Optional.of (getParameterValue (query, TIMEINTERVAL_PARAM));
			}
			if (query.containsKey (RETURNFORMAT_PARAM)) {
				returnformat = Optional.of (getParameterValue (query, RETURNFORMAT_PARAM));
			}
		}
		if (0 == items.length) {
			LOG.trace ("400 Bad Request");
			this.dispatcher.sendMore (identity);
			this.dispatcher.sendMore ("");
			this.dispatcher.sendMore (Integer.toString (HttpURLConnection.HTTP_BAD_REQUEST));
			this.dispatcher.send ("No items requested.");
			return;
		} else if (!signal.isPresent() && !techanalysis.isPresent()) {
// TBD: insert vanilla multi-pass here.
/*			final ItemStream[] streams = new ItemStream[ items.length ];
			for (int i = 0; i < streams.length; ++i) {
				streams[i] = new ItemStream (this);
			}
			this.consumer.batchCreateItemStream ("ELEKTRON_EDGE", items, DEFAULT_FIELDS, streams);
			this.multipass = new Multipass (ImmutableSet.copyOf (streams), this.dispatcher); */
			this.dispatcher.sendMore (identity);
			this.dispatcher.sendMore ("");
			this.dispatcher.sendMore (Integer.toString (HttpURLConnection.HTTP_NOT_IMPLEMENTED));
			this.dispatcher.send ("Not implemented.");
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
							"TechAnalysis",
							signal.get(),
							items[i]);
				streams[i] = new AnalyticStream (this, identity);
			}
		}
		else if ("tas".equals (techanalysis.get())
			|| "taq".equals (techanalysis.get())
/* TBD: minutes to be deployed 2016 */
			|| "days".equals (techanalysis.get())
			|| "weeks".equals (techanalysis.get())
			|| "months".equals (techanalysis.get())
			|| "quarters".equals (techanalysis.get())
			|| "years".equals (techanalysis.get()))
		{
			Interval parsed_interval;
			if (timeinterval.isPresent()) {
				try {
					parsed_interval = Interval.parse (timeinterval.get());
LOG.debug ("{} -> {}", timeinterval.get(), parsed_interval.toString());
				} catch (IllegalArgumentException e) {
					LOG.trace ("400 Bad Request");
					this.dispatcher.sendMore (identity);
					this.dispatcher.sendMore ("");
					this.dispatcher.sendMore (Integer.toString (HttpURLConnection.HTTP_BAD_REQUEST));
					this.dispatcher.send ("interval: " + e.getMessage());
					return;
				}
			} else {
				LOG.trace ("400 Bad Request");
				this.dispatcher.sendMore (identity);
				this.dispatcher.sendMore ("");
				this.dispatcher.sendMore (Integer.toString (HttpURLConnection.HTTP_BAD_REQUEST));
				this.dispatcher.send ("Interval parameter required for historical requests.");
				return;
			}
			LOG.trace ("history: {}", techanalysis.get());
			for (int i = 0; i < streams.length; ++i) {
				LOG.trace ("item[{}]: {}", i, items[i]);
				analytics[i] = new Analytic ("ELEKTRON_AUX_TEST",
							"History",
							techanalysis.get(),
							items[i]);
				analytics[i].setInterval (parsed_interval);
				streams[i] = new AnalyticStream (this, identity);
			}
		}
		else
		{
			StringBuilder sb = new StringBuilder();
			sb.append ("#type=")
			  .append (techanalysis.get());
			if (datetime.isPresent()) {
				try {
					final OffsetDateTime parsed_datetime = OffsetDateTime.parse (datetime.get());
					sb.append (" datetime=")
					  .append (parsed_datetime.toString());
				} catch (IllegalArgumentException e) {
					LOG.trace ("400 Bad Request");
					this.dispatcher.sendMore (identity);
					this.dispatcher.sendMore ("");
					this.dispatcher.sendMore (Integer.toString (HttpURLConnection.HTTP_BAD_REQUEST));
					this.dispatcher.send ("datetime: " + e.getMessage());
					return;
				}
			}
			if (snapby.isPresent()) {
				sb.append (" snapby=")
				  .append (snapby.get());
			}
			if (lag.isPresent()) {
				if (lagtype.isPresent()) {
// override content
					if (lagtype.get().equals ("duration")) {
						try {
							final Duration duration = Duration.ofDays (Period.parse (lag.get()).getDays());
							lagtype = Optional.of ("second");
/* drop nanoseconds */
							lag = Optional.of (Long.toString (duration.getSeconds()));
						} catch (IllegalArgumentException e) {
							LOG.trace ("400 Bad Request");
							this.dispatcher.sendMore (identity);
							this.dispatcher.sendMore ("");
							this.dispatcher.sendMore (Integer.toString (HttpURLConnection.HTTP_BAD_REQUEST));
							this.dispatcher.send ("lag: " + e.getMessage());
							return;
						}
					}
					sb.append (" lagtype=")
					  .append (lagtype.get());
				}
				sb.append (" lag=")
				  .append (lag.get());
			}
			if (timeinterval.isPresent()) {
				try {
					final Interval parsed_interval = Interval.parse (timeinterval.get());
					sb.append (" startdatetime=")
					  .append (parsed_interval.getStart().toDateTime (org.joda.time.DateTimeZone.UTC).toString())
					  .append (" enddatetime=")
					  .append (parsed_interval.getEnd().toDateTime (org.joda.time.DateTimeZone.UTC).toString())
					  .append (" returnmode=historical");
				} catch (IllegalArgumentException e) {
					LOG.trace ("400 Bad Request");
					this.dispatcher.sendMore (identity);
					this.dispatcher.sendMore ("");
					this.dispatcher.sendMore (Integer.toString (HttpURLConnection.HTTP_BAD_REQUEST));
					this.dispatcher.send ("interval: " + e.getMessage());
					return;
				}
			}
			if (returnformat.isPresent()) {
				sb.append (" returnformat=")
				  .append (returnformat.get());
			}
			final String analytic = sb.toString();
			LOG.trace ("techanalysis: {}", analytic);
			for (int i = 0; i < streams.length; ++i) {
				LOG.trace ("item[{}]: {}", i, items[i]);
				analytics[i] = new Analytic ("ECP_SAP",
							"TechAnalysis",
							analytic,
							items[i]);
				streams[i] = new AnalyticStream (this, identity);
			}
		}
		this.analytic_consumer.batchCreateAnalyticStream (analytics, streams);
		this.multipass.put (identity, new Multipass (ImmutableSet.copyOf (streams), this.dispatcher, identity));
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

		if (null != this.analytic_consumer) {
			LOG.trace ("Closing Consumer.");
			this.analytic_consumer.Close();
			this.analytic_consumer = null;
		}

		if (null != this.upa) {
			LOG.trace ("Closing UPA.");
			this.upa.clear();
			this.upa = null;
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
