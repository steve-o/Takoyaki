/* Gateway configuration.
 */

package com.thomsonreuters.Takoyaki;

import com.google.common.base.Optional;
import com.google.gson.Gson;

public class SessionConfig {
//  RFA session name, one session contains a horizontal scaling set of connections.
	private String session_name;

//  RFA connection name, used for logging.
	private String connection_name;

//  RFA consumer name.
	private String consumer_name;

//  Protocol name, RSSL or SSL.
	private String protocol;

//  TREP-RT ADH hostname or IP address.
	private String[] servers;

//  Default TREP-RT R/SSL port, e.g. 14002, 14003, 8101.
	private Optional<String> default_port;

//  TREP-RT service name, e.g. IDN_RDF.
	private Optional<String> service_name;

/* DACS application Id.  If the server authenticates with DACS, the consumer
 * application may be required to pass in a valid ApplicationId.
 * Range: "" (None) or 1-511 as an Ascii string.
 */
	private Optional<String> application_id;

/* InstanceId is used to differentiate applications running on the same host.
 * If there is more than one noninteractive provider instance running on the
 * same host, they must be set as a different value by the provider
 * application. Otherwise, the infrastructure component which the providers
 * connect to will reject a login request that has the same InstanceId value
 * and cut the connection.
 * Range: "" (None) or any Ascii string, presumably to maximum RFA_String length.
 */
	private Optional<String> instance_id;

/* DACS username, frequently non-checked and set to similar: user1.
 */
	private Optional<String> user_name;

/* DACS position, the station which the user is using.
 * Range: "" (None) or "<IPv4 address>/hostname" or "<IPv4 address>/net"
 */
	private Optional<String> position;

/* Local dictionary files to override source delivered version.
 */
	private Optional<String> field_dictionary;
	private Optional<String> enum_dictionary;

/* c.f. retryTimer and retryLimit in ADH.
 * retryTimer - The time to wait for a response for an item before the item is re-requested.
 *              0 = immediate SUSPECT STATUS generation.
 * retryLimit - Number of times to send an open request before giving up.
 *              0 = abort after first retryTimer interval.
 */
	private Optional<String> retry_timer;
	private Optional<String> retry_limit;

/* Application server login UUID.
 */
	private Optional<String> uuid;

/* App server login password.
 */
	private Optional<String> password;

	public SessionConfig (String session_name, String connection_name, String consumer_name, String protocol, String[] servers) {
		this.session_name = session_name;
		this.connection_name = connection_name;
		this.consumer_name = consumer_name;
		this.protocol = protocol;
		this.servers = servers;
		this.default_port = Optional.absent();
		this.service_name = Optional.absent();
		this.application_id = Optional.absent();
		this.instance_id = Optional.absent();
		this.user_name = Optional.absent();
		this.position = Optional.absent();
		this.field_dictionary = Optional.absent();
		this.enum_dictionary = Optional.absent();
		this.retry_timer = Optional.absent();
		this.retry_limit = Optional.absent();
                this.uuid = Optional.absent();
                this.password = Optional.absent();
	}

	public String getSessionName() {
		return this.session_name;
	}

	public String getConnectionName() {
		return this.connection_name;
	}

	public String getConsumerName() {
		return this.consumer_name;
	}

	public String getProtocol() {
		return this.protocol;
	}

	public String[] getServers() {
		return this.servers;
	}

/* optional parameters */
	public boolean hasDefaultPort() {
		return this.default_port.isPresent();
	}

	public String getDefaultPort() {
		return this.default_port.get();
	}

	public void setDefaultPort (String default_port) {
		this.default_port = Optional.of (default_port);
	}

	public boolean hasServiceName() {
		return this.service_name.isPresent();
	}

	public String getServiceName() {
		return this.service_name.get();
	}

	public void setServiceName (String service_name) {
		this.service_name = Optional.of (service_name);
	}

	public boolean hasApplicationId() {
		return this.application_id.isPresent();
	}

	public String getApplicationId() {
		return this.application_id.get();
	}

	public void setApplicationId (String application_id) {
		this.application_id = Optional.of (application_id);
	}

	public boolean hasInstanceId() {
		return this.instance_id.isPresent();
	}

	public String getInstanceId() {
		return this.instance_id.get();
	}

	public void setInstanceId (String instance_id) {
		this.instance_id = Optional.of (instance_id);
	}

	public boolean hasUserName() {
		return this.user_name.isPresent();
	}

	public String getUserName() {
		return this.user_name.get();
	}

	public void setUserName (String user_name) {
		this.user_name = Optional.of (user_name);
	}

	public boolean hasPosition() {
		return this.position.isPresent();
	}

	public String getPosition() {
		return this.position.get();
	}

	public void setPosition (String position) {
		this.position = Optional.of (position);
	}

	public boolean hasFieldDictionary() {
		return this.field_dictionary.isPresent();
	}
                
	public String getFieldDictionary() {
		return this.field_dictionary.get();
	}
                
	public void setFieldDictionary (String dictionary) {
		this.field_dictionary = Optional.of (dictionary);
	}
                
	public boolean hasEnumDictionary() {
		return this.enum_dictionary.isPresent();
	}
                
	public String getEnumDictionary() {
		return this.enum_dictionary.get();
	}
                
	public void setEnumDictionary (String dictionary) {
		this.enum_dictionary = Optional.of (dictionary);
	}

	public boolean hasRetryTimer() {
		return this.retry_timer.isPresent();
	}

	public String getRetryTimer() {
		return this.retry_timer.get();
	}

	public void setRetryTimer (String time) {
		this.retry_timer = Optional.of (time);
	}

	public boolean hasRetryLimit() {
		return this.retry_limit.isPresent();
	}

	public String getRetryLimit() {
		return this.retry_limit.get();
	}

	public void setRetryLimit (String time) {
		this.retry_limit = Optional.of (time);
	}
                
	public boolean hasUuid() {
		return this.uuid.isPresent();
	}
                
	public String getUuid() {
		return this.uuid.get();
	}
                
	public void setUuid (String uuid) {
		this.uuid = Optional.of (uuid);
	}

	public boolean hasPassword() {
		return this.password.isPresent();
	}

	public String getPassword() {
		return this.password.get();
	}

	public void setPassword (String password) {
		this.password = Optional.of (password);
	}

	@Override
	public String toString() {
		return new Gson().toJson (this);
	}
}

/* eof */
