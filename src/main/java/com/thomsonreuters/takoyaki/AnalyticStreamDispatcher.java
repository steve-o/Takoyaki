/* final analytic stream serialized message bubble up device.
 */

package com.thomsonreuters.Takoyaki;

public interface AnalyticStreamDispatcher {
	public void dispatch (AnalyticStream stream, String response);
}

/* eof */
