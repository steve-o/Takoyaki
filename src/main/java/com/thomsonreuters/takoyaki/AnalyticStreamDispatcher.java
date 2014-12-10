/* final analytic stream serialized message bubble up device.
 */

package com.thomsonreuters.Takoyaki;

public interface AnalyticStreamDispatcher {
	public void dispatch (AnalyticStream stream, int response_code, String response);
}

/* eof */
