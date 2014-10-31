/* final item stream serialized message bubble up device.
 */

package com.thomsonreuters.Takoyaki;

public interface ItemStreamDispatcher {
	public void dispatch (ItemStream stream, String response);
}

/* eof */
