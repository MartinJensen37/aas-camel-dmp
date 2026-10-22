/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.component.health;

import org.apache.camel.Exchange;
import org.apache.camel.Handler;
import java.net.HttpURLConnection;
import dk.aau.smartproductionlab.dmp.component.ManagedRuntimeStatus;

/**
 * A processor class for processing the health status
 *
 * @author danish
 *
 */
public class HealthCheckProcessor {

	@Handler
	public void processHealthCheck(Exchange exchange) {
		var status = ManagedRuntimeStatus.snapshot();
		exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE,
				status.ready() ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_UNAVAILABLE);
		exchange.getMessage().setBody(java.util.Map.of("ready", status.ready(),
				"state", status.state().name().toLowerCase(java.util.Locale.ROOT),
				"initialScanComplete", status.initialScanComplete(), "category", status.category()));
	}
}
