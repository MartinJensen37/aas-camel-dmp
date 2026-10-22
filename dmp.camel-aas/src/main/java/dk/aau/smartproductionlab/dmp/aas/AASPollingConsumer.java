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

package dk.aau.smartproductionlab.dmp.aas;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultMessage;
import org.apache.camel.support.PollingConsumerSupport;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;

/**
 * An implementation of AASPolling Consumer
 * @author rana
 *
 */
public class AASPollingConsumer extends PollingConsumerSupport {
	private static final int WAIT_INDEFINITELY = -1;
	private static final int NO_WAIT = 0;
	private AASEndpoint endpoint;
	
	public AASPollingConsumer(AASEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}
	
	@Override
	public AASEndpoint getEndpoint() {
		return (AASEndpoint) super.getEndpoint();
	}
	

	@Override
	public Exchange receive() {
		return doReceive(WAIT_INDEFINITELY);
	}

	@Override
	public Exchange receiveNoWait() {
		return doReceive(NO_WAIT);
	}

	@Override
	public Exchange receive(long timeout) {
		return doReceive(timeout);
	}
	
	protected Exchange doReceive(long timeout) {
		ExecutorService execService = Executors.newSingleThreadExecutor();
	        Callable<Object> runWithTimeout = this::getMetamodelValue;
        
	        Future<Object> responseFuture = execService.submit(runWithTimeout);
        
        try {
			// Camel's PollingConsumer timeout is in milliseconds. A failed read
			// must return no exchange: an empty string would enter the fan-in cache.
			Object element = timeout == WAIT_INDEFINITELY
					? responseFuture.get() : responseFuture.get(timeout, TimeUnit.MILLISECONDS);
			// Lua receives the element's value, typed as the value feed delivers it (SPEC §5.3).
			Object decodedValue = AasValues.elementValue(element);
			if (decodedValue == null) {
				return null;
			}
			Exchange exchange = createExchange(decodedValue);
			defaultConsumerCallback(exchange, true);
			return exchange;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			getExceptionHandler().handleException(e);
		} catch (TimeoutException | ExecutionException e) {
			getExceptionHandler().handleException(e);
        } finally {
			responseFuture.cancel(true);
			execService.shutdownNow();
        }
		return null;
	}
	
	private Exchange createExchange(Object decodedValue) {
		
		Exchange exchange = endpoint.createExchange();
		
		DefaultMessage exMsg = new DefaultMessage(exchange.getContext());
		exMsg.setBody(decodedValue);
		exchange.setIn(exMsg);
		
		return exchange;
	}
	
	/**
	 * Get serialized metamodel
	 * @return serialized meta model
	 */
	private Object getMetamodelValue() throws IOException {
		return JsonCodec.parseJson(readJson(getMetamodelUrl(), "AAS"));
	}

	private static final java.net.http.HttpClient CLIENT = java.net.http.HttpClient.newBuilder()
			.connectTimeout(java.time.Duration.ofSeconds(10)).build();

	private static String readJson(String url, String description) throws IOException {
		var request = java.net.http.HttpRequest.newBuilder(java.net.URI.create(url)).timeout(java.time.Duration.ofSeconds(30))
				.header("Content-type", "application/json").header("Accept", "application/json").GET().build();
		java.net.http.HttpResponse<String> response;
		try { response = CLIENT.send(request, java.net.http.HttpResponse.BodyHandlers.ofString(java.nio.charset.StandardCharsets.UTF_8)); }
		catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IOException(description + " read interrupted", interrupted);
		}
		if (response.statusCode() < 200 || response.statusCode() >= 300)
			throw new IOException(description + " read failed with HTTP " + response.statusCode());
		return response.body();
	}

	/**
	 * Get metamodel url
	 * @return url
	 */
	protected String getMetamodelUrl() {
		
		if (!this.getEndpoint().getPropertyPath().isEmpty())
			return this.getEndpoint().getFullProxyUrl();
		
		return this.getEndpoint().getSubmodelEndpoint();
	}
}
