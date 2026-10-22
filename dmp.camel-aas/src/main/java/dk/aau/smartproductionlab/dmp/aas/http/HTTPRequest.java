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
package dk.aau.smartproductionlab.dmp.aas.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** JSON writes to the AAS repository over the JDK HTTP client. */
public class HTTPRequest {
	private static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

	public static void patchRequest(String url, String content) throws IOException {
		send("PATCH", url, content);
	}

	public static void putRequest(String url, String content) throws IOException {
		send("PUT", url, content);
	}

	private static void send(String method, String url, String content) throws IOException {
		HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30))
				.header("Content-type", "application/json")
				.method(method, HttpRequest.BodyPublishers.ofString(content, StandardCharsets.UTF_8)).build();
		HttpResponse<String> response;
		try { response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)); }
		catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IOException("AAS write interrupted", interrupted);
		}
		int status = response.statusCode();
		String detail = response.body() == null ? "" : response.body();
		if (status < 200 || status >= 300) {
			throw new IOException("AAS write failed with HTTP " + status + (detail.isBlank() ? "" : ": " + detail));
		}
	}
}
