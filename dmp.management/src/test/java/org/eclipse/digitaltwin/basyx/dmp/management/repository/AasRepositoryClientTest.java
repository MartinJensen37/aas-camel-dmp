/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
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
package org.eclipse.digitaltwin.basyx.dmp.management.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

class AasRepositoryClientTest {
	@Test
	void usesExactUnpaddedBase64UrlIdentifiersAndSafeAuthorization() throws Exception {
		String identifier = "urn:aas:owner/temperature-\u03c0";
		String expected = Base64.getUrlEncoder().withoutPadding().encodeToString(identifier.getBytes(StandardCharsets.UTF_8));
		assertEquals(expected, AasRepositoryClient.base64UrlIdentifier(identifier));
		AtomicReference<String> requestPath = new AtomicReference<>();
		AtomicReference<String> authorization = new AtomicReference<>();
		HttpServer server = server(exchange -> {
			requestPath.set(exchange.getRequestURI().getRawPath());
			authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
			respond(exchange, 200, submodel(identifier));
		});
		try {
			server.start();
			var client = client(server, Duration.ofSeconds(1),
					AasRepositoryClient.bearerAuthorization("unit-secret"));
			var result = client.getSubmodel(identifier);
			assertEquals(AasRepositoryClient.Status.FOUND, result.status());
			assertEquals(identifier, result.value().getId());
			assertEquals("/repo/submodels/" + expected, requestPath.get());
			assertEquals("Bearer unit-secret", authorization.get());
			assertFalse(result.detail().contains("unit-secret"));
		} finally {
			server.stop(0);
		}
	}

	@Test
	void listsBySemanticIdWithAStableCursorAndIsolatesMalformedSubmodels() throws Exception {
		String aimc = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel";
		AtomicInteger pages = new AtomicInteger();
		HttpServer server = server(exchange -> {
			String query = exchange.getRequestURI().getRawQuery();
			assertTrue(query.contains("semanticId=" + AasRepositoryClient.base64UrlIdentifier(aimc)), query);
			if (pages.getAndIncrement() == 0) {
				assertTrue(query.contains("limit=1"));
				respond(exchange, 200, "{\"result\":[" + header("urn:arbitrary:one", aimc).replace("{\"malformed-but-unfetched\":true}", "")
						+ "],\"paging_metadata\":{\"cursor\":\"next token\"}}");
			} else {
				assertTrue(query.contains("cursor=next+token"));
				respond(exchange, 200, "{\"result\":[{\"modelType\":\"Submodel\",\"id\":\"urn:arbitrary:two\","
						+ "\"submodelElements\":[{\"modelType\":\"NoSuchElement\"}]}],\"paging_metadata\":{}}");
			}
		});
		try {
			server.start();
			var result = client(server, Duration.ofSeconds(1), null, 1, 2).listSubmodelsBySemanticId(aimc);
			assertEquals(AasRepositoryClient.Status.FOUND, result.status());
			assertEquals(2, result.value().size());
			assertEquals("urn:arbitrary:one", result.value().get(0).id());
			assertTrue(org.eclipse.digitaltwin.basyx.dmp.management.model.WireProjection.isAimc(result.value().get(0).submodel()));
			assertEquals("urn:arbitrary:two", result.value().get(1).id());
			assertEquals(null, result.value().get(1).submodel(), "a malformed listed submodel keeps its ID only");
			assertEquals(2, pages.get());
		} finally {
			server.stop(0);
		}
	}

	@Test
	void readsMetadataWithoutElementsAndElementsByEncodedIdShortPath() throws Exception {
		AtomicReference<String> requestPath = new AtomicReference<>();
		HttpServer server = server(exchange -> {
			String path = exchange.getRequestURI().getRawPath();
			requestPath.set(path);
			if (path.endsWith("/$metadata")) respond(exchange, 200, header("urn:x", "urn:semantic"));
			else respond(exchange, 200, "{\"modelType\":\"Property\",\"idShort\":\"c\",\"valueType\":\"xs:int\",\"value\":\"4\"}");
		});
		try {
			server.start();
			var client = client(server, Duration.ofSeconds(1), null);
			var metadata = client.getSubmodelMetadata("urn:x");
			assertEquals(AasRepositoryClient.Status.FOUND, metadata.status());
			assertTrue(metadata.value().getSubmodelElements() == null || metadata.value().getSubmodelElements().isEmpty(),
					"$metadata elements are neither parsed nor kept");
			assertEquals("/repo/submodels/" + AasRepositoryClient.base64UrlIdentifier("urn:x") + "/$metadata", requestPath.get());
			var element = client.getSubmodelElement("urn:x", "a.b[0].c");
			assertEquals("c", element.value().getIdShort());
			assertEquals("/repo/submodels/" + AasRepositoryClient.base64UrlIdentifier("urn:x") + "/submodel-elements/a.b%5B0%5D.c",
					requestPath.get());
		} finally { server.stop(0); }
	}

	@Test
	void distinguishesMissingAuthorizationTransientAndInvalidResponses() throws Exception {
		HttpServer server = server(exchange -> {
			String encoded = exchange.getRequestURI().getRawPath().substring("/repo/submodels/".length());
			String identifier = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
			if (identifier.equals("missing")) respond(exchange, 404, "{}");
			else if (identifier.equals("private")) respond(exchange, 401, "{}");
			else if (identifier.equals("busy")) respond(exchange, 503, "{}");
			else respond(exchange, 200, "not-json");
		});
		try {
			server.start();
			var client = client(server, Duration.ofSeconds(1), null);
			assertEquals(AasRepositoryClient.Status.NOT_FOUND, client.getSubmodel("missing").status());
			assertEquals(AasRepositoryClient.Status.UNAUTHORIZED, client.getSubmodel("private").status());
			assertEquals(AasRepositoryClient.Status.TRANSIENT_FAILURE, client.getSubmodel("busy").status());
			assertEquals(AasRepositoryClient.Status.INVALID_RESPONSE, client.getSubmodel("bad").status());
		} finally {
			server.stop(0);
		}
	}

	@Test
	void boundsRequestTimeAndRejectsCredentialBearingBaseUris() throws Exception {
		HttpServer server = server(exchange -> {
			try { Thread.sleep(250); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			respond(exchange, 200, submodel("urn:slow"));
		});
		try {
			server.start();
			var result = client(server, Duration.ofMillis(40), null).getSubmodel("urn:slow");
			assertEquals(AasRepositoryClient.Status.TRANSIENT_FAILURE, result.status());
			assertTrue(result.detail().contains("timed out"));
		} finally {
			server.stop(0);
		}
		org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
				() -> new AasRepositoryClient(URI.create("http://user:secret@localhost"), Duration.ofSeconds(1),
						Duration.ofSeconds(1), 10, 4, null));
	}

	@Test
	void rejectsOversizedMembershipResponseWithoutTreatingItAsEmpty() throws Exception {
		HttpServer server = server(exchange -> {
			try { respond(exchange, 200, " ".repeat(16 * 1024 * 1024 + 1)); }
			catch (IOException cancelledBody) { exchange.close(); }
		});
		server.start();
		try {
			var result = client(server, Duration.ofSeconds(5), null).listSubmodelsBySemanticId("urn:any");
			assertEquals(AasRepositoryClient.Status.INVALID_RESPONSE, result.status());
			assertEquals(null, result.value());
			assertTrue(result.detail().contains("body limit"));
		} finally { server.stop(0); }
	}

	private static AasRepositoryClient client(HttpServer server, Duration timeout, String authorization) {
		return client(server, timeout, authorization, 10, 4);
	}

	private static AasRepositoryClient client(HttpServer server, Duration timeout, String authorization,
			int pageSize, int maxPages) {
		return new AasRepositoryClient(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/repo"),
				Duration.ofSeconds(1), timeout, pageSize, maxPages, authorization);
	}

	private static HttpServer server(com.sun.net.httpserver.HttpHandler handler) throws IOException {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/repo", handler);
		return server;
	}

	private static void respond(HttpExchange exchange, int status, String body) throws IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().set("Content-Type", "application/json");
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}

	private static String header(String id, String semantic) {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + id + "\",\"idShort\":\"x\","
				+ "\"semanticId\":{\"type\":\"ModelReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\""
				+ semantic + "\"}]},\"submodelElements\":[{\"malformed-but-unfetched\":true}]}";
	}

	private static String submodel(String id) {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + id
				+ "\",\"idShort\":\"submodel\",\"submodelElements\":[]}";
	}
}
