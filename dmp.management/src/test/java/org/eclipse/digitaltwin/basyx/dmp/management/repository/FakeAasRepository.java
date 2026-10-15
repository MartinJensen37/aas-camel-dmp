/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.management.repository;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * An in-memory AAS submodel repository speaking the parts of the Part 2 API the DMP reads: semanticId-filtered
 * listing, whole submodels, {@code $metadata} and element paths. Tests can override any response.
 */
public final class FakeAasRepository implements AutoCloseable {
	private final HttpServer server;
	private final String base;
	private final Map<String, String> models = Collections.synchronizedMap(new LinkedHashMap<>());
	private final List<String> requests = new CopyOnWriteArrayList<>();
	private final java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();
	private final Map<String, Function<String, Response>> overrides = new ConcurrentHashMap<>();
	private volatile boolean semanticFilter = true, metadata = true, elementPaths = true;

	/** A response: status and body. */
	public record Response(int status, String body) { }

	public FakeAasRepository() throws IOException { this("/repo"); }

	public FakeAasRepository(String base) throws IOException {
		this.base = base;
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(8, task -> {
			Thread thread = new Thread(task, "fake-aas-repository");
			thread.setDaemon(true);
			return thread;
		}));
		server.createContext(base.isEmpty() ? "/" : base, this::handle);
		server.start();
	}

	public URI uri() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + base); }
	public int port() { return server.getAddress().getPort(); }

	public AasRepositoryClient client() {
		return new AasRepositoryClient(uri(), Duration.ofSeconds(2), Duration.ofSeconds(2), 50, 4, null);
	}

	/** Adds or replaces a submodel by its {@code id}. */
	public FakeAasRepository put(String submodelJson) {
		String id = JsonParser.parseString(submodelJson).getAsJsonObject().get("id").getAsString();
		models.put(id, submodelJson);
		return this;
	}

	public void remove(String id) { models.remove(id); }
	public String get(String id) { return models.get(id); }
	public Map<String, String> models() { return models; }

	/** Request paths, with their queries, in arrival order. */
	public List<String> requests() { return List.copyOf(requests); }
	public void clearRequests() { requests.clear(); }

	/** Requests other than GET; the DMP only reads the repository. */
	public int writes() { return writes.get(); }

	/** Requests that read this submodel: whole, {@code $metadata} or an element. */
	public long reads(String id) {
		String path = base + "/submodels/" + AasRepositoryClient.base64UrlIdentifier(id);
		return requests.stream().filter(request -> request.equals(path) || request.startsWith(path + "/")).count();
	}

	/** Answers matching requests itself; return null from {@code respond} to fall through. */
	public void override(String name, Function<String, Response> respond) { overrides.put(name, respond); }
	public void clearOverride(String name) { overrides.remove(name); }

	/** Like a repository that ignores the semanticId query parameter. */
	public void ignoreSemanticFilter() { semanticFilter = false; }
	/** Like a repository without {@code $metadata} (400). */
	public void withoutMetadata() { metadata = false; }
	/** Like a repository without element-path reads (404). */
	public void withoutElementPaths() { elementPaths = false; }

	@Override public void close() { server.stop(0); }

	private void handle(HttpExchange exchange) throws IOException {
		try {
			String path = exchange.getRequestURI().getRawPath();
			String query = exchange.getRequestURI().getRawQuery();
			String request = path + (query == null ? "" : "?" + query);
			requests.add(request);
			if (!"GET".equals(exchange.getRequestMethod())) writes.incrementAndGet();
			for (var override : overrides.values()) {
				Response response = override.apply(request);
				if (response != null) { respond(exchange, response); return; }
			}
			respond(exchange, answer(path.substring(base.length()), query));
		} catch (RuntimeException failure) {
			respond(exchange, new Response(500, "{}"));
		}
	}

	private Response answer(String path, String query) {
		if (path.equals("/submodels")) return list(query);
		if (!path.startsWith("/submodels/")) return new Response(404, "{}");
		String[] parts = path.substring("/submodels/".length()).split("/", 3);
		String id = new String(Base64.getUrlDecoder().decode(parts[0]), StandardCharsets.UTF_8);
		String model = models.get(id);
		if (parts.length == 1) return model == null ? new Response(404, "{}") : new Response(200, model);
		if (parts.length == 2 && parts[1].equals("$metadata")) {
			if (!metadata) return new Response(400, "{}");
			if (model == null) return new Response(404, "{}");
			JsonObject header = JsonParser.parseString(model).getAsJsonObject();
			header.remove("submodelElements");
			return new Response(200, header.toString());
		}
		if (parts.length == 3 && parts[1].equals("submodel-elements")) {
			if (!elementPaths || model == null) return new Response(404, "{}");
			JsonElement element = navigate(JsonParser.parseString(model).getAsJsonObject(),
					URLDecoder.decode(parts[2], StandardCharsets.UTF_8));
			return element == null ? new Response(404, "{}") : new Response(200, element.toString());
		}
		return new Response(404, "{}");
	}

	private Response list(String query) {
		Map<String, String> parameters = new LinkedHashMap<>();
		if (query != null) for (String pair : query.split("&")) {
			int equals = pair.indexOf('=');
			if (equals > 0) parameters.put(pair.substring(0, equals), URLDecoder.decode(pair.substring(equals + 1), StandardCharsets.UTF_8));
		}
		String semantic = semanticFilter && parameters.containsKey("semanticId")
				? new String(Base64.getUrlDecoder().decode(parameters.get("semanticId")), StandardCharsets.UTF_8) : null;
		List<String> matching = new ArrayList<>();
		synchronized (models) {
			for (String model : models.values()) if (semantic == null || semantics(model).contains(semantic)) matching.add(model);
		}
		int limit = parameters.containsKey("limit") ? Integer.parseInt(parameters.get("limit")) : matching.size();
		int from = parameters.containsKey("cursor") ? Integer.parseInt(parameters.get("cursor")) : 0;
		int to = Math.min(matching.size(), from + limit);
		String cursor = to < matching.size() ? ",\"paging_metadata\":{\"cursor\":\"" + to + "\"}" : ",\"paging_metadata\":{}";
		return new Response(200, "{\"result\":[" + String.join(",", matching.subList(from, to)) + "]" + cursor + "}");
	}

	private static List<String> semantics(String model) {
		JsonObject object = JsonParser.parseString(model).getAsJsonObject();
		List<String> values = new ArrayList<>();
		if (object.get("semanticId") instanceof JsonObject semantic && semantic.get("keys") instanceof JsonArray keys)
			for (JsonElement key : keys) values.add(key.getAsJsonObject().get("value").getAsString());
		return values;
	}

	/** Follows an idShort path ({@code a.b[0].c}) through submodelElements, value and statements. */
	static JsonElement navigate(JsonObject submodel, String idShortPath) {
		JsonElement current = submodel;
		for (String part : idShortPath.split("\\.")) {
			int bracket = part.indexOf('[');
			String name = bracket < 0 ? part : part.substring(0, bracket);
			current = child(current, name);
			if (current == null) return null;
			if (bracket >= 0) for (String index : part.substring(bracket + 1).split("\\]\\[?")) {
				if (index.isEmpty()) continue;
				JsonElement values = current.getAsJsonObject().get("value");
				if (values == null || !values.isJsonArray() || values.getAsJsonArray().size() <= Integer.parseInt(index)) return null;
				current = values.getAsJsonArray().get(Integer.parseInt(index));
			}
		}
		return current;
	}

	private static JsonElement child(JsonElement parent, String idShort) {
		if (parent == null || !parent.isJsonObject()) return null;
		for (String key : new String[] { "submodelElements", "value", "statements" }) {
			JsonElement children = parent.getAsJsonObject().get(key);
			if (children == null || !children.isJsonArray()) continue;
			for (JsonElement child : children.getAsJsonArray())
				if (child.isJsonObject() && child.getAsJsonObject().has("idShort")
						&& idShort.equals(child.getAsJsonObject().get("idShort").getAsString())) return child;
		}
		return null;
	}

	private static void respond(HttpExchange exchange, Response response) throws IOException {
		byte[] bytes = response.body().getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().add("Content-Type", "application/json");
		exchange.sendResponseHeaders(response.status(), bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}
}
