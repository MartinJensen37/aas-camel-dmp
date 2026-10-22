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
package dk.aau.smartproductionlab.dmp.management.repository;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;

/** Read-only bounded HTTP client for the AAS Repository v3 endpoints used by management. */
public final class AasRepositoryClient {
	private static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
	private static final int MAX_PAGINATED_RESULTS = 10_000;
	private static final ObjectMapper PAGES = new ObjectMapper();
	public enum Status { FOUND, NOT_FOUND, UNAUTHORIZED, TRANSIENT_FAILURE, INVALID_RESPONSE }

	public record Result<T>(Status status, T value, int httpStatus, String detail) {
		public Result {
			Objects.requireNonNull(status);
			if (detail == null) detail = "";
		}
		public boolean found() { return status == Status.FOUND; }
	}

	private final URI baseUri;
	private final HttpClient http;
	private final Duration requestTimeout;
	private final int pageSize;
	private final int maxPages;
	private final String authorization;

	public AasRepositoryClient(URI baseUri, Duration connectTimeout, Duration requestTimeout,
			int pageSize, int maxPages, String authorization) {
		this.baseUri = validateBase(baseUri);
		this.requestTimeout = requireBounded(requestTimeout, "requestTimeout");
		if (pageSize < 1 || pageSize > 1_000) throw new IllegalArgumentException("pageSize must be 1..1000");
		if (maxPages < 1 || maxPages > 1_000) throw new IllegalArgumentException("maxPages must be 1..1000");
		this.pageSize = pageSize;
		this.maxPages = maxPages;
		this.authorization = validateAuthorization(authorization);
		this.http = HttpClient.newBuilder().connectTimeout(requireBounded(connectTimeout, "connectTimeout"))
				.followRedirects(HttpClient.Redirect.NEVER).build();
	}

	public static String base64UrlIdentifier(String identifier) {
		if (identifier == null || identifier.isEmpty()) throw new IllegalArgumentException("identifier is required");
		return Base64.getUrlEncoder().withoutPadding().encodeToString(identifier.getBytes(StandardCharsets.UTF_8));
	}

	public static String bearerAuthorization(String token) {
		if (token == null || token.isBlank() || token.contains("\r") || token.contains("\n")) {
			throw new IllegalArgumentException("bearer token is required");
		}
		return "Bearer " + token;
	}

	public static String basicAuthorization(String username, String password) {
		if (username == null || password == null || username.contains(":") || username.contains("\r")
				|| username.contains("\n") || password.contains("\r") || password.contains("\n")) {
			throw new IllegalArgumentException("valid basic credentials are required");
		}
		String encoded = Base64.getEncoder().encodeToString((username + ":" + password)
				.getBytes(StandardCharsets.UTF_8));
		return "Basic " + encoded;
	}

	public Result<Submodel> getSubmodel(String identifier) {
		return get("/submodels/" + base64UrlIdentifier(identifier), json -> Aas.read(json, Submodel.class));
	}

	/** The submodel without its elements ({@code $metadata}): identity and semantics only. */
	public Result<Submodel> getSubmodelMetadata(String identifier) {
		return get("/submodels/" + base64UrlIdentifier(identifier) + "/$metadata",
				json -> Aas.read(withoutElements(parseObject(json)).toString(), Submodel.class));
	}

	/** One element subtree addressed by its idShortPath ({@code a.b[0].c}). */
	public Result<SubmodelElement> getSubmodelElement(String identifier, String idShortPath) {
		if (idShortPath == null || idShortPath.isBlank()) throw new IllegalArgumentException("idShortPath is required");
		return get("/submodels/" + base64UrlIdentifier(identifier) + "/submodel-elements/" + encodePath(idShortPath),
				json -> Aas.read(json, SubmodelElement.class));
	}

	/**
	 * Lists every submodel carrying {@code semanticId}, filtered by the repository. A repository that ignores the
	 * filter still returns correct input: callers check the semantics of each listed submodel. A listed submodel
	 * that cannot be read is returned with its identifier and a null value, so one bad model never hides the others.
	 */
	public Result<List<Listed>> listSubmodelsBySemanticId(String semanticId) {
		return pages("/submodels", "semanticId=" + base64UrlIdentifier(semanticId));
	}

	/** One listed submodel; {@code submodel} is null when the listed JSON is not a valid submodel. */
	public record Listed(String id, Submodel submodel) { }

	private static String encodePath(String idShortPath) {
		return URLEncoder.encode(idShortPath, StandardCharsets.UTF_8).replace("+", "%20");
	}

	private static JsonNode withoutElements(JsonNode submodel) {
		if (submodel instanceof com.fasterxml.jackson.databind.node.ObjectNode object) object.remove("submodelElements");
		return submodel;
	}

	public Result<WireModel.Aimc> getAimc(String identifier) {
		return get("/submodels/" + base64UrlIdentifier(identifier),
				json -> WireProjection.aimcIsolated(Aas.read(json, Submodel.class)));
	}

	public Result<WireModel.Aid> getAid(String identifier) {
		return get("/submodels/" + base64UrlIdentifier(identifier), json -> WireProjection.aid(Aas.read(json, Submodel.class)));
	}

	private <T> Result<T> get(String path, Function<String, T> decode) {
		try {
			HttpResponse<String> response = http.send(request(path), ignored -> new BoundedStringBodySubscriber());
			if (response.statusCode() != 200) return statusFailure(response.statusCode());
			try {
				return found(decode.apply(response.body()));
			} catch (RuntimeException e) {
				return invalid("malformed repository model response");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return transientFailure("repository request interrupted");
		} catch (java.net.http.HttpTimeoutException e) {
			return transientFailure("repository request timed out");
		} catch (ResponseTooLargeException e) {
			return invalid("repository response exceeded configured body limit");
		} catch (IOException e) {
			if (e.getCause() instanceof ResponseTooLargeException) return invalid("repository response exceeded configured body limit");
			return transientFailure("repository transport failed");
		} catch (IllegalArgumentException e) {
			return transientFailure("repository transport failed");
		}
	}

	private Result<List<Listed>> pages(String path, String filter) {
		List<Listed> result = new ArrayList<>();
		Set<String> cursors = new HashSet<>();
		String cursor = null;
		for (int page = 0; page < maxPages; page++) {
			String query = "?" + filter + "&limit=" + pageSize
					+ (cursor == null ? "" : "&cursor=" + URLEncoder.encode(cursor, StandardCharsets.UTF_8));
			Result<JsonNode> response = get(path + query, AasRepositoryClient::parseObject);
			if (!response.found()) return new Result<>(response.status(), null, response.httpStatus(), response.detail());
			JsonNode rawResults = response.value().get("result");
			if (rawResults == null || !rawResults.isArray()) return invalid("repository page has no result array");
			if (result.size() + rawResults.size() > MAX_PAGINATED_RESULTS) return invalid("repository pagination exceeded configured result bound");
			for (JsonNode item : rawResults) {
				if (item == null || !item.isObject()) return invalid("repository page contains a non-object result entry");
				JsonNode id = item.get("id");
				String identifier = id != null && id.isTextual() && !id.asText().isBlank() ? id.asText() : null;
				Submodel submodel;
				try { submodel = Aas.read(item.toString(), Submodel.class); }
				catch (IllegalArgumentException malformed) { submodel = null; }
				if (identifier == null && submodel == null) return invalid("repository page contains an unidentifiable submodel");
				result.add(new Listed(identifier == null ? submodel.getId() : identifier, submodel));
			}
			JsonNode next = response.value().path("paging_metadata").get("cursor");
			if (next == null || !next.isTextual() || next.asText().isBlank()) return found(List.copyOf(result));
			if (!cursors.add(next.asText())) return invalid("repository pagination cursor repeated");
			cursor = next.asText();
		}
		return invalid("repository pagination exceeded configured page bound");
	}

	private HttpRequest request(String path) {
		String base = baseUri.toString();
		URI uri = URI.create((base.endsWith("/") ? base.substring(0, base.length() - 1) : base) + path);
		HttpRequest.Builder request = HttpRequest.newBuilder(uri).timeout(requestTimeout)
				.header("Accept", "application/json").GET();
		if (authorization != null) request.header("Authorization", authorization);
		return request.build();
	}

	private static final class ResponseTooLargeException extends IOException {
		private static final long serialVersionUID = 1L;
	}

	private static final class BoundedStringBodySubscriber implements HttpResponse.BodySubscriber<String> {
		private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		private final CompletableFuture<String> body = new CompletableFuture<>();
		private Flow.Subscription subscription;
		private int received;

		@Override public CompletionStage<String> getBody() { return body; }
		@Override public void onSubscribe(Flow.Subscription value) { subscription=value; subscription.request(1); }
		@Override public void onNext(List<ByteBuffer> chunks) {
			for(ByteBuffer chunk:chunks) {
				int length=chunk.remaining();
				if(received+length>MAX_RESPONSE_BYTES) {
					subscription.cancel();
					body.completeExceptionally(new ResponseTooLargeException());
					return;
				}
				byte[] buffer=new byte[length];chunk.get(buffer);bytes.write(buffer,0,buffer.length);received+=length;
			}
			subscription.request(1);
		}
		@Override public void onError(Throwable failure) { body.completeExceptionally(failure); }
		@Override public void onComplete() { body.complete(bytes.toString(StandardCharsets.UTF_8)); }
	}

	private static <T> Result<T> statusFailure(int status) {
		if (status == 404) return new Result<>(Status.NOT_FOUND, null, status, "repository resource not found");
		if (status == 401 || status == 403) return new Result<>(Status.UNAUTHORIZED, null, status, "repository authorization failed");
		if (status == 408 || status == 425 || status == 429 || status >= 500) {
			return new Result<>(Status.TRANSIENT_FAILURE, null, status, "repository temporarily unavailable");
		}
		return new Result<>(Status.INVALID_RESPONSE, null, status, "repository rejected the request");
	}

	private static <T> Result<T> found(T value) { return new Result<>(Status.FOUND, value, 200, ""); }
	private static <T> Result<T> invalid(String detail) { return new Result<>(Status.INVALID_RESPONSE, null, 0, detail); }
	private static <T> Result<T> transientFailure(String detail) { return new Result<>(Status.TRANSIENT_FAILURE, null, 0, detail); }

	private static URI validateBase(URI base) {
		Objects.requireNonNull(base, "baseUri");
		if (!("http".equalsIgnoreCase(base.getScheme()) || "https".equalsIgnoreCase(base.getScheme()))
				|| base.getHost() == null || base.getUserInfo() != null || base.getQuery() != null || base.getFragment() != null) {
			throw new IllegalArgumentException("repository base URI must be an HTTP(S) origin/path without credentials or query");
		}
		return base;
	}

	private static Duration requireBounded(Duration timeout, String name) {
		if (timeout == null || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(2)) > 0) {
			throw new IllegalArgumentException(name + " must be between 0 and 2 minutes");
		}
		return timeout;
	}

	private static String validateAuthorization(String authorization) {
		if (authorization == null) return null;
		if (!(authorization.startsWith("Bearer ") || authorization.startsWith("Basic "))
				|| authorization.contains("\r") || authorization.contains("\n")) {
			throw new IllegalArgumentException("authorization must be a valid bearer or basic header");
		}
		return authorization;
	}

	private static JsonNode parseObject(String json) {
		try {
			JsonNode value = PAGES.readTree(json);
			if (value == null || !value.isObject()) throw new IllegalArgumentException("JSON object required");
			return value;
		} catch (com.fasterxml.jackson.core.JsonProcessingException malformed) {
			throw new IllegalArgumentException("malformed JSON", malformed);
		}
	}
}
