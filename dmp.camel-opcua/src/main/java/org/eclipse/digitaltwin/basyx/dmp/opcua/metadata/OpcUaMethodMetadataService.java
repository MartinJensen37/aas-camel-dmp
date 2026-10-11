package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.nodes.UaMethodNode;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.structured.Argument;

/**
 * Reads OPC UA method argument metadata and caches it by server, object, and method NodeId.
 * Successful values expire five minutes after loading by default. Failed reads are not cached.
 */
public final class OpcUaMethodMetadataService {
	public static final Duration DEFAULT_REFRESH_AFTER = Duration.ofMinutes(5);

	@FunctionalInterface
	interface MetadataLoader {
		MethodMetadata load(String serverUrl, NodeId objectNodeId, NodeId methodNodeId);
	}

	private final ConcurrentMap<CacheKey, CacheEntry> cache = new ConcurrentHashMap<>();
	private final long refreshNanos;
	private final LongSupplier nanoTime;
	private final MetadataLoader loader;

	public OpcUaMethodMetadataService() {
		this(DEFAULT_REFRESH_AFTER);
	}

	public OpcUaMethodMetadataService(Duration refreshAfter) {
		this(refreshAfter, System::nanoTime, OpcUaMethodMetadataService::readFromServer);
	}

	OpcUaMethodMetadataService(Duration refreshAfter, LongSupplier nanoTime, MetadataLoader loader) {
		Objects.requireNonNull(refreshAfter, "refreshAfter");
		if (refreshAfter.isNegative() || refreshAfter.isZero()) {
			throw new IllegalArgumentException("refreshAfter must be positive");
		}
		this.refreshNanos = refreshAfter.toNanos();
		this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
		this.loader = Objects.requireNonNull(loader, "loader");
	}

	/** Returns cached metadata or reads it from the server if absent or expired. */
	public MethodMetadata getMethodMetadata(String serverUrl, NodeId objectNodeId, NodeId methodNodeId) {
		CacheKey key = new CacheKey(serverUrl, objectNodeId, methodNodeId);
		long now = nanoTime.getAsLong();
		CacheEntry entry = cache.get(key);
		if (entry != null && now - entry.loadedAtNanos < refreshNanos) {
			return entry.metadata;
		}
		return loadAndCache(key);
	}

	/** Reads fresh metadata immediately and replaces any cached value for this method. */
	public MethodMetadata refresh(String serverUrl, NodeId objectNodeId, NodeId methodNodeId) {
		return loadAndCache(new CacheKey(serverUrl, objectNodeId, methodNodeId));
	}

	/** Removes cached metadata for exactly this server/object/method tuple. */
	public void invalidate(String serverUrl, NodeId objectNodeId, NodeId methodNodeId) {
		cache.remove(new CacheKey(serverUrl, objectNodeId, methodNodeId));
	}

	/** Removes all cached method metadata. */
	public void invalidateAll() {
		cache.clear();
	}

	private MethodMetadata loadAndCache(CacheKey key) {
		MethodMetadata metadata = Objects.requireNonNull(
				loader.load(key.serverUrl, key.objectNodeId, key.methodNodeId), "loader returned null");
		cache.put(key, new CacheEntry(metadata, nanoTime.getAsLong()));
		return metadata;
	}

	private static MethodMetadata readFromServer(String serverUrl, NodeId objectNodeId, NodeId methodNodeId) {
		Objects.requireNonNull(serverUrl, "serverUrl");
		Objects.requireNonNull(objectNodeId, "objectNodeId");
		Objects.requireNonNull(methodNodeId, "methodNodeId");
		OpcUaClient client = null;
		try {
			client = OpcUaClient.create(serverUrl);
			client.connect();
			UaMethodNode method = (UaMethodNode) client.getAddressSpace().getNode(methodNodeId);
			Argument[] inputs = method.readInputArgumentsAsync().join();
			Argument[] outputs = method.readOutputArgumentsAsync().join();
			return new MethodMetadata(definitions(inputs), definitions(outputs));
		} catch (UaException | CompletionException e) {
			throw new IllegalStateException("Unable to read OPC UA method metadata from " + serverUrl
					+ " for method " + methodNodeId, e);
		} finally {
			if (client != null) {
				try {
					client.disconnect();
				} catch (UaException ignored) {
					// Preserve the read failure, if any; disconnect is best-effort.
				}
			}
		}
	}

	private static List<MethodArgumentDefinition> definitions(Argument[] arguments) {
		List<MethodArgumentDefinition> definitions = new ArrayList<>(arguments == null ? 0 : arguments.length);
		if (arguments != null) {
			for (Argument argument : arguments) {
				definitions.add(new MethodArgumentDefinition(argument.getName(), argument.getDataType(),
						argument.getValueRank(), argument.getArrayDimensions() == null
								? List.of() : List.of(argument.getArrayDimensions())));
			}
		}
		return definitions;
	}

	private record CacheKey(String serverUrl, NodeId objectNodeId, NodeId methodNodeId) {
		private CacheKey {
			Objects.requireNonNull(serverUrl, "serverUrl");
			Objects.requireNonNull(objectNodeId, "objectNodeId");
			Objects.requireNonNull(methodNodeId, "methodNodeId");
		}
	}

	private record CacheEntry(MethodMetadata metadata, long loadedAtNanos) {
	}
}
