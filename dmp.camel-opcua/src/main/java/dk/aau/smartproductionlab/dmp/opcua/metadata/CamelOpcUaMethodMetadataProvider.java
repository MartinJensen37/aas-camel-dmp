package dk.aau.smartproductionlab.dmp.opcua.metadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.lang.reflect.Array;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.component.milo.client.MiloClientConnection;
import org.apache.camel.component.milo.client.MiloClientEndpoint;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.nodes.UaMethodNode;
import dk.aau.smartproductionlab.dmp.opcua.component.OpcUaStructures;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.Matrix;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;

/** Reads server-declared method signatures through Camel's configured Milo endpoint. */
public final class CamelOpcUaMethodMetadataProvider implements OpcUaMethodMetadataProvider {
	private static final long REFRESH_NANOS = Duration.ofMinutes(5).toNanos();

	private final ConcurrentMap<CacheKey, CacheEntry> cache = new ConcurrentHashMap<>();
	private final BiFunction<CamelContext, String, MethodSignature> reader;

	public CamelOpcUaMethodMetadataProvider() {
		this(CamelOpcUaMethodMetadataProvider::readFromEndpoint);
	}

	CamelOpcUaMethodMetadataProvider(BiFunction<CamelContext, String, MethodSignature> reader) {
		this.reader = Objects.requireNonNull(reader, "reader");
	}

	@Override
	public MethodSignature read(CamelContext context, String commandEndpointUri) {
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(commandEndpointUri, "commandEndpointUri");
		CacheKey key = new CacheKey(context, commandEndpointUri);
		long now = System.nanoTime();
		CacheEntry existing = cache.get(key);
		if (existing != null && now - existing.loadedAtNanos < REFRESH_NANOS) {
			return existing.signature;
		}

		MethodSignature signature = Objects.requireNonNull(reader.apply(context, commandEndpointUri),
				"metadata reader returned null");
		cache.put(key, new CacheEntry(signature, System.nanoTime()));
		return signature;
	}

	@Override
	public Object toVariantArray(List<Object> orderedValues, List<Argument> arguments) {
		Objects.requireNonNull(orderedValues, "orderedValues");
		if (orderedValues.size() != arguments.size()) {
			throw new IllegalArgumentException("ordered OPC UA values do not match argument definitions");
		}
		Variant[] variants = new Variant[orderedValues.size()];
		for (int i = 0; i < variants.length; i++) {
			variants[i] = new Variant(coerce(orderedValues.get(i), arguments.get(i)));
		}
		return variants;
	}

	private static Object coerce(Object value, Argument argument) {
		if (value == null) return null;
		int typeId = builtinTypeId(argument.dataType());
		// Structures, enumerations and other DataTypes are encoded by the producer, which holds the client.
		if (typeId < 1 || typeId > 12) return new OpcUaStructures.Pending(argument.dataType(), value);
		int rank = argument.valueRank();
		if (value instanceof List<?> || value.getClass().isArray()) {
			return arrayValue(value, typeId, rank, argument.arrayDimensions());
		}
		if (rank >= 1 || rank == 0) {
			throw new IllegalArgumentException("OPC UA array argument requires an array value: " + argument.name());
		}
		return scalar(value, typeId);
	}

	private static Object arrayValue(Object value, int typeId, int declaredRank, List<Long> expectedDimensions) {
		List<Object> flattened = new ArrayList<>();
		List<Integer> dimensions = new ArrayList<>();
		collectArray(value, 0, dimensions, flattened);
		int rank = dimensions.size();
		if (rank == 0 || declaredRank == -1 || declaredRank >= 1 && declaredRank != rank
				|| declaredRank == -3 && rank != 1) {
			throw new IllegalArgumentException("OPC UA array value rank does not match method signature");
		}
		for (int i = 0; i < expectedDimensions.size() && i < rank; i++) {
			long expected = expectedDimensions.get(i);
			if (expected > 0 && expected != dimensions.get(i)) {
				throw new IllegalArgumentException("OPC UA array dimension " + i + " expected "
						+ expected + " items but received " + dimensions.get(i));
			}
		}
		Object array = Array.newInstance(componentType(typeId), flattened.size());
		for (int i = 0; i < flattened.size(); i++) Array.set(array, i, scalar(flattened.get(i), typeId));
		if (rank == 1) return array;
		return new Matrix(array, dimensions.stream().mapToInt(Integer::intValue).toArray());
	}

	private static void collectArray(Object value, int depth, List<Integer> dimensions, List<Object> flattened) {
		List<?> values;
		if (value instanceof List<?> list) values = list;
		else if (value != null && value.getClass().isArray()) {
			int length = Array.getLength(value);
			List<Object> converted = new ArrayList<>(length);
			for (int i = 0; i < length; i++) converted.add(Array.get(value, i));
			values = converted;
		} else {
			flattened.add(value);
			return;
		}
		if (dimensions.size() == depth) dimensions.add(values.size());
		else if (dimensions.get(depth) != values.size()) {
			throw new IllegalArgumentException("OPC UA matrix arguments must be rectangular");
		}
		for (Object item : values) collectArray(item, depth + 1, dimensions, flattened);
	}

	private static int builtinTypeId(String dataType) {
		if (dataType == null) return -1;
		java.util.regex.Matcher matcher = java.util.regex.Pattern
				.compile("(?:ns=0;)?i=(\\d+)").matcher(dataType);
		if (!matcher.matches()) return -1;
		try { return Integer.parseInt(matcher.group(1)); }
		catch (NumberFormatException ignored) { return -1; }
	}

	private static Class<?> componentType(int id) {
		return switch (id) {
		case 1 -> boolean.class;
		case 2 -> byte.class;
		case 3 -> UByte.class;
		case 4 -> short.class;
		case 5 -> UShort.class;
		case 6 -> int.class;
		case 7 -> UInteger.class;
		case 8 -> long.class;
		case 9 -> ULong.class;
		case 10 -> float.class;
		case 11 -> double.class;
		case 12 -> String.class;
		default -> Object.class;
		};
	}

	private static Object scalar(Object value, int id) {
		if (value == null) return null;
		if (value instanceof Number n) {
			if (id >= 2 && id <= 9) {
				java.math.BigInteger integral;
				try {
					integral = new java.math.BigDecimal(n.toString()).toBigIntegerExact();
				} catch (NumberFormatException | ArithmeticException e) {
					throw new IllegalArgumentException("OPC UA integer argument is not an exact integer: " + value, e);
				}
				java.math.BigInteger min = switch (id) {
				case 2 -> java.math.BigInteger.valueOf(Byte.MIN_VALUE);
				case 3, 5, 7, 9 -> java.math.BigInteger.ZERO;
				case 4 -> java.math.BigInteger.valueOf(Short.MIN_VALUE);
				case 6 -> java.math.BigInteger.valueOf(Integer.MIN_VALUE);
				default -> java.math.BigInteger.valueOf(Long.MIN_VALUE);
				};
				java.math.BigInteger max = switch (id) {
				case 2 -> java.math.BigInteger.valueOf(Byte.MAX_VALUE);
				case 3 -> java.math.BigInteger.valueOf(0xffL);
				case 4 -> java.math.BigInteger.valueOf(Short.MAX_VALUE);
				case 5 -> java.math.BigInteger.valueOf(0xffffL);
				case 6 -> java.math.BigInteger.valueOf(Integer.MAX_VALUE);
				case 7 -> new java.math.BigInteger("4294967295");
			case 8 -> java.math.BigInteger.valueOf(Long.MAX_VALUE);
			default -> new java.math.BigInteger("18446744073709551615");
			};
				if (integral.compareTo(min) < 0 || integral.compareTo(max) > 0) {
					throw new IllegalArgumentException("OPC UA integer argument is outside the range for built-in type " + id
							+ ": " + value);
				}
				return switch (id) {
				case 2 -> integral.byteValue();
				case 3 -> UByte.valueOf(integral.shortValue());
				case 4 -> integral.shortValue();
				case 5 -> UShort.valueOf(integral.intValue());
				case 6 -> integral.intValue();
				case 7 -> UInteger.valueOf(integral.longValue());
				case 8 -> integral.longValue();
				case 9 -> ULong.valueOf(integral.toString());
			default -> value;
			};
			}
			if (id == 10) {
				float converted = n.floatValue();
				if (!Float.isFinite(converted)) throw new IllegalArgumentException("OPC UA Float argument is out of range: " + value);
				return converted;
			}
			if (id == 11) {
				double converted = n.doubleValue();
				if (!Double.isFinite(converted)) throw new IllegalArgumentException("OPC UA Double argument is out of range: " + value);
				return converted;
			}
		}
		if (id == 1) {
			if (value instanceof Boolean) return value;
			throw new IllegalArgumentException("OPC UA Boolean argument requires a boolean value: " + value);
		}
		if (id == 12) {
			if (value instanceof String) return value;
			throw new IllegalArgumentException("OPC UA String argument requires a string value: " + value);
		}
		if (id >= 2 && id <= 11) {
			throw new IllegalArgumentException("OPC UA numeric argument requires a numeric value: " + value);
		}
		return value;
	}

	private static MethodSignature readFromEndpoint(CamelContext context, String endpointUri) {
		Endpoint resolved = context.getEndpoint(endpointUri);
		if (!(resolved instanceof MiloClientEndpoint endpoint)) {
			throw new IllegalArgumentException("Command endpoint is not a Camel Milo client endpoint: " + endpointUri);
		}
		// The signature is the Method's alone; its owning Object (a node or a browse path) is the Call's concern.
		if (endpoint.getMethod() == null) {
			throw new IllegalArgumentException("Milo command endpoint must configure a method: " + endpointUri);
		}

		MiloClientConnection connection = endpoint.createConnection();
		try {
			OpcUaClient client = connection.getOpcUaClient();
			// The Method's NodeId, or its AID browse path.
			var methodId = dk.aau.smartproductionlab.dmp.opcua.component.OpcUaBrowsePaths.resolve(client, endpoint.getMethod());
			UaMethodNode method = (UaMethodNode) client.getAddressSpace().getNode(methodId);
			org.eclipse.milo.opcua.stack.core.types.structured.Argument[] inputs = arguments(method.readInputArgumentsAsync());
			org.eclipse.milo.opcua.stack.core.types.structured.Argument[] outputs = arguments(method.readOutputArgumentsAsync());
			return new MethodSignature(definitions(inputs), definitions(outputs));
		} catch (CompletionException e) {
			throw new IllegalStateException("Unable to read OPC UA method metadata from " + endpointUri, e);
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read OPC UA method metadata from " + endpointUri, e);
		} finally {
			endpoint.releaseConnection(connection);
		}
	}

	/**
	 * A Method's InputArguments or OutputArguments property. OPC UA makes both optional: a
	 * Method without arguments of a kind has no such property, which means none (not an error).
	 */
	static org.eclipse.milo.opcua.stack.core.types.structured.Argument[] arguments(
			java.util.concurrent.CompletableFuture<org.eclipse.milo.opcua.stack.core.types.structured.Argument[]> read) {
		try {
			return read.join();
		} catch (CompletionException failure) {
			for (Throwable cause = failure; cause != null; cause = cause.getCause() == cause ? null : cause.getCause())
				if (cause instanceof org.eclipse.milo.opcua.stack.core.UaException ua && ABSENT_PROPERTY.contains(ua.getStatusCode().getValue()))
					return new org.eclipse.milo.opcua.stack.core.types.structured.Argument[0];
			throw failure;
		}
	}

	private static final java.util.Set<Long> ABSENT_PROPERTY = java.util.Set.of(
			org.eclipse.milo.opcua.stack.core.StatusCodes.Bad_NotFound,
			org.eclipse.milo.opcua.stack.core.StatusCodes.Bad_NoMatch,
			org.eclipse.milo.opcua.stack.core.StatusCodes.Bad_NodeIdUnknown);

	private static List<Argument> definitions(org.eclipse.milo.opcua.stack.core.types.structured.Argument[] args) {
		List<Argument> definitions = new ArrayList<>(args == null ? 0 : args.length);
		if (args != null) {
			for (org.eclipse.milo.opcua.stack.core.types.structured.Argument argument : args) {
				List<Long> dimensions = argument.getArrayDimensions() == null ? List.of()
						: java.util.Arrays.stream(argument.getArrayDimensions()).map(value -> value.longValue()).toList();
				definitions.add(new Argument(argument.getName(), argument.getDataType().toParseableString(),
						argument.getValueRank(), dimensions));
			}
		}
		return definitions;
	}

	private record CacheKey(CamelContext context, String endpointUri) {
	}

	private record CacheEntry(MethodSignature signature, long loadedAtNanos) {
	}
}
