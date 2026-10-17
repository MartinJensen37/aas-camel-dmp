package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.digitaltwin.basyx.dmp.opcua.metadata.OpcUaMethodMetadataProvider.Argument;
import org.eclipse.digitaltwin.basyx.dmp.opcua.metadata.OpcUaMethodMetadataProvider.MethodSignature;
import org.junit.jupiter.api.Test;

class CamelOpcUaMethodMetadataProviderTest {
	@Test
	void serviceLoaderFindsProtocolNeutralInvocationAdapter() {
		assertInstanceOf(OpcUaSynchronousInvocationAdapter.class,
				ServiceLoader.load(SynchronousInvocationAdapter.class).stream()
						.filter(provider -> provider.get().supports("dmp-milo-client:test"))
						.findFirst().orElseThrow().get());
	}

	@Test
	void readsAndCachesOrderedSignaturesByContextAndCommandUri() {
		AtomicInteger reads = new AtomicInteger();
		MethodSignature signature = new MethodSignature(
				List.of(new Argument("first", "ns=0;i=6", -1, List.of()),
						new Argument("second", "ns=0;i=11", 1, List.of(2L))),
				List.of(new Argument("result", "ns=0;i=1", -1, List.of())));
		CamelOpcUaMethodMetadataProvider provider = new CamelOpcUaMethodMetadataProvider((context, uri) -> {
			reads.incrementAndGet();
			return signature;
		});
		var context = new DefaultCamelContext();

		MethodSignature first = provider.read(context, "milo-client:opc.tcp://localhost:4840?node=RAW(ns=2;s=Object)&method=RAW(ns=2;s=Start)");
		assertSame(signature, first);
		assertSame(first, provider.read(context,
				"milo-client:opc.tcp://localhost:4840?node=RAW(ns=2;s=Object)&method=RAW(ns=2;s=Start)"));
		assertEquals(List.of("first", "second"), first.inputs().stream().map(Argument::name).toList());
		assertEquals(List.of("result"), first.outputs().stream().map(Argument::name).toList());
		assertEquals(1, reads.get());
	}

	@Test
	void convertsOrderedValuesToNativeVariantArray() {
		CamelOpcUaMethodMetadataProvider provider = new CamelOpcUaMethodMetadataProvider((context, uri) -> null);
		Object converted = provider.toVariantArray(List.of("start", 7, true), List.of(
				new Argument("state", "i=12", -1, List.of()),
				new Argument("count", "i=6", -1, List.of()),
				new Argument("enabled", "i=1", -1, List.of())));

		assertInstanceOf(Variant[].class, converted);
		Variant[] variants = (Variant[]) converted;
		assertArrayEquals(new Variant[] { new Variant("start"), new Variant(7), new Variant(true) }, variants);
	}

	@Test
	void convertsArrayValuedArgumentToTypedJavaArray() {
		CamelOpcUaMethodMetadataProvider provider = new CamelOpcUaMethodMetadataProvider((context, uri) -> null);
		Variant[] variants = (Variant[]) provider.toVariantArray(List.of(List.of(2L, 4L)),
				List.of(new Argument("positions", "i=6", 1, List.of(2L))));
		assertArrayEquals(new int[] { 2, 4 }, (int[]) variants[0].getValue());
	}

	@Test
	void rejectsIntegerNarrowingOutsideUaRanges() {
		CamelOpcUaMethodMetadataProvider provider = new CamelOpcUaMethodMetadataProvider((context, uri) -> null);
		assertThrows(IllegalArgumentException.class, () -> provider.toVariantArray(List.of(2147483648L),
				List.of(new Argument("count", "i=6", -1, List.of()))));
		assertThrows(IllegalArgumentException.class, () -> provider.toVariantArray(List.of(-1),
				List.of(new Argument("count", "i=3", -1, List.of()))));
		assertThrows(IllegalArgumentException.class, () -> provider.toVariantArray(List.of("2147483648"),
				List.of(new Argument("count", "i=6", -1, List.of()))));
		Variant[] valid = (Variant[]) provider.toVariantArray(List.of(2147483647L),
				List.of(new Argument("count", "i=6", -1, List.of())));
		assertEquals(Integer.MAX_VALUE, valid[0].getValue());
		assertThrows(IllegalArgumentException.class, () -> provider.toVariantArray(
				List.of(List.of(1, 2)), List.of(new Argument("count", "i=6", -1, List.of()))));
	}

	@Test
	void convertsRankTwoArraysToUaMatrices() {
		CamelOpcUaMethodMetadataProvider provider = new CamelOpcUaMethodMetadataProvider((context, uri) -> null);
		Variant[] variants = (Variant[]) provider.toVariantArray(
				List.of(List.of(List.of(1, 2), List.of(3, 4))),
				List.of(new Argument("matrix", "i=6", 2, List.of(2L, 2L))));
		org.eclipse.milo.opcua.stack.core.types.builtin.Matrix matrix =
				(org.eclipse.milo.opcua.stack.core.types.builtin.Matrix) variants[0].getValue();
		assertArrayEquals(new int[] { 2, 2 }, matrix.getDimensions());
		assertArrayEquals(new int[] { 1, 2, 3, 4 }, (int[]) matrix.getElements());
	}
}
