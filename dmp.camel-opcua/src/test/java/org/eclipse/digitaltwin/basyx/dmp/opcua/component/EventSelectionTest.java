package org.eclipse.digitaltwin.basyx.dmp.opcua.component;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.junit.jupiter.api.Test;

class EventSelectionTest {
	@Test
	void fieldsAreNamedBrowsePathsRelativeToTheEvent() {
		EventSelection selection = EventSelection.parse("Message,ToStateId=0:ToState/0:Id,Done=/2:Job/2:Done", "i=2378");
		List<EventSelection.Field> fields = selection.fields();
		assertEquals(List.of("Message", "ToStateId", "Done"), fields.stream().map(EventSelection.Field::name).toList());
		assertArrayEquals(new QualifiedName[] { new QualifiedName(0, "Message") }, fields.get(0).path());
		assertArrayEquals(new QualifiedName[] { new QualifiedName(0, "ToState"), new QualifiedName(0, "Id") }, fields.get(1).path());
		assertArrayEquals(new QualifiedName[] { new QualifiedName(2, "Job"), new QualifiedName(2, "Done") }, fields.get(2).path());
		assertEquals("i=2378", selection.eventType());
		assertNull(EventSelection.parse("Message", " ").eventType());
	}

	@Test
	void malformedSelectionsAreRejected() {
		assertThrows(IllegalArgumentException.class, () -> EventSelection.parse("", null));
		assertThrows(IllegalArgumentException.class, () -> EventSelection.parse("Message,Message", null));
		assertThrows(IllegalArgumentException.class, () -> EventSelection.parse("1st=0:Message", null));
		assertThrows(IllegalArgumentException.class, () -> EventSelection.parse("State=0:ToState//0:Id", null));
	}

	@Test
	void aNotificationMapsEachFieldToItsJsonValue() {
		EventSelection selection = EventSelection.parse("Message,Severity,Time,Source=0:SourceNode,Missing", null);
		DateTime time = DateTime.now();
		Map<String, Object> notification = selection.notification(new Variant[] {
				new Variant(LocalizedText.english("calibration done")), new Variant(Unsigned.ushort(500)),
				new Variant(time), new Variant(NodeId.parse("ns=1;s=Calibration")), Variant.NULL_VALUE });
		Map<String, Object> expected = new LinkedHashMap<>();
		expected.put("Message", "calibration done");
		expected.put("Severity", 500L);
		expected.put("Time", time.getJavaInstant().toString());
		expected.put("Source", "ns=1;s=Calibration");
		expected.put("Missing", null);
		assertEquals(expected, notification);
	}

	@Test
	void endpointTakesTheEventOptionsUpstreamDoesNotKnow() throws Exception {
		try (var camel = new DefaultCamelContext()) {
			var endpoint = (DmpMiloClientEndpoint) camel.getEndpoint("dmp-milo-client:opc.tcp://127.0.0.1:4840"
					+ "?node=RAW(i=2253)&eventFields=RAW(Message,ToStateId=0:ToState/0:Id)&eventType=RAW(i=2378)"
					+ "&requestedPublishingInterval=100");
			assertEquals("i=2253", endpoint.getNode());
			assertEquals(List.of("Message", "ToStateId"),
					endpoint.getEventSelection().fields().stream().map(EventSelection.Field::name).toList());
			assertEquals("i=2378", endpoint.getEventSelection().eventType());
			assertEquals(100d, endpoint.getConfiguration().getRequestedPublishingInterval());
			var variable = (DmpMiloClientEndpoint) camel.getEndpoint("dmp-milo-client:opc.tcp://127.0.0.1:4840?node=RAW(ns=1;s=X)");
			assertSame(null, variable.getEventSelection());
			assertThrows(Exception.class, () -> camel.getEndpoint(
					"dmp-milo-client:opc.tcp://127.0.0.1:4840?node=RAW(i=2253)&eventType=RAW(i=2378)"));
		}
	}
}
