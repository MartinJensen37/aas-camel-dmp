package org.eclipse.digitaltwin.basyx.dmp.management.compiler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;

import org.eclipse.digitaltwin.basyx.dmp.management.model.AasFixtures;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel.Presence;
import org.junit.jupiter.api.Test;

class AidFormsTest {

	@Test
	void hrefsResolveAgainstTheBaseAsRfc3986Says() {
		assertEquals("mqtt://b:1883/site/st/DATA/State", AidForms.resolve("mqtt://b:1883/site/st/", "DATA/State").toString());
		assertEquals("mqtt://b:1883/site/DATA/State", AidForms.resolve("mqtt://b:1883/site/st", "DATA/State").toString());
		assertEquals("mqtt://b:1883/DATA/State", AidForms.resolve("mqtt://b:1883/site/st/", "/DATA/State").toString());
		assertEquals("mqtt://b:1883/DATA", AidForms.resolve("mqtt://b:1883", "DATA").toString());
		assertEquals("mqtt://b:1883//my/topic", AidForms.resolve("mqtt://b:1883/", ".//my/topic").toString());
		assertEquals("modbus+tcp://h:502/1/40089?quantity=2", AidForms.resolve("modbus+tcp://h:502/1/", "40089?quantity=2").toString());
		assertEquals("http://h/api/v1", AidForms.resolve("http://h/api/v1", "").toString());
		assertEquals("http://other/x", AidForms.resolve("http://h/api/", "http://other/x").toString());
		assertEquals("http://h/a/c", AidForms.resolve("http://h/a/b/", "../c").toString());
		assertThrows(IllegalArgumentException.class, () -> AidForms.resolve(null, "relative"));
		assertEquals("/alarms/#", AidForms.percentDecode("/alarms/%23"));
		assertEquals("a+b", AidForms.percentDecode("a+b"));
	}

	@Test
	void absentOpMeansTheTdDefaultsOfTheAffordance() {
		assertEquals(Set.of("readproperty", "writeproperty"), AidForms.operations(property(), form()));
		assertEquals(Set.of("readproperty", "observeproperty", "unobserveproperty"),
				AidForms.operations(property(AasFixtures.property("observable", "true"), AasFixtures.property("readOnly", "true")), form()));
		assertEquals(Set.of("writeproperty"), AidForms.operations(property(AasFixtures.property("writeOnly", "true")), form()));
		assertEquals(Set.of("invokeaction"), AidForms.operations(affordance("action"), form()));
		assertEquals(Set.of("subscribeevent", "unsubscribeevent"), AidForms.operations(affordance("event"), form()));
		assertEquals(Set.of("observeproperty"), AidForms.operations(property(), form("observeproperty")));
	}

	private static WireModel.Affordance property(org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement... terms) {
		return new WireModel.Affordance("property", Presence.of("p"), Presence.absent(), List.of(), Presence.absent(),
				Presence.absent(), Presence.absent(), AasFixtures.collection("p", terms));
	}

	private static WireModel.Affordance affordance(String kind) {
		return new WireModel.Affordance(kind, Presence.of("a"), Presence.absent(), List.of(), Presence.absent(),
				Presence.absent(), Presence.absent(), AasFixtures.collection("a"));
	}

	private static WireModel.Form form(String... operations) {
		return new WireModel.Form(Presence.of("x"), Presence.absent(), List.of(operations), Presence.absent(),
				java.util.Map.of(), Presence.absent(), java.util.Map.of(), AasFixtures.collection(null));
	}
}
