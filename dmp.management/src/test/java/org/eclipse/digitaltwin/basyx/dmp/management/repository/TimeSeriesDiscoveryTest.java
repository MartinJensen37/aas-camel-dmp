/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.management.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.influxdb2.InfluxSettings;
import org.eclipse.digitaltwin.basyx.dmp.influxdb2.RecordColumn;
import org.eclipse.digitaltwin.basyx.dmp.influxdb2.configuration.InfluxDb2DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.influxdb2.configuration.InfluxDb2DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.ConnectorConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.ConnectorConfigurationFactory.DesiredBundle;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler;
import org.junit.jupiter.api.Test;

/**
 * A Sink on a TimeSeries LinkedSegment, compiled through discovery from the Variables submodel aas-model
 * generates ({@code timeseries/variables.json}): one series {@code State.Station} with fields PackMLState, Count
 * (column {@code count}) and Position.X, owned by {@code imaDispensing}.
 */
class TimeSeriesDiscoveryTest {
	private static final String VARIABLES = "https://smartproductionlab.aau.dk/aas/imaDispensing/submodels/Variables";
	private static final String AIMC_ID = "urn:fixture/submodels/AIMC";
	private static final String DATA_ID = "urn:fixture/submodels/Data";
	private static final String SEGMENT = "State.Station.Segments.Live";
	private static final InfluxSettings INFLUX = new InfluxSettings(URI.create("http://influxdb:8086"), "ap2030", "token");

	@Test
	void aSegmentSinkWritesTheSeriesRecordToItsMeasurement() throws Exception {
		try (var repository = fixture(variables())) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertTrue(bundle.valid(), bundle.issues().toString());
			var sink = assertInstanceOf(InfluxDb2DataSinkConfiguration.class, bundle.bundle().configuration().getDatasinks()
					.values().stream().filter(InfluxDb2DataSinkConfiguration.class::isInstance).findFirst().orElseThrow());
			assertEquals("Live", sink.getSinkId());
			assertEquals("station_state", sink.getMeasurement());
			assertEquals("ap2030", sink.getBucket());
			assertEquals(Map.of("asset", "imaDispensing"), sink.getTags());
			assertEquals(List.of(new RecordColumn("PackMLState", "PackMLState", "xs:string"),
					new RecordColumn("Count", "count", "xs:long"), new RecordColumn("Position.X", "Position.X", "xs:double")),
					sink.getColumns());
			assertTrue(sink.getConnectionURI().startsWith("dmp-influxdb2:station_state?"), sink.getConnectionURI());
			assertFalse(sink.getConnectionURI().contains("token"));
			assertTrue(bundle.desired().dependencies().contains(new MappingPlanCompiler.Dependency("submodel", VARIABLES, "State.Station")),
					bundle.desired().dependencies().toString());
		}
	}

	@Test
	void aChangeBesideTheSegmentIsRelevantButAnUnrelatedGroupIsNot() throws Exception {
		try (var repository = fixture(variables())) {
			var discovery = discovery(repository, INFLUX);
			discovery.scan();
			assertTrue(relevant(discovery, "State.Station.Metadata.Record.Count"), "the Record is the row shape");
			assertTrue(relevant(discovery, "State.Station.Segments.Live.DmpTarget.Measurement"));
			assertFalse(relevant(discovery, "Measurements.Weight"));
		}
	}

	@Test
	void withoutAConfiguredInfluxDbTheMappingIsBlocked() throws Exception {
		try (var repository = fixture(variables())) {
			DesiredBundle bundle = only(discovery(repository, null).scan());
			assertFalse(bundle.valid());
			assertTrue(issues(bundle).contains(InfluxSettings.URL), issues(bundle));
		}
	}

	@Test
	void aSegmentOnAnotherInfluxDbOrOrgIsBlocked() throws Exception {
		try (var repository = fixture(variables())) {
			var elsewhere = new InfluxSettings(URI.create("http://influxdb:8086"), "other-org", "token");
			DesiredBundle bundle = only(discovery(repository, elsewhere).scan());
			assertFalse(bundle.valid());
			assertTrue(issues(bundle).contains("does not name the configured InfluxDB"), issues(bundle));
		}
	}

	@Test
	void aRecordFieldWithoutAColumnBlocksTheMapping() throws Exception {
		// A qualifier typed "Column" is not enough: the Column is recognised by its semanticId.
		String withoutColumn = variables().replaceFirst("timeseries/Column/1/0", "timeseries/NotAColumn/1/0");
		try (var repository = fixture(withoutColumn)) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertFalse(bundle.valid());
			assertTrue(bundle.desired().failures().stream().anyMatch(failure -> failure.detail().contains("needs exactly one Column")),
					bundle.desired().failures().toString());
		}
	}

	@Test
	void aSegmentSourceReadsTheLatestRowAtItsPollingInterval() throws Exception {
		try (var repository = new FakeAasRepository().put(variables()).put(data())
				.put(aimc(segmentRef(), "2", refProperty(DATA_ID, "Value")))) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertTrue(bundle.valid(), issues(bundle));
			var source = assertInstanceOf(InfluxDb2DataSourceConfiguration.class, bundle.bundle().configuration().getDatasources()
					.values().stream().filter(InfluxDb2DataSourceConfiguration.class::isInstance).findFirst().orElseThrow());
			assertEquals("State", source.getSourceId());
			assertEquals(2000, source.getPollingIntervalMs());
			assertEquals("sync", source.getDelivery());
			assertEquals("station_state", source.getMeasurement());
			assertEquals(Map.of("asset", "imaDispensing"), source.getTags());
		}
	}

	@Test
	void aSegmentSourceWithoutAPollingIntervalIsBlocked() throws Exception {
		try (var repository = new FakeAasRepository().put(variables()).put(data())
				.put(aimc(segmentRef(), null, refProperty(DATA_ID, "Value")))) {
			DesiredBundle bundle = only(discovery(repository, INFLUX).scan());
			assertFalse(bundle.valid());
			assertTrue(issues(bundle).contains("needs a PollingInterval"), issues(bundle));
		}
	}

	private static FakeAasRepository fixture(String variables) throws IOException {
		return new FakeAasRepository().put(variables).put(data()).put(aimc(refProperty(DATA_ID, "Value"), segmentRef()));
	}

	private static RepositoryDiscovery discovery(FakeAasRepository repository, InfluxSettings influx) {
		return new RepositoryDiscovery(repository.client(), new ConnectorConfigurationFactory.RuntimeSettings(repository.uri(), influx));
	}

	private static DesiredBundle only(RepositoryDiscovery.Scan scan) {
		assertEquals(1, scan.mappings().size(), scan.toString());
		return scan.mappings().values().iterator().next().desired();
	}

	private static String issues(DesiredBundle bundle) {
		return bundle.issues().toString() + bundle.desired().failures();
	}

	private static boolean relevant(RepositoryDiscovery discovery, String path) {
		String shape = "{\"modelType\":\"Property\",\"idShort\":\"x\",\"valueType\":\"xs:string\",\"value\":\"\"}";
		return discovery.relevant(AasControlEvent.parse("{\"type\":\"SME_UPDATED\",\"id\":\"" + VARIABLES
				+ "\",\"smElementPath\":\"" + path + "\",\"submodelElement\":" + shape + "}"));
	}

	private static String variables() throws IOException {
		try (InputStream in = TimeSeriesDiscoveryTest.class.getResourceAsStream("/timeseries/variables.json")) {
			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static String segmentRef() {
		return "{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", VARIABLES) + ","
				+ String.join(",", java.util.Arrays.stream(SEGMENT.split("\\.")).map(id -> key("SubmodelElementCollection", id)).toList())
				+ "]}";
	}

	private static String refProperty(String submodel, String property) {
		return "{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", submodel) + "," + key("Property", property) + "]}";
	}

	private static String key(String type, String value) {
		return "{\"type\":\"" + type + "\",\"value\":\"" + value + "\"}";
	}

	private static String data() {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + DATA_ID + "\",\"idShort\":\"Data\",\"submodelElements\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"Value\",\"valueType\":\"xs:string\",\"value\":\"IDLE\"}]}";
	}

	private static String aimc(String source, String sink) {
		return aimc(source, null, sink);
	}

	private static String aimc(String source, String pollingInterval, String sink) {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + AIMC_ID + "\",\"idShort\":\"AIMC\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel\"}]},"
				+ "\"submodelElements\":[{\"modelType\":\"SubmodelElementList\",\"idShort\":\"MappingConfigurations\",\"typeValueListElement\":\"SubmodelElementCollection\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/1/0/MappingConfigurations\"}]},"
				+ "\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"state\",\"value\":["
				+ endpoint("Sources", "Source", "SourceId", "State", source, pollingInterval) + ","
				+ endpoint("Sinks", "Sink", "SinkId", "Live", sink, null)
				+ "]}]}]}";
	}

	private static String endpoint(String listName, String referenceName, String idName, String id, String reference,
			String pollingInterval) {
		String polling = pollingInterval == null ? ""
				: ",{\"modelType\":\"Property\",\"idShort\":\"PollingInterval\",\"valueType\":\"xs:string\",\"value\":\"" + pollingInterval + "\"}";
		return "{\"modelType\":\"SubmodelElementList\",\"idShort\":\"" + listName
				+ "\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"value\":["
				+ "{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + referenceName + "\",\"value\":" + reference + "},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"" + idName + "\",\"valueType\":\"xs:string\",\"value\":\"" + id + "\"}"
				+ polling + "]}]}";
	}
}
