package org.eclipse.digitaltwin.basyx.dmp.management.compiler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultFile;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList;
import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AASDatasinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AASPollingDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.operation.ProtocolBridgeRouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.HttpDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.HttpPollingDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.LuaTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.MqttDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.MqttDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.Plc4xDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.Plc4xDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.AidTarget;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.DirectTarget;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.EndpointPlan;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.MappingPlan;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.Plan;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.basyx.dmp.management.model.AasFixtures;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel.Presence;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasReferenceResolver;
import org.junit.jupiter.api.Test;

class ConnectorConfigurationFactoryTest {
	private final ConnectorConfigurationFactory factory = new ConnectorConfigurationFactory();

	@Test
	void projectsHttpEndpointsAndManagedLuaWithoutPuttingScriptInUri() {
		MappingPlan plan = mapping(WireModel.ProtocolFamily.HTTP, true);
		var result = factory.create(new Plan(List.of(plan)), "owner/mapping-1");

		assertTrue(result.issues().isEmpty());
		var source = assertInstanceOf(HttpPollingDataSourceConfiguration.class,
				result.configuration().getDatasources().values().iterator().next());
		var sink = assertInstanceOf(HttpDataSinkConfiguration.class,
				result.configuration().getDatasinks().values().iterator().next());
		assertEquals("Source.authored", source.getSourceId());
		assertEquals("Sink.authored", sink.getSinkId());
		assertEquals(List.of(source.getUniqueId()), result.configuration().getRoutes().get(0).getDatasources());
		var route = assertInstanceOf(ExecutionPolicyRouteConfiguration.class, result.configuration().getRoutes().get(0));
		assertEquals("owner/mapping-1:3", route.getRouteId());
		var transformer = assertInstanceOf(LuaTransformerConfiguration.class,
				result.configuration().getTransformers().values().iterator().next());
		assertTrue(transformer.getConnectionURI().contains("managed:"));
		assertFalse(transformer.getConnectionURI().contains("aimc_main"));
		assertTrue(transformer.isKeyedInput());
	}

	@Test
	void emitsOneStableOwnedConfigurationPerMapping() {
		var first=mappingPlan("map-one",0,
				new EndpointPlan("source-one",reference(),target(WireModel.ProtocolFamily.HTTP,"GET",Presence.absent())),
				new EndpointPlan("sink-one",reference(),target(WireModel.ProtocolFamily.HTTP,"POST",Presence.absent())));
		var second=mappingPlan("map-two",1,
				new EndpointPlan("source-two",reference(),target(WireModel.ProtocolFamily.HTTP,"GET",Presence.absent())),
				new EndpointPlan("sink-two",reference(),target(WireModel.ProtocolFamily.HTTP,"POST",Presence.absent())));
		var plan=new Plan("urn:owner/submodels/AIMC",List.of(first,second));
		var bundles=factory.createBundles(plan,null);
		assertTrue(bundles.issues().isEmpty());
		assertEquals(2,bundles.bundles().size());
		assertEquals("map-one",bundles.bundles().get(0).mappingIdentity());
		assertEquals("map-two",bundles.bundles().get(1).mappingIdentity());
		assertFalse(bundles.bundles().get(0).bundleId().equals(bundles.bundles().get(1).bundleId()));
		for(var bundle:bundles.bundles()) {
			assertEquals(1,bundle.configuration().getRoutes().size());
			assertEquals(1,bundle.configuration().getDatasources().size());
			assertEquals(1,bundle.configuration().getDatasinks().size());
		}
	}

	@Test
	void reportsUnsupportedFamilyWithPathAndMappingIdentity() {
		var result = factory.create(new Plan(List.of(mapping(WireModel.ProtocolFamily.BACNET, false))), "route");
		assertEquals(2, result.issues().size());
		assertTrue(result.issues().stream().allMatch(issue -> issue.mappingId().equals("logical-map")));
		assertTrue(result.issues().stream().allMatch(issue -> issue.path().contains("Sources") || issue.path().contains("Sinks")));
		assertTrue(result.configuration().getRoutes().isEmpty());
	}

	@Test
	void projectsMqttBaseRelativeTopicsAndDeclaredQualityTerms() {
		var endpointMetadata = AasFixtures.collection("EndpointMetadata", property("base", "mqtt://broker.example:1883/base"));
		var sourceForm = mqttForm("/state", "0", null);
		var sinkForm = mqttForm("/command", "1", "true");
		var sourceTarget = mqttTarget(endpointMetadata, sourceForm, "State");
		var sinkTarget = mqttTarget(endpointMetadata, sinkForm, "Command");
		var source = new EndpointPlan("State", reference(), sourceTarget);
		var sink = new EndpointPlan("Command", reference(), sinkTarget);
		var mapping = new MappingPlan("Fan", 0, List.of(source), List.of(sink), Presence.absent(),
				Presence.absent(), Presence.absent(), Presence.absent(), Presence.absent(), Presence.absent(), List.of());
		var result = factory.create(new Plan("urn:owner/submodels/AIMC", List.of(mapping)), "unit");

		assertTrue(result.issues().isEmpty());
		var dataSource = assertInstanceOf(MqttDataSourceConfiguration.class,
				result.configuration().getDatasources().values().iterator().next());
		var dataSink = assertInstanceOf(MqttDataSinkConfiguration.class,
				result.configuration().getDatasinks().values().iterator().next());
		assertEquals("broker.example", dataSource.getServerUrl());
		assertEquals(1883, dataSource.getServerPort());
		assertEquals("base/state", dataSource.getTopic());
		assertEquals(0, dataSource.getQos());
		assertEquals("base/command", dataSink.getTopic());
		assertEquals(1, dataSink.getQos());
		assertEquals(true, dataSink.getRetained());
		assertTrue(dataSource.getConnectionURI().contains("clientId=dmp-"));
	}

	@Test
	void resolvesRelativeHttpFormsAgainstTheirAuthoredEndpointBase() {
		var source=endpointPlan("read",protocolTarget(WireModel.ProtocolFamily.HTTP,
				"http://api.example:8080/api/v1","/temperature",Map.of(),"GET"));
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,
				"http://api.example:8080/api/v1","/alarm",Map.of(),"POST"));
		var result=factory.create(new Plan(List.of(mappingWithEndpoints(source,sink))),"route");
		assertTrue(result.issues().isEmpty());
		var httpSource=assertInstanceOf(HttpPollingDataSourceConfiguration.class,
				result.configuration().getDatasources().values().iterator().next());
		assertEquals("http://api.example:8080/api/v1/temperature",httpSource.getServerUrl());
		var httpSink=assertInstanceOf(HttpDataSinkConfiguration.class,
				result.configuration().getDatasinks().values().iterator().next());
		assertEquals("api.example",httpSink.getServerUrl());
		assertEquals("/api/v1/alarm",httpSink.getPath());
	}

	@Test
	void projectsOpcUaNodeAndModbusTagDetailsFromTypedBindingTerms() {
		var uaSource = endpointPlan("ua-read", protocolTarget(WireModel.ProtocolFamily.OPCUA,
				"opc.tcp://ua.example:4840/milo", "/?id=ns=2;s=Temperature", Map.of(), null));
		var uaSink = endpointPlan("ua-write", protocolTarget(WireModel.ProtocolFamily.OPCUA,
				"opc.tcp://ua.example:4840/milo", "/?id=ns=2;s=Temperature", Map.of(), null));
		var modbusSource = endpointPlan("mb-read", protocolTarget(WireModel.ProtocolFamily.MODBUS,
				"tcp://plc.example:502/service?unit=2", "/coil:1", Map.of("modv_entity", "coil:1"), null));
		var modbusSink = endpointPlan("mb-write", protocolTarget(WireModel.ProtocolFamily.MODBUS,
				"tcp://plc.example:502/service?unit=2", "/coil:1", Map.of("modv_entity", "coil:1"), null));
		var plan = new Plan(List.of(mappingPlan("ua-map",0,uaSource,uaSink),
				mappingPlan("mb-map",1,modbusSource,modbusSink)));
		var result = factory.create(plan, "route");
		assertTrue(result.issues().isEmpty());
		var uaDataSource = assertInstanceOf(OpcuaDataSourceConfiguration.class,
				result.configuration().getDatasources().values().stream()
						.filter(OpcuaDataSourceConfiguration.class::isInstance).findFirst().orElseThrow());
		assertEquals("ua.example", uaDataSource.getServerUrl());
		assertEquals("milo", uaDataSource.getPathToService());
		assertEquals("ns=2;s=Temperature", uaDataSource.getNodeInformation());
		var uaDataSink = assertInstanceOf(OpcuaDataSinkConfiguration.class,
				result.configuration().getDatasinks().values().stream()
						.filter(OpcuaDataSinkConfiguration.class::isInstance).findFirst().orElseThrow());
		assertEquals("ns=2;s=Temperature", uaDataSink.getNodeInformation());
		var plcSource = assertInstanceOf(Plc4xDataSourceConfiguration.class,
				result.configuration().getDatasources().values().stream()
						.filter(Plc4xDataSourceConfiguration.class::isInstance).findFirst().orElseThrow());
		assertEquals("modbus-tcp", plcSource.getDriver());
		assertEquals("service", plcSource.getServicePath());
		assertEquals("unit", plcSource.getOptions().get(0).getName());
		assertEquals("2", plcSource.getOptions().get(0).getValue());
		assertEquals("coil:1", plcSource.getTags().get(0).getValue());
		var plcSink = assertInstanceOf(Plc4xDataSinkConfiguration.class,
				result.configuration().getDatasinks().values().stream()
						.filter(Plc4xDataSinkConfiguration.class::isInstance).findFirst().orElseThrow());
		assertEquals("coil:1", plcSink.getTags().get(0).getValue());
	}

	@Test
	void rejectsAnAuthoredModbusFunctionNotRepresentedByTheConnectorConfiguration() {
		var modbus=endpointPlan("mb",protocolTarget(WireModel.ProtocolFamily.MODBUS,
				"tcp://plc.example:502", "/coil:1", Map.of("modv_function", "read-holding"), null));
		var result=factory.create(new Plan(List.of(mappingWithEndpoints(modbus,modbus))),"route");
		assertFalse(result.issues().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.path().contains("modv_function")));
		assertTrue(result.configuration().getRoutes().isEmpty());
	}

	@Test
	void mapsScalarBacnetBindingsToThePlc4xBacnetIpDriver() {
		var source=endpointPlan("present-value",protocolTarget(WireModel.ProtocolFamily.BACNET,
				"bacnet://bacnet.example:47808", "/analog-input:1", Map.of(), null));
		var sink=endpointPlan("present-value",protocolTarget(WireModel.ProtocolFamily.BACNET,
				"bacnet://bacnet.example:47808", "/analog-output:2", Map.of(), null));
		var result=factory.create(new Plan(List.of(mappingPlan("bacnet-map",0,source,sink))),"route");
		assertTrue(result.issues().isEmpty());
		var dataSource=assertInstanceOf(Plc4xDataSourceConfiguration.class,
				result.configuration().getDatasources().values().iterator().next());
		assertEquals("bacnet-ip",dataSource.getDriver());
		assertEquals(47808,dataSource.getServerPort());
		assertEquals("/analog-input:1",dataSource.getTags().get(0).getValue());
		var dataSink=assertInstanceOf(Plc4xDataSinkConfiguration.class,
				result.configuration().getDatasinks().values().iterator().next());
		assertEquals("bacnet-ip",dataSink.getDriver());
		assertEquals("/analog-output:2",dataSink.getTags().get(0).getValue());
	}

	@Test
	void mapsIolinkRestBindingsThroughTheHttpConnector() {
		var source=endpointPlan("input",protocolTarget(WireModel.ProtocolFamily.IOLINK,
				"http://iolink.example:8080/api", "/input",Map.of(),"GET"));
		var sink=endpointPlan("output",protocolTarget(WireModel.ProtocolFamily.IOLINK,
				"http://iolink.example:8080/api", "/output",Map.of(),"POST"));
		var result=factory.create(new Plan(List.of(mappingPlan("iolink-map",0,source,sink))),"route");
		assertTrue(result.issues().isEmpty());
		assertInstanceOf(HttpPollingDataSourceConfiguration.class,
				result.configuration().getDatasources().values().iterator().next());
		assertInstanceOf(HttpDataSinkConfiguration.class,
				result.configuration().getDatasinks().values().iterator().next());
	}

	@Test
	void directAasTargetsRequireAndUseExplicitRepositoryRuntimeSettings() {
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),
				property("State","RUNNING")));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","Property","State");
		var source=new EndpointPlan("PackMLState",reference,target,Presence.of("2"));
		var sink=new EndpointPlan("OccupationState",reference,target);
		var plan=new Plan(List.of(mappingPlan("aas-map",0,source,sink)));
		var noServer=factory.create(plan,"route");
		assertTrue(noServer.configuration().getRoutes().isEmpty());
		assertTrue(noServer.issues().stream().anyMatch(issue -> issue.message().contains("explicit repository runtime settings")));

		var result=factory.create(plan,"route",new ConnectorConfigurationFactory.RuntimeSettings(
				URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty());
		var ds=assertInstanceOf(AASPollingDataSourceConfiguration.class,
				result.configuration().getDatasources().values().iterator().next());
		String encoded=java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
				"urn:owner/submodels/Variables".getBytes(StandardCharsets.UTF_8));
		assertEquals("http://aas.example:8081/submodels/"+encoded,ds.getSubmodelEndpoint());
		assertEquals("State",ds.getIdShortPath());
		assertEquals(2000,ds.getPollingIntervalMs(),"an authored PollingInterval keeps polling");
		var sinkConfig=assertInstanceOf(AASDatasinkConfiguration.class,
				result.configuration().getDatasinks().values().iterator().next());
		assertEquals("OccupationState",sinkConfig.getSinkId());
		assertEquals("OccupationState",sinkConfig.getValuePath());
	}

	@Test
	void aasPropertySourcesWithoutPollingIntervalFollowRepositoryEvents() {
		var collection=AasFixtures.collection("Status",property("State","RUNNING"));
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),collection.getValue().get(0)));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementCollection","Status","Property","State");
		var plan=new Plan(List.of(mappingPlan("aas-event",0,new EndpointPlan("PackMLState",reference,target),
				new EndpointPlan("OccupationState",reference,target))));
		var result=factory.create(plan,"route",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var ds=assertInstanceOf(org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AasEventDataSourceConfiguration.class,
				result.configuration().getDatasources().values().iterator().next());
		assertEquals("urn:owner/submodels/Variables",ds.getSubmodelId());
		assertEquals("Status.State",ds.getIdShortPath());
		assertEquals("async",ds.getDelivery());
		assertEquals("aas-element-event",ds.getSourceKind());
		assertTrue(ds.getConnectionURI().startsWith("aas-event:"));
		String encoded=java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
				"urn:owner/submodels/Variables".getBytes(StandardCharsets.UTF_8));
		assertEquals("http://aas.example:8081/submodels/"+encoded+"/submodel-elements/Status.State",ds.getEventFilter().get("readUrl"));
		assertEquals(Map.of("type","string"),ds.getSchema());
	}

	@Test
	void bridgeRestPathIsTheDelegationQualifierPathAndAMissingQualifierBlocksTheMapping() {
		var result=factory.create(explicitResponsePlan("aid-command"),"bridge",new ConnectorConfigurationFactory.RuntimeSettings(
				URI.create("http://aas.example:8081")));
		var bridge=assertInstanceOf(ProtocolBridgeRouteConfiguration.class,result.configuration().getRoutes().get(0));
		assertEquals("/operations/asset%20one/Move",bridge.getTriggerData().get(ProtocolBridgeRouteConfiguration.REST_PATH));

		var plan=explicitResponsePlan("aid-command");
		var operation=(org.eclipse.digitaltwin.aas4j.v3.model.Operation)((DirectTarget)plan.mappings().get(0).sources().get(0).target())
				.resolved().element();
		for (String invalid : new String[] { null, "", "/operations/asset/Move", "http://dmp:8080/operations/{aas_id_short}/Move",
				"http://dmp:8080/" }) {
			operation.setQualifiers(invalid==null?List.of():List.of(AasFixtures.delegation(invalid)));
			var blocked=factory.create(plan,"bridge",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
			assertTrue(blocked.configuration().getRoutes().isEmpty(),String.valueOf(invalid));
			assertTrue(blocked.issues().stream().anyMatch(issue -> issue.message().contains("invocationDelegation")),String.valueOf(invalid));
		}
	}

	@Test
	void typedPropertyListsAreTypedEvenWhenEmpty() {
		var list=new org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList.Builder().idShort("Readings")
				.typeValueListElement(org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements.PROPERTY)
				.valueTypeListElement(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.xsd("xs:double")).build();
		var target=new DirectTarget(new AasReferenceResolver.ResolvedElement(
				AasFixtures.submodel("urn:owner/submodels/Variables","Variables"),list));
		var reference=AasFixtures.reference("Submodel","urn:owner/submodels/Variables","SubmodelElementList","Readings");
		var source=new EndpointPlan("Readings",reference,target,Presence.of("1"));
		var plan=new Plan(List.of(mappingPlan("list",0,source,new EndpointPlan("Out",reference,target))));
		var result=factory.create(plan,"route",new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		assertEquals(Map.of("type","array","items",Map.of("type","number")),
				result.configuration().getDatasources().values().iterator().next().getSchema());
	}

	@Test void fingerprintsAreDeterministicSecretSafeAndCredentialSensitiveInternally() {
		var first=credentialsPlan("one-password");var rotated=credentialsPlan("another-password");
		var a=factory.createBundles(first,null).bundles().get(0).fingerprint();
		var repeated=factory.createBundles(first,null).bundles().get(0).fingerprint();
		var b=factory.createBundles(rotated,null).bundles().get(0).fingerprint();
		assertEquals(a.exported(),repeated.exported());assertTrue(a.sameEffectiveConfiguration(repeated));
		assertEquals(a.exported(),b.exported(),"exported fingerprints must not provide a credential-derived oracle");
		assertFalse(a.sameEffectiveConfiguration(b),"credential rotation still changes internal identity");
		assertEquals(a.exported(),a.toString());
		String json=new com.google.gson.Gson().toJson(a);
		assertFalse(json.contains("internal"));assertFalse(json.contains("password"));
	}

	@Test void authoredMappingIdentitySurvivesListReorderingAndEffectiveOptionsChangeFingerprint() {
		var first=mapping(WireModel.ProtocolFamily.HTTP,true);
		var relocated=new MappingPlan(first.identity(),17,first.sources(),first.sinks(),first.defaultPollingInterval(),
				first.transformation(),first.responseSources(),first.responseSinks(),first.responseTransformation(),first.triggerSourceIds(),first.qualifiers());
		var a=factory.createBundles(new Plan(List.of(first)),null).bundles().get(0);
		var b=factory.createBundles(new Plan(List.of(relocated)),null).bundles().get(0);
		assertEquals(a.bundleId(),b.bundleId());assertEquals(a.routeId(),b.routeId());
		assertTrue(a.fingerprint().sameEffectiveConfiguration(b.fingerprint()));
		var interval=new MappingPlan(first.identity(),first.listIndex(),first.sources(),first.sinks(),
				Presence.of("7"),first.transformation(),first.responseSources(),first.responseSinks(),
				first.responseTransformation(),first.triggerSourceIds(),first.qualifiers());
		var changed=factory.createBundles(new Plan(List.of(interval)),null).bundles().get(0);
		assertFalse(a.fingerprint().sameEffectiveConfiguration(changed.fingerprint()));
		assertFalse(a.fingerprint().exported().equals(changed.fingerprint().exported()));
	}

	@Test void defaultPollingDoesNotConvertMqttSubscriptionToQueueSampling() {
		var source=endpointPlan("mqtt",protocolTarget(WireModel.ProtocolFamily.MQTT,"mqtt://broker.example:1883","/state",Map.of(),null));
		var sink=endpointPlan("http",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		var map=mappingPlan("fan",0,source,sink);
		map=new MappingPlan(map.identity(),0,map.sources(),map.sinks(),Presence.of("2"),
				map.transformation(),map.responseSources(),map.responseSinks(),map.responseTransformation(),map.triggerSourceIds(),map.qualifiers());
		var result=factory.create(new Plan(List.of(map)),"route");
		assertTrue(result.issues().isEmpty());
		var configured=result.configuration().getDatasources().values().iterator().next();
		assertEquals("async",configured.getDelivery());assertEquals(0,configured.getPollingIntervalMs());
	}

	@Test void authoredUnknownProtocolOptionsBlockOnlyTheirMapping() {
		var bad=endpointPlan("bad",protocolTarget(WireModel.ProtocolFamily.OPCUA,"opc.tcp://ua.example:4840/service", "ns=2;s=Value",
				Map.of("uav_securityPolicy","unsupported"),null));
		var invalid=mappingPlan("blocked",0,bad,bad);
		var valid=mappingPlan("valid",1,endpointPlan("read",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"GET")),
				endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST")));
		var result=factory.createBundles(new Plan(List.of(invalid,valid)),null);
		assertEquals(1,result.bundles().size());assertEquals("valid",result.bundles().get(0).mappingIdentity());
		assertTrue(result.issues().stream().allMatch(issue -> issue.mappingId().equals("blocked")));
		assertTrue(result.issues().stream().anyMatch(issue -> issue.path().endsWith("uav_securityPolicy")));
	}

	private static Plan credentialsPlan(String password) {
		var source=endpointPlan("read",protocolTarget(WireModel.ProtocolFamily.HTTP,
				"http://alice:"+password+"@api.example:8080", "/state",Map.of(),"GET"));
		var sink=endpointPlan("write",protocolTarget(WireModel.ProtocolFamily.HTTP,"http://api.example:8080","/state",Map.of(),"POST"));
		return new Plan(List.of(mappingPlan("credentials",0,source,sink)));
	}

	@Test void compilesExactActionOutputProjectionCallerReplyAndGroupedExtraWrites() {
		var plan=explicitResponsePlan("aid-command");
		var result=factory.create(plan,"bridge",new ConnectorConfigurationFactory.RuntimeSettings(
				URI.create("http://aas.example:8081")));
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=assertInstanceOf(ProtocolBridgeRouteConfiguration.class,result.configuration().getRoutes().get(0));
		assertTrue(bridge.isExplicitResponseMapping());
		assertEquals("Reply",bridge.getCallerReplySinkId());
		assertEquals(List.of(Map.of("sourceId","Body,&#","valuePath","$.Count")),bridge.getResponseSourceBindings());
		assertEquals(1,bridge.getTransformers().size(),"response Lua must not execute in the request chain");
		assertEquals(2,result.configuration().getTransformers().size());
		assertFalse(bridge.getTransformers().contains(bridge.getResponseTransformer()));
		var request=(LuaTransformerConfiguration)result.configuration().getTransformers().get(bridge.getTransformers().get(0));
		var response=(LuaTransformerConfiguration)result.configuration().getTransformers().get(bridge.getResponseTransformer());
		assertTrue(request.isKeyedInput());assertTrue(response.isKeyedInput());
		assertFalse(response.getConnectionURI().contains("sourceKeys="));
		assertEquals(Map.of("Count","integer"),bridge.getResponseTypes());
		assertEquals(Map.of("Count","xs:string"),bridge.getTriggerData().get("callerResponseTypes"));
		assertEquals(2,bridge.getResponseSinkBindings().size(),"caller Operation is a reply, not another device write");
		assertEquals(bridge.getResponseSinkBindings().get(0).get("payloadGroup"),bridge.getResponseSinkBindings().get(1).get("payloadGroup"));
		assertEquals("$.x",bridge.getResponseSinkBindings().get(0).get("valuePath"));
		assertEquals("$.y",bridge.getResponseSinkBindings().get(1).get("valuePath"));
		assertEquals(3,result.configuration().getDatasinks().size(),"one action target plus grouped response members");
	}

	@Test void scalarCciInoutputsAreCallerInputsAndOutputs() {
		var limit=AasFixtures.property("Limit","xs:int",null);
		var inoutputs=List.of(AasFixtures.variable(property("Uuid","id")),AasFixtures.variable(property("Trace","trace")),
				AasFixtures.variable(limit));
		var result=factory.create(explicitResponsePlan("aid-command",inoutputs),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(ProtocolBridgeRouteConfiguration)result.configuration().getRoutes().get(0);
		assertEquals(Map.of("Uuid","xs:string","Trace","xs:string","Limit","xs:int","Count","xs:string"),
				bridge.getTriggerData().get("callerResponseTypes"));
		assertEquals(List.of("Uuid","Trace","Limit","Count"),bridge.getTriggerData().get("callerOutputOrder"));
	}

	@Test void structuredCciVariablesAreDeclaredTemplates() {
		var position=AasFixtures.collection("Target",AasFixtures.property("x","xs:double",null),
				AasFixtures.property("y","xs:double",null));
		var waypoints=new DefaultSubmodelElementList.Builder().idShort("Waypoints")
				.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
				.value(List.of(AasFixtures.collection(null,AasFixtures.property("x","xs:double",null)))).build();
		var codes=new DefaultSubmodelElementList.Builder().idShort("Codes").typeValueListElement(AasSubmodelElements.PROPERTY)
				.valueTypeListElement(Aas.xsd("xs:int")).build();
		var result=factory.create(explicitResponsePlan("aid-command",
				List.of(AasFixtures.variable(position),AasFixtures.variable(waypoints),AasFixtures.variable(codes))),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(ProtocolBridgeRouteConfiguration)result.configuration().getRoutes().get(0);
		assertEquals(List.of("Target","Waypoints","Codes","Count"),bridge.getTriggerData().get("callerOutputOrder"));
		assertEquals(Map.of("Count","xs:string"),bridge.getTriggerData().get("callerResponseTypes"),
				"only scalar Properties have a scalar caller type");
		assertEquals(Set.of("Target","Waypoints","Codes"),bridge.getCallerInputTemplates().keySet());
		assertEquals(Set.of("Target","Waypoints","Codes","Count"),bridge.getCallerOutputTemplates().keySet());
		assertEquals(position,bridge.getCallerOutputTemplates().get("Target"));
		assertEquals(waypoints,bridge.getCallerInputTemplates().get("Waypoints"));
	}

	@Test void rejectsUnsupportedStructuredAndDuplicateCciInoutputs() {
		var customType=AasFixtures.property("Uuid","xs:dateTime",null);
		var nestedCustomType=AasFixtures.collection("Uuid",AasFixtures.property("at","xs:dateTime",null));
		var duplicateMembers=AasFixtures.collection("Uuid",property("a","1"),property("a","2"));
		var mixedList=new DefaultSubmodelElementList.Builder().idShort("Uuid").typeValueListElement(AasSubmodelElements.PROPERTY)
				.valueTypeListElement(Aas.xsd("xs:int")).value(List.of(property("a","x"))).build();
		var untypedEmptyList=new DefaultSubmodelElementList.Builder().idShort("Uuid")
				.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION).build();
		var file=new DefaultFile.Builder().idShort("Uuid").contentType("text/plain").build();
		var trace=AasFixtures.variable(property("Trace","trace"));
		for (var inoutputs : List.of(List.of(AasFixtures.variable(customType)),List.of(AasFixtures.variable(nestedCustomType)),
				List.of(AasFixtures.variable(duplicateMembers)),List.of(AasFixtures.variable(mixedList)),
				List.of(AasFixtures.variable(untypedEmptyList)),List.of(AasFixtures.variable(file)),List.of(trace,trace))) {
			var result=factory.create(explicitResponsePlan("aid-command",inoutputs),"bridge");
			assertTrue(result.configuration().getRoutes().isEmpty(),inoutputs.toString());
			assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("CCI inoutput")),
					"InOut variables must be supported Properties, collections or lists with unique names");
		}
	}

	@Test void identicalRemoteActionShapeCannotSubstituteForExactResponseAnchor() {
		var result=factory.create(explicitResponsePlan("aid-remote"),"bridge");
		assertTrue(result.configuration().getRoutes().isEmpty());
		assertTrue(result.issues().stream().anyMatch(issue -> issue.message().contains("exact command ACTION anchor")));
	}

	@Test void methodArgumentTypesPreserveArrayRankAndTypedPropertySchemas() {
		var base=explicitResponsePlan("aid-command");
		var original=base.mappings().get(0);
		var sink=original.sinks().get(0);
		var target=((AidTarget)sink.target()).resolved();
		var integer=dataSchema("integer",Map.of());
		var array=new WireModel.DataSchema(Presence.absent(),Presence.absent(),Presence.absent(),Presence.of("array"),
				Map.of(),Presence.of(integer),Presence.absent(),Map.of(),emptyElement());
		var matrix=new WireModel.DataSchema(Presence.absent(),Presence.absent(),Presence.absent(),Presence.of("array"),
				Map.of(),Presence.of(array),Presence.absent(),Map.of(),emptyElement());
		var property=new WireModel.DataSchema(Presence.of("xs:string"),Presence.absent(),Presence.absent(),Presence.absent(),
				Map.of(),Presence.absent(),Presence.absent(),Map.of(),emptyElement());
		var output=dataSchema("object",Map.of("Count",integer,"Samples",array,"Matrix",matrix,"Label",property));
		var action=target.affordance();
		var typedAction=new WireModel.Affordance(action.kind(),action.idShort(),action.semanticId(),action.forms(),
				action.schema(),action.input(),Presence.of(output),action.element());
		var typedTarget=new AidTarget(new AasReferenceResolver.AidTarget(target.owner(),typedAction,target.schema(),
				target.schemaPath(),target.submodelId()));
		var mapping=new MappingPlan(original.identity(),original.listIndex(),original.sources(),
				List.of(new EndpointPlan(sink.id(),sink.reference(),typedTarget)),original.defaultPollingInterval(),
				original.transformation(),Presence.absent(),Presence.absent(),Presence.absent(),
				original.triggerSourceIds(),original.qualifiers());
		var result=factory.create(new Plan(base.aimcSubmodelId(),List.of(mapping)),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(ProtocolBridgeRouteConfiguration)result.configuration().getRoutes().get(0);
		assertEquals(Map.of("Count","integer","Samples","array<integer>","Matrix","array<array<integer>>",
				"Label","string"),bridge.getResponseTypes());
	}

	@Test void compiledResponseLuaAndRealFanOutBoundaryEmitOneCompletePayload() throws Exception {
		var result=factory.create(explicitResponsePlan("aid-command"),"bridge");
		assertTrue(result.issues().isEmpty(),result.issues().toString());
		var bridge=(ProtocolBridgeRouteConfiguration)result.configuration().getRoutes().get(0);
		var lua=(LuaTransformerConfiguration)result.configuration().getTransformers().get(bridge.getResponseTransformer());
		var writes=new java.util.concurrent.atomic.AtomicInteger();
		var payload=new java.util.concurrent.atomic.AtomicReference<Object>();
		try(var context=new org.apache.camel.impl.DefaultCamelContext()) {
			context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
				@Override public void configure() {
					from("direct:response-write").process(exchange -> { writes.incrementAndGet();payload.set(exchange.getMessage().getBody()); });
				}
			});
			lua.bind(context);context.start();
			List<Map<String,Object>> bindings=bridge.getResponseSinkBindings().stream().map(binding -> {
				Map<String,Object> copy=new java.util.LinkedHashMap<>(binding);copy.put("endpoint","direct:response-write");return copy;
			}).toList();
			try(var template=context.createProducerTemplate();var fanout=new org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor(context,"compiled-response",bindings)) {
				Object projected=org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.ResponseProjectionProcessor.project(
						Map.of("Count",42),bridge.getResponseSourceBindings());
				Map<?,?> transformed=template.requestBody(lua.getConnectionURI(),projected,Map.class);
				assertEquals(Map.of("Count","42"),transformed.get("Reply"));
				var exchange=new org.apache.camel.support.DefaultExchange(context);exchange.getMessage().setBody(transformed);
				var jobs=fanout.prepareJobs(exchange);assertEquals(1,jobs.size());
				exchange.getMessage().setBody(jobs.get(0));fanout.sendJob(exchange);
				assertEquals(1,writes.get());assertEquals(Map.of("x",1L,"y",2L),payload.get());
				exchange.getMessage().setBody(Map.of("X",1));assertTrue(fanout.prepareJobs(exchange).isEmpty());
				assertEquals(1,writes.get(),"missing group members must not emit a partial payload");
			} finally { context.stop();lua.close(); }
		}
	}

	private static Plan explicitResponsePlan(String responseAnchor) {
		return explicitResponsePlan(responseAnchor,List.of());
	}
	private static Plan explicitResponsePlan(String responseAnchor,List<OperationVariable> inoutputs) {
		var callerProperty=property("Count","0");
		var operation=AasFixtures.operation("operation",List.of(),List.of(AasFixtures.variable(callerProperty)),inoutputs);
		operation.setQualifiers(List.of(AasFixtures.delegation("http://dmp:8080/operations/asset%20one/Move")));
		var direct=new DirectTarget(new AasReferenceResolver.ResolvedElement(AasFixtures.submodel("cci","CCI",operation),operation));
		var operationRef=AasFixtures.reference("Submodel","cci","SubmodelElementCollection","Skills",
				"SubmodelElementCollection","Move","Operation","operation");
		var source=new EndpointPlan("invoke",operationRef,direct);
		var base=protocolTarget(WireModel.ProtocolFamily.HTTP,"http://device.example:8080","/move",Map.of(),"POST");
		var input=dataSchema("object",Map.of());
		var count=dataSchema("integer",Map.of());var output=dataSchema("object",Map.of("Count",count));
		var action=new WireModel.Affordance("action",Presence.of("Move"),Presence.absent(),base.resolved().affordance().forms(),
				Presence.absent(),Presence.of(input),Presence.of(output),emptyElement());
		var iface=base.resolved().owner();
		var command=new AidTarget(new AasReferenceResolver.AidTarget(iface,action,Presence.of(input),List.of(),"aid-command"));
		var response=new AidTarget(new AasReferenceResolver.AidTarget(iface,action,Presence.of(count),List.of("Count"),responseAnchor));
		var commandEndpoint=new EndpointPlan("Move",actionReference("aid-command","input"),command);
		var responseSource=new EndpointPlan("Body,&#",actionReference(responseAnchor,"output"),response);
		var extraBase=protocolTarget(WireModel.ProtocolFamily.HTTP,"http://extra.example:8080","/state",Map.of(),"POST");
		var payload=dataSchema("object",Map.of("x",count,"y",count));
		var extraAffordance=new WireModel.Affordance("property",Presence.of("State"),Presence.absent(),extraBase.resolved().affordance().forms(),
				Presence.of(payload),Presence.absent(),Presence.absent(),emptyElement());
		var x=new AidTarget(new AasReferenceResolver.AidTarget(extraBase.resolved().owner(),extraAffordance,Presence.of(count),List.of("x"),"aid-extra"));
		var y=new AidTarget(new AasReferenceResolver.AidTarget(extraBase.resolved().owner(),extraAffordance,Presence.of(count),List.of("y"),"aid-extra"));
		var mapping=new MappingPlan("responses",0,List.of(source),List.of(commandEndpoint),Presence.absent(),
				Presence.of(new WireModel.Blob("function aimc_main(sources) return {Move=sources.invoke} end".getBytes(StandardCharsets.UTF_8),"text/x-lua")),
				Presence.of(List.of(responseSource)),Presence.of(List.of(new EndpointPlan("Reply",operationRef,direct),
						new EndpointPlan("X",reference(),x),new EndpointPlan("Y",reference(),y))),
				Presence.of(new WireModel.Blob("function aimc_main(sources) return {Reply={Count=tostring(sources['Body,&#'])},X=1,Y=2} end".getBytes(StandardCharsets.UTF_8),"text/x-lua")),Presence.absent(),List.of());
		return new Plan("aimc",List.of(mapping));
	}
	private static Reference actionReference(String anchor,String direction) {
		return AasFixtures.reference("Submodel",anchor,"SubmodelElementCollection","interface_device",
				"SubmodelElementCollection","InteractionMetadata","SubmodelElementCollection","actions",
				"SubmodelElementCollection","Move","SubmodelElementCollection",direction);
	}
	private static WireModel.DataSchema dataSchema(String type,Map<String,WireModel.DataSchema> properties) {
		return new WireModel.DataSchema(Presence.absent(),Presence.absent(),Presence.absent(),Presence.of(type),properties,
				Presence.absent(),Presence.absent(),Map.of(),emptyElement());
	}

	private static MappingPlan mapping(WireModel.ProtocolFamily family, boolean withLua) {
		var schema = Presence.<WireModel.DataSchema>absent();
		var source = new EndpointPlan("Source.authored", reference(), target(family, "GET", schema));
		var sink = new EndpointPlan("Sink.authored", reference(), target(family, "POST", schema));
		Presence<WireModel.Blob> transformation = withLua
				? Presence.of(new WireModel.Blob("function aimc_main(sources) return sources end".getBytes(StandardCharsets.UTF_8), "text/x-lua"))
				: Presence.absent();
		return new MappingPlan("logical-map", 3, List.of(source), List.of(sink), Presence.absent(),
				transformation, Presence.absent(), Presence.absent(), Presence.absent(), Presence.absent(), List.of());
	}

	private static AidTarget target(WireModel.ProtocolFamily family, String method,
			Presence<WireModel.DataSchema> schema) {
		var form = new WireModel.Form(Presence.of("http://device.example/api/value"), Presence.absent(),
				Presence.absent(), Presence.absent(), Presence.of(method), Map.of(), Presence.absent(), emptyElement());
		var affordance = new WireModel.Affordance("property", Presence.of("value"), Presence.absent(),
				List.of(form), schema, schema, schema, emptyElement());
		var iface = new WireModel.Interface("interface-semantic", "interface-device", List.of(), List.of(family),
				emptyElement(), List.of(form), List.of(), List.of(affordance), List.of(), List.of(),
				Presence.absent(), Presence.absent(), schema, schema, Presence.absent(), Presence.absent());
		return new AidTarget(new AasReferenceResolver.AidTarget(iface, affordance, schema, List.of()));
	}

	private static AidTarget protocolTarget(WireModel.ProtocolFamily family,String base,String href,
			Map<String,String> bindingTerms, String httpMethod) {
		Map<String,String> terms=new java.util.LinkedHashMap<>(bindingTerms);
		var form=new WireModel.Form(Presence.of(href),Presence.absent(),Presence.absent(),Presence.absent(),
				httpMethod==null?Presence.absent():Presence.of(httpMethod),Map.of(),Presence.absent(),terms,emptyElement());
		var affordance=new WireModel.Affordance("property",Presence.of("value"),Presence.absent(),
				List.of(form),Presence.absent(),Presence.absent(),Presence.absent(),emptyElement());
		SubmodelElement endpointMetadata=AasFixtures.collection("EndpointMetadata",property("base",base));
		var iface=new WireModel.Interface("interface-semantic","interface_device",List.of("binding"),List.of(family),
				emptyElement(),List.of(form),List.of(),List.of(affordance),List.of(),List.of(),Presence.of(endpointMetadata),
				Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent());
		return new AidTarget(new AasReferenceResolver.AidTarget(iface,affordance,Presence.absent(),List.of()));
	}

	private static EndpointPlan endpointPlan(String id,AidTarget target) {
		return new EndpointPlan(id,reference(),target);
	}

	private static MappingPlan mappingWithEndpoints(EndpointPlan source,EndpointPlan sink) {
		return mappingPlan("logical-map",0,source,sink);
	}

	private static MappingPlan mappingPlan(String identity,int index,EndpointPlan source,EndpointPlan sink) {
		return new MappingPlan(identity,index,List.of(source),List.of(sink),Presence.absent(),Presence.absent(),
				Presence.absent(),Presence.absent(),Presence.absent(),Presence.absent(),List.of());
	}

	private static Reference reference() {
		return AasFixtures.reference("Submodel", "aid");
	}
	private static AidTarget mqttTarget(SubmodelElement endpoint, WireModel.Form form, String id) {
		var affordance = new WireModel.Affordance("property", Presence.of(id), Presence.absent(), List.of(form),
				Presence.absent(), Presence.absent(), Presence.absent(), emptyElement());
		var iface = new WireModel.Interface("interface-semantic", "interface_mqtt", List.of("http://www.w3.org/2011/mqtt"),
				List.of(WireModel.ProtocolFamily.MQTT), emptyElement(), List.of(form), List.of(), List.of(affordance),
					List.of(), List.of(), Presence.of(endpoint), Presence.absent(), Presence.absent(), Presence.absent(),
					Presence.absent(), Presence.absent());
		return new AidTarget(new AasReferenceResolver.AidTarget(iface, affordance, Presence.absent(), List.of()));
	}
	private static WireModel.Form mqttForm(String href, String qos, String retain) {
		Map<String, String> terms = new java.util.LinkedHashMap<>();
		terms.put("mqv_qos", qos);
		if (retain != null) terms.put("mqv_retain", retain);
		return new WireModel.Form(Presence.of(href), Presence.absent(), Presence.absent(), Presence.absent(),
				Presence.absent(), terms, Presence.absent(), emptyElement());
	}
	private static SubmodelElement property(String id, String value) {
		return AasFixtures.property(id, value);
	}
	private static SubmodelElement emptyElement() {
		return AasFixtures.collection(null);
	}
}
