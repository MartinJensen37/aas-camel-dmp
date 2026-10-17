/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.management.compiler;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.security.BasicCredential;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AASDatasinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AASPollingDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AasEventDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.operation.OperationVariableValues;
import org.eclipse.digitaltwin.basyx.dmp.aas.operation.ProtocolBridgeRouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.HttpDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.HttpPollingDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.LuaTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaCompiledSource;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.MqttDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.MqttDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.Option;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.Plc4xDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.Plc4xDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.Tag;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.AidTarget;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.DirectTarget;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.EndpointPlan;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.MappingPlan;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.Plan;
import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.QualifierKind;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;
import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel.DataSchema;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel.Presence;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel.Form;
import org.eclipse.digitaltwin.basyx.dmp.management.configuration.MqttV5OperationDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasReferenceResolver;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasRepositoryClient;

/** Compiles resolved wire plans into existing runtime connector configuration classes. */
public final class ConnectorConfigurationFactory {
	private static final long MAX_LUA_BYTES = 1_048_576;
	// Two scripts per mapping, covering installed, desired and staged generations at the 8192-mapping limit.
	private static final int MAX_CACHED_LUA_SOURCES = 65_536;
	private record ScriptIdentity(String diagnostic, String content) { }
	private static final Map<ScriptIdentity, java.util.concurrent.CompletableFuture<java.lang.ref.WeakReference<LuaCompiledSource>>> LUA_SOURCES =
			new LinkedHashMap<>();

	private static LuaCompiledSource compiledLua(byte[] bytes, String identity) {
		if (bytes.length > MAX_LUA_BYTES) throw new IllegalArgumentException("Lua source exceeds maxScriptBytes");
		String content;
		try { content = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)); }
		catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
		// Cache immutable prototypes only, not bindings or evaluation state. Retired sources remain collectible.
		ScriptIdentity key = new ScriptIdentity(identity, content);
		while (true) {
			java.util.concurrent.CompletableFuture<java.lang.ref.WeakReference<LuaCompiledSource>> pending;
			boolean compile = false;
			synchronized (LUA_SOURCES) {
				pending = LUA_SOURCES.get(key);
				if (pending != null && pending.isDone() && pending.join().get() == null) {
					LUA_SOURCES.remove(key);
					pending = null;
				}
				if (pending == null) {
					if (LUA_SOURCES.size() >= MAX_CACHED_LUA_SOURCES)
						LUA_SOURCES.values().removeIf(value -> value.isDone() && value.join().get() == null);
					if (LUA_SOURCES.size() >= MAX_CACHED_LUA_SOURCES)
						throw new IllegalStateException("managed Lua compilation capacity exceeded");
					pending = new java.util.concurrent.CompletableFuture<>();
					LUA_SOURCES.put(key, pending);
					compile = true;
				}
			}
			// Deduplicate one source without serializing independent AIMC compilation.
			if (compile) {
				try {
					LuaCompiledSource source = LuaCompiledSource.fromBytes(bytes, identity, MAX_LUA_BYTES);
					pending.complete(new java.lang.ref.WeakReference<>(source));
					return source;
				} catch (RuntimeException | Error failure) {
					synchronized (LUA_SOURCES) { LUA_SOURCES.remove(key, pending); }
					pending.completeExceptionally(failure);
					throw failure;
				}
			}
			LuaCompiledSource source = pending.join().get();
			if (source != null) return source;
		}
	}

	public record Issue(String path, String mappingId, String message) { }
	public record Result(RoutesConfiguration configuration, List<Issue> issues) {
		public Result { issues = List.copyOf(issues); }
	}
	public record CompiledBundle(String mappingIdentity,int listIndex,String bundleId,String routeId,
			RoutesConfiguration configuration, EffectiveFingerprint fingerprint) { }
	public record BundleResult(List<CompiledBundle> bundles,List<Issue> issues) {
		public BundleResult { bundles=List.copyOf(bundles);issues=List.copyOf(issues); }
	}
	public record DesiredBundle(MappingPlanCompiler.DesiredMapping desired,CompiledBundle bundle,List<Issue> issues,
			EffectiveFingerprint fingerprint) {
		public DesiredBundle { issues=List.copyOf(issues); }
		public boolean valid() { return desired.valid() && bundle!=null && issues.isEmpty(); }
	}
	public record RuntimeSettings(URI aasRepositoryBaseUri) {
		public RuntimeSettings {
			Objects.requireNonNull(aasRepositoryBaseUri,"aasRepositoryBaseUri");
			if(aasRepositoryBaseUri.getUserInfo()!=null || aasRepositoryBaseUri.getQuery()!=null
					|| aasRepositoryBaseUri.getFragment()!=null || aasRepositoryBaseUri.getHost()==null
					|| !("http".equalsIgnoreCase(aasRepositoryBaseUri.getScheme()) || "https".equalsIgnoreCase(aasRepositoryBaseUri.getScheme()))) {
				throw new IllegalArgumentException("AAS runtime base must be a credential-free HTTP(S) URI");
			}
		}
	}

	/** routeId is an ownership identity supplied by the mapping-bundle caller. */
	public Result create(Plan plan, String routeId) {
		return create(plan,routeId,null);
	}

	public Result create(Plan plan, String routeId, RuntimeSettings runtime) {
		Objects.requireNonNull(plan, "plan");
		if (routeId == null || routeId.isBlank()) throw new IllegalArgumentException("routeId is required");
		RoutesConfiguration output = new RoutesConfiguration();
		List<Issue> issues = new ArrayList<>();
		for (MappingPlan mapping : plan.mappings()) {
			int issueStart = issues.size();
			validateCapabilities(mapping,issues);
			if(issues.size()!=issueStart)continue;
			if (operationMapping(mapping)) {
				compileBridgeMapping(mapping,plan,routeId,runtime,output,issues);
				continue;
			}
			List<DataSourceConfiguration> sources = new ArrayList<>();
			List<DataSinkConfiguration> sinks = new ArrayList<>();
			List<String> sourceIds = new ArrayList<>();
			List<String> sinkIds = new ArrayList<>();
			for (int i = 0; i < mapping.sources().size(); i++) {
				EndpointPlan endpoint = mapping.sources().get(i);
				DataSourceConfiguration source = source(endpoint, mapping, i, plan, routeId, runtime, issues);
				if (source != null) { sources.add(source); sourceIds.add(source.getUniqueId()); }
			}
			for (int i = 0; i < mapping.sinks().size(); i++) {
				EndpointPlan endpoint = mapping.sinks().get(i);
				DataSinkConfiguration sink = sink(endpoint, mapping, i, plan, routeId, runtime, issues);
				if(sink!=null)payloadBinding(sink,endpoint,mapping,issues);
				if (sink != null) { sinks.add(sink); sinkIds.add(sink.getUniqueId()); }
			}
			OptionalLong pollingMillis = pollingMillis(mapping, issues);
			if (pollingMillis.isPresent()) sources.stream()
					.filter(source -> "sync".equalsIgnoreCase(source.getDelivery()) && source.getPollingIntervalMs()<=0)
					.forEach(source -> {
						source.setDelivery("sync");
						source.setPollingIntervalMs(pollingMillis.getAsLong());
					});
			sources.stream().filter(source -> "sync".equalsIgnoreCase(source.getDelivery()) && source.getPollingIntervalMs()<=0)
					.forEach(source -> source.setPollingIntervalMs(5000));
			if (mapping.qualifiers().stream().anyMatch(ConnectorConfigurationFactory::behaviorQualifier)) issue(issues, mapping,
					"MappingConfigurations/value/" + mapping.listIndex() + "/qualifiers",
					"mapping qualifier semantics are not represented by the runtime route API");
			if (mapping.responseSources().present() || mapping.responseSinks().present()
					|| mapping.responseTransformation().present() || mapping.statusSources().present()
					|| mapping.statusTransformation().present() || mapping.statusTimeout().present()) issue(issues, mapping,
					"MappingConfigurations/value/" + mapping.listIndex() + "/response",
					"response routes are not represented by this ordinary execution-policy route factory");
			if (mapping.triggerSourceIds().present() && (mapping.triggerSourceIds().value() == null
					|| mapping.triggerSourceIds().value().isEmpty())) issue(issues, mapping,
					"MappingConfigurations/value/" + mapping.listIndex() + "/ExecutionSemantics/TriggerSourceIds",
					"authored trigger selection must be a nonempty list");

			List<DataTransformerConfiguration> transformers = new ArrayList<>();
			if (mapping.transformation().present()) {
				WireModel.Blob blob = mapping.transformation().value();
				if (blob == null) issue(issues, mapping,
						"MappingConfigurations/value/" + mapping.listIndex() + "/Transformation",
						"authored Lua Blob is null");
				else {
					try {
						String id = entityId(mapping, "transform", 0);
						LuaCompiledSource compiled = compiledLua(blob.bytes(), mapping.identity());
						LuaTransformerConfiguration lua=new LuaTransformerConfiguration(id,compiled);
						lua.setSourceKeys(String.join(",",mapping.sources().stream().map(EndpointPlan::id).toList()));
						lua.setKeyedInput(true);
						transformers.add(lua);
					} catch (RuntimeException failure) {
						issue(issues, mapping, "MappingConfigurations/value/" + mapping.listIndex() + "/Transformation",
								"Lua source cannot be compiled: " + failure.getMessage());
					}
				}
			}

			// A mapping is atomic: never return a route with only a subset of its endpoints.
			if (issues.size() != issueStart) continue;
			output.addDatasources(sources);
			output.addDatasinks(sinks);
			output.addTransformers(transformers);
			RouteConfiguration route = new RouteConfiguration(ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER,
					sourceIds, transformers.stream().map(DataTransformerConfiguration::getUniqueId).toList(), sinkIds);
			route.setRouteId(routeId + ":" + mapping.listIndex());
			if (mapping.triggerSourceIds().present()) {
				Map<String, Object> triggerData = new LinkedHashMap<>();
				triggerData.put("triggerSourceIds", mapping.triggerSourceIds().value());
				route.setTriggerData(triggerData);
			}
			output.addRoute(new ExecutionPolicyRouteConfiguration(route));
		}
		return new Result(output, issues);
	}

	/** A bridge mapping with a status phase: it declares StatusSources. */
	private static boolean twoPhase(MappingPlan mapping) {
		return mapping.statusSources().value()!=null && !mapping.statusSources().value().isEmpty();
	}

	private static boolean statusSource(EndpointPlan endpoint) {
		return endpoint.target() instanceof AidTarget target
				&& List.of("property","event").contains(target.resolved().affordance().kind().toLowerCase(java.util.Locale.ROOT));
	}

	private static boolean operationMapping(MappingPlan mapping) {
		return mapping.sources().stream().anyMatch(endpoint -> endpoint.target() instanceof DirectTarget direct
				&& direct.resolved().element() instanceof Operation);
	}

	private void compileBridgeMapping(MappingPlan mapping,Plan plan,String routeId,RuntimeSettings runtime,
			RoutesConfiguration output,List<Issue> issues) {
		int issueStart=issues.size();
		String root="MappingConfigurations/value/"+mapping.listIndex();
		List<EndpointPlan> operations=mapping.sources().stream().filter(endpoint -> endpoint.target() instanceof DirectTarget direct
				&& direct.resolved().element() instanceof Operation).toList();
		if(operations.size()!=1 || mapping.sources().size()!=1) {
			issue(issues,mapping,root+"/Sources","bridge mappings require exactly one declared CCI Operation source");
			return;
		}
		EndpointPlan operation=operations.get(0);
		String action=operationAction(operation.reference());
		if(action==null || action.isBlank()) { issue(issues,mapping,root+"/Sources","Operation reference does not identify an exact parent skill/action");return; }
		if(mapping.sinks().isEmpty()) { issue(issues,mapping,root+"/Sinks","bridge Operation requires at least one declared command binding");return; }
		if(mapping.qualifiers().stream().anyMatch(ConnectorConfigurationFactory::behaviorQualifier)) issue(issues,mapping,root+"/qualifiers","mapping qualifiers are not represented by the bridge route schema");
		if(mapping.triggerSourceIds().present() && (mapping.triggerSourceIds().value()==null
				|| !mapping.triggerSourceIds().value().equals(List.of(operation.id()))))
			issue(issues,mapping,root+"/ExecutionSemantics/TriggerSourceIds","operation trigger selection must include the exact Operation SourceId");
		// Command sinks: the Operation's AID action, plus parameter properties written before it.
		AidTarget actionTarget=null;
		List<EndpointPlan> actionSinks=new ArrayList<>(),parameterSinks=new ArrayList<>();
		for(EndpointPlan sink:mapping.sinks()) {
			if(sink.target() instanceof AidTarget target && "action".equalsIgnoreCase(target.resolved().affordance().kind())
					&& action.equals(target.resolved().affordance().idShort().value())) {
				if(actionTarget==null) actionTarget=target;
				else if(!sameAction(actionTarget,target)) issue(issues,mapping,root+"/Sinks/"+sink.id(),
						"grouped command members must share one AID action endpoint");
				actionSinks.add(sink);
			} else if(sink.target() instanceof AidTarget target && "property".equalsIgnoreCase(target.resolved().affordance().kind())) {
				parameterSinks.add(sink);
			} else {
				issue(issues,mapping,root+"/Sinks/"+sink.id(),"command sinks must address the Operation's AID action or parameter properties of that AID");
			}
		}
		if(actionTarget==null && issues.size()==issueStart)
			issue(issues,mapping,root+"/Sinks","command binding must address the same exact AID action as the CCI Operation");
		if(actionTarget==null || issues.size()!=issueStart) return;
		for(EndpointPlan sink:parameterSinks)
			if(!Objects.equals(((AidTarget)sink.target()).resolved().submodelId(),actionTarget.resolved().submodelId()))
				issue(issues,mapping,root+"/Sinks/"+sink.id(),"parameter properties must belong to the action's AID");
		if(issues.size()!=issueStart) return;
		if(mapping.responseTransformation().present() && mapping.responseTransformation().value()==null) {
			issue(issues,mapping,root+"/ResponseTransformation","response transformation Blob is explicitly null");return;
		}
		String commandId=entityId(mapping,"command",0);
		Map<String,Object> targetOptions=new LinkedHashMap<>();
		DataSinkConfiguration commandSink=bridgeCommandSink(mapping,action,actionTarget,plan,routeId,commandId,
				targetOptions,issues,root+"/Sinks",twoPhase(mapping));
		if(commandSink==null || issues.size()!=issueStart) return;
		DataSchema input=actionTarget.resolved().affordance().input().present()
				?actionTarget.resolved().affordance().input().value():null;
		DataSchema response=actionTarget.resolved().affordance().output().present()
				?actionTarget.resolved().affordance().output().value():null;
		Map<String,String> parameterTypes=schemaTypes(input);
		Map<String,String> responseTypes=schemaTypes(response);
		List<String> parameterRequired=schemaRequired(input);
		List<String> responseRequired=schemaRequired(response);
		String restPath=delegationPath((Operation)((DirectTarget)operation.target()).resolved().element());
		if(restPath==null) { issue(issues,mapping,root+"/Sources","CCI Operation needs an invocationDelegation qualifier whose URL names the DMP path");return; }
		Map<String,Object> trigger=new LinkedHashMap<>();
		trigger.put(ProtocolBridgeRouteConfiguration.REST_PATH,restPath);
		trigger.put(ProtocolBridgeRouteConfiguration.REST_METHOD,"POST");
		trigger.put("action",action);
		// The Operation's AAS identity, for the interaction log: submodel ID and idShort path.
		List<Key> operationKeys=Aas.keys(operation.reference());
		if(!operationKeys.isEmpty()) {
			trigger.put(ProtocolBridgeRouteConfiguration.OPERATION_SUBMODEL_ID,operationKeys.get(0).getValue());
			trigger.put(ProtocolBridgeRouteConfiguration.OPERATION_PATH,String.join(".",
					operationKeys.subList(1,operationKeys.size()).stream().map(Key::getValue).toList()));
		}
		trigger.put(ProtocolBridgeRouteConfiguration.OPERATION_SOURCE_ID,operation.id());
		trigger.put("requestSourceIds",List.of(operation.id()));
		trigger.put(ProtocolBridgeRouteConfiguration.COMMAND_SINK_ID,actionSinks.get(0).id());
		trigger.put(ProtocolBridgeRouteConfiguration.COMMAND_SINK_IDS,List.of(commandSink.getUniqueId()));
		List<Map<String,Object>> bindings=new ArrayList<>();
		for(EndpointPlan sink:actionSinks) {
			AidTarget endpointTarget=(AidTarget)sink.target();
			Map<String,Object> binding=new LinkedHashMap<>();
			binding.put("sinkId",sink.id());
			binding.put("dataSinkId",commandSink.getUniqueId());
			binding.put("valuePath",jsonPointer(endpointTarget.resolved().schemaPath()));
			bindings.add(binding);
		}
		trigger.put(ProtocolBridgeRouteConfiguration.COMMAND_SINK_BINDINGS,List.copyOf(bindings));
		List<DataSinkConfiguration> parameterDestinations=new ArrayList<>();
		List<Map<String,Object>> parameterBindings=new ArrayList<>();
		for(int i=0;i<parameterSinks.size();i++) {
			EndpointPlan endpoint=parameterSinks.get(i);
			DataSinkConfiguration sink=sink(endpoint,mapping,300000+i,plan,routeId,runtime,issues);
			if(sink==null)continue;
			payloadBinding(sink,endpoint,mapping,issues);
			parameterDestinations.add(sink);
			parameterBindings.add(sinkBinding(endpoint,sink));
		}
		trigger.put(ProtocolBridgeRouteConfiguration.PARAMETER_SINK_BINDINGS,List.copyOf(parameterBindings));
		trigger.put(ProtocolBridgeRouteConfiguration.CALLER_REPLY_SINK_ID,action);
		trigger.put(ProtocolBridgeRouteConfiguration.PARAMETER_TYPES,parameterTypes);
		trigger.put("parameterRequired",parameterRequired);
		trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_TYPES,responseTypes);
		trigger.put("responseRequired",responseRequired);
		List<String> responseOrder=operation.target() instanceof DirectTarget direct
				&& direct.resolved().element() instanceof Operation cciOperation
				? Aas.nonNull(cciOperation.getOutputVariables()).stream().map(variable -> variable.getValue()==null?null:variable.getValue().getIdShort())
						.filter(Objects::nonNull).toList() : List.of();
		Map<String,String> callerTypes=new LinkedHashMap<>();
		Map<String,String> inputTemplates=new LinkedHashMap<>();
		Map<String,String> outputTemplates=new LinkedHashMap<>();
		Operation cci=(Operation)((DirectTarget)operation.target()).resolved().element();
		List<String> callerOrder=new ArrayList<>();
		Set<String> callerNames=new LinkedHashSet<>();
		// Inputs reach the caller request; InOut variables are both request and reply, outputs reply only.
		Map<String,List<OperationVariable>> categories=new LinkedHashMap<>();
		categories.put("input",Aas.nonNull(cci.getInputVariables()));
		categories.put("inoutput",Aas.nonNull(cci.getInoutputVariables()));
		categories.put("output",Aas.nonNull(cci.getOutputVariables()));
		categories.forEach((category,variables) -> {
			for(OperationVariable variable:variables) {
				SubmodelElement value=variable==null?null:variable.getValue();
				if(value==null || !callerElementSupported(value,true) || !callerNames.add(value.getIdShort())) {
					issue(issues,mapping,root+"/Sources","CCI "+category+" variables must be uniquely named Properties with"
							+" supported valueTypes, or collections and lists of them");
					continue;
				}
				String template=Aas.write(value);
				if(!"output".equals(category))inputTemplates.put(value.getIdShort(),template);
				if(!"input".equals(category)) {
					outputTemplates.put(value.getIdShort(),template);
					if(value instanceof Property property)callerTypes.put(value.getIdShort(),Aas.xsd(property.getValueType()));
					callerOrder.add(value.getIdShort());
				}
			}
		});
		trigger.put("callerResponseTypes",Map.copyOf(callerTypes));
		trigger.put(ProtocolBridgeRouteConfiguration.CALLER_INPUT_TEMPLATES,Map.copyOf(inputTemplates));
		trigger.put(ProtocolBridgeRouteConfiguration.CALLER_OUTPUT_TEMPLATES,Map.copyOf(outputTemplates));
		if(!responseOrder.isEmpty()) {
			trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_ORDER,responseOrder);
		}
		if(!callerOrder.isEmpty())trigger.put("callerOutputOrder",List.copyOf(callerOrder));
		if(response!=null) trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_SCHEMA,schema(Presence.of(response)));
		boolean explicit=mapping.responseSources().present() || mapping.responseSinks().present();
		trigger.put(ProtocolBridgeRouteConfiguration.EXPLICIT_RESPONSE_MAPPING,explicit);
		List<DataSinkConfiguration> responseDestinations=new ArrayList<>();
		List<DataSourceConfiguration> statusReads=new ArrayList<>(),responseReads=new ArrayList<>();
		compileStatus(mapping,actionTarget,plan,routeId,runtime,trigger,statusReads,issues);
		if(explicit)compileResponseBindings(mapping,operation,actionTarget,plan,routeId,runtime,trigger,responseDestinations,
				responseReads,issues);
		else if(mapping.responseTransformation().present())trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_SOURCE_BINDINGS,
				List.of(Map.of("sourceId",action)));
		trigger.put("targetOptions",Map.copyOf(targetOptions));

		List<DataTransformerConfiguration> transformers=new ArrayList<>();
		String requestTransformerId=entityId(mapping,"request-transform",0);
		if(!mapping.transformation().present() || mapping.transformation().value()==null) {
			issue(issues,mapping,root+"/Transformation","Operation mappings require an authored request Lua transform");return;
		}
		byte[] requestBytes=mapping.transformation().value().bytes();
		addLua(transformers,requestTransformerId,requestBytes,mapping.identity()+":request",mapping,root+"/Transformation",issues);
		if(!transformers.isEmpty()) {
			LuaTransformerConfiguration requestLua=(LuaTransformerConfiguration)transformers.get(0);
			requestLua.setSourceKeys(operation.id());requestLua.setKeyedInput(true);
		}
		List<String> transformerIds=new ArrayList<>();
		if(!transformers.isEmpty()) transformerIds.add(requestTransformerId);
		if(twoPhase(mapping) && mapping.statusTransformation().value()!=null) {
			String statusTransformerId=entityId(mapping,"status-transform",0);
			int before=transformers.size();
			addLua(transformers,statusTransformerId,mapping.statusTransformation().value().bytes(),
					mapping.identity()+":status",mapping,root+"/StatusTransformation",issues);
			if(transformers.size()>before) {
				LuaTransformerConfiguration statusLua=(LuaTransformerConfiguration)transformers.get(transformers.size()-1);
				statusLua.setSourceKeys(String.join(",",mapping.statusSources().value().stream().map(EndpointPlan::id).toList()));
				statusLua.setKeyedInput(true);
				trigger.put(ProtocolBridgeRouteConfiguration.STATUS_TRANSFORMER,statusTransformerId);
			}
		}
		if(mapping.responseTransformation().present() && mapping.responseTransformation().value()!=null) {
			String responseTransformerId=entityId(mapping,"response-transform",0);
			addLua(transformers,responseTransformerId,mapping.responseTransformation().value().bytes(),
					mapping.identity()+":response",mapping,root+"/ResponseTransformation",issues);
			if(issues.size()==issueStart) {
				LuaTransformerConfiguration responseLua=(LuaTransformerConfiguration)transformers.get(transformers.size()-1);
				List<String> responseKeys=new ArrayList<>();
				if(trigger.get(ProtocolBridgeRouteConfiguration.RESPONSE_SOURCE_BINDINGS) instanceof List<?> declared)
					for(Object binding:declared)responseKeys.add(String.valueOf(((Map<?,?>)binding).get("sourceId")));
				responseLua.setSourceKeys(explicit?String.join(",",responseKeys):action);
				responseLua.setKeyedInput(true);
				trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_TRANSFORMER,responseTransformerId);
			}
		}
		if(issues.size()!=issueStart) return;
		output.addDatasources(statusReads);
		output.addDatasources(responseReads);
		output.addDatasinks(parameterDestinations);
		output.addDatasink(commandSink);
		output.addDatasinks(responseDestinations);
		output.addTransformers(transformers);
		List<String> sinkIds=new ArrayList<>();
		parameterDestinations.forEach(sink -> sinkIds.add(sink.getUniqueId()));
		sinkIds.add(commandSink.getUniqueId());
		responseDestinations.forEach(sink -> sinkIds.add(sink.getUniqueId()));
		RouteConfiguration base=new RouteConfiguration(ProtocolBridgeRouteConfiguration.ROUTE_TRIGGER,
				List.of(),transformerIds,List.copyOf(sinkIds));
		ProtocolBridgeRouteConfiguration bridge=new ProtocolBridgeRouteConfiguration(base);
		bridge.setRouteId(routeId+":"+mapping.listIndex());
		bridge.setTriggerData(trigger);
		output.addRoute(bridge);
	}

	private DataSinkConfiguration bridgeCommandSink(MappingPlan mapping,String action,AidTarget target,Plan plan,
			String routeId,String uniqueId,Map<String,Object> targetOptions,List<Issue> issues,String path,boolean twoPhase) {
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if(families.size()!=1) { issue(issues,mapping,path,"Operation command requires one unambiguous AID binding, found "+families);return null; }
		DataSchema input=target.resolved().affordance().input().present()?target.resolved().affordance().input().value():null;
		WireModel.ProtocolFamily family=families.get(0);
		if(family==WireModel.ProtocolFamily.HTTP || family==WireModel.ProtocolFamily.IOLINK) {
			if(!selectHttpForm(target,mapping,path,issues))return null;
			Form form=selectedForm(target);
			if(!validateForm(form,"POST",mapping,path,issues))return null;
			URI uri=formUri(target,form,mapping,path,false,issues);
			if(uri==null || !httpUri(uri,mapping,path,issues))return null;
			if(uri.getUserInfo()!=null) { issue(issues,mapping,path+"/forms/href","HTTP operation targets have no credentials field");return null; }
			HttpDataSinkConfiguration sink=new HttpDataSinkConfiguration(uniqueId,uri.getScheme(),uri.getHost(),uri.getPath(),effectivePort(uri));
			sink.setQuery(uri.getQuery());sink.setSinkId(action);sink.setSchema(schemaOptional(input));
			String method=form.httpMethod().present()?form.httpMethod().value():"POST";
			targetOptions.put("httpMethod",method);targetOptions.put("timeoutSeconds",30);
			return sink;
		}
		if(family==WireModel.ProtocolFamily.OPCUA) {
			Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
			ProtocolBase base=protocolBase(target,"opc.tcp",mapping,path,issues);if(base==null)return null;
			String method=opcuaNode(form,mapping,path,issues);
			String componentOf=componentOf(target,mapping,path,issues);
			if(method==null || componentOf==null)return null;
			Map<String,String> options=new LinkedHashMap<>();
			options.put("method","RAW("+method+")");
			OpcuaDataSinkConfiguration sink=new OpcuaDataSinkConfiguration(uniqueId,base.host(),base.port(),base.servicePath(),
					componentOf,base.username(),base.password(),options);
			sink.setSinkId(action);sink.setSchema(schemaOptional(input));
			targetOptions.put("timeoutSeconds",30);
			return sink;
		}
		if(family==WireModel.ProtocolFamily.MQTT) {
			Form form=mqttForm(target,mapping,path,"sink",true,issues);if(form==null)return null;
			MqttConnection base=mqttConnection(target,mapping,path,issues);if(base==null)return null;
			String topic=mqttTopic(base,form,mapping,path,issues);if(topic==null)return null;
			String reply=form.responseTopic().present()?form.responseTopic().value():mqttTerm(form,"mqv_response_topic");
			String requestReply=mqttTerm(form,"mqv_request_reply");
			if(!"true".equalsIgnoreCase(requestReply) || reply==null || reply.isBlank()) {
				issue(issues,mapping,path+"/forms","MQTT Operation requires requestReply=true and an exact reply topic");return null;
			}
			String replyTopic=mqttTopic(base,reply,mapping,path+"/forms/response",issues);
			Integer qos=mqttQos(form,mapping,path,issues);
			Boolean retained=mqttRetain(form,mapping,path,issues);
			if(replyTopic==null || hasIssue(issues,mapping))return null;
			String timeout=mqttTerm(form,"mqv_reply_timeout");
			long timeoutSeconds=30;
			if(timeout!=null)try { timeoutSeconds=Long.parseLong(timeout); }
				catch(NumberFormatException invalid) { issue(issues,mapping,path+"/forms/mqv_reply_timeout","reply timeout must be a positive integer seconds");return null; }
			if(timeoutSeconds<1) { issue(issues,mapping,path+"/forms/mqv_reply_timeout","reply timeout must be positive");return null; }
			MqttV5OperationDataSinkConfiguration sink=new MqttV5OperationDataSinkConfiguration(uniqueId,
					"tcp://"+base.host()+":"+base.port(),topic,stableMqttClientId(plan,routeId,mapping,action,"operation"),
					qos==null?2:qos,Boolean.TRUE.equals(retained),replyTopic,timeoutSeconds,base.username(),base.password());
			sink.setSinkId(action);sink.setSchema(schemaOptional(input));
			targetOptions.put("requestReply",true);targetOptions.put("responseTopic",replyTopic);
			targetOptions.put("timeoutSeconds",timeoutSeconds);
			if(twoPhase)targetOptions.put("firstReplyIsAck",true);
			return sink;
		}
		if(family==WireModel.ProtocolFamily.MODBUS || family==WireModel.ProtocolFamily.BACNET) {
			// A tag write starts the action; only the status reads tell when it has finished.
			if(!twoPhase) { issue(issues,mapping,path,"a PLC4X tag-write action requires StatusSources: a tag write does not report when the action has finished");return null; }
			Plc4xDataSinkConfiguration sink=plc4xTagSink(target,uniqueId,action,mapping,path,issues);
			if(sink==null)return null;
			sink.setSinkId(action);sink.setSchema(schemaOptional(input));
			targetOptions.put("commandTag",action);
			if(input!=null && input.type().value()!=null && !"object".equals(input.type().value()))
				targetOptions.put("commandType",argumentType(input));
			return sink;
		}
		issue(issues,mapping,path,"unsupported Operation connector family "+families);
		return null;
	}

	/** The path of the Operation's BaSyx invocationDelegation URL: where BaSyx will call this DMP. */
	private static String delegationPath(Operation operation) {
		List<String> values=Aas.nonNull(operation.getQualifiers()).stream()
				.filter(qualifier -> "invocationDelegation".equals(qualifier.getType())).map(Qualifier::getValue).toList();
		if(values.size()!=1 || values.get(0)==null || values.get(0).isBlank())return null;
		try {
			URI uri=new URI(values.get(0).trim());
			String path=uri.getRawPath();
			if(!uri.isAbsolute() || uri.getHost()==null || path==null || path.isBlank() || "/".equals(path)
					|| uri.getRawQuery()!=null || uri.getRawFragment()!=null || path.contains("{"))return null;
			return path;
		} catch(java.net.URISyntaxException invalid) { return null; }
	}

	private static String operationAction(Reference reference) {
		List<Key> keys=Aas.keys(reference);
		for(int i=keys.size()-1;i>0;i--)if("Operation".equals(Aas.keyType(keys.get(i))))return keys.get(i-1).getValue();
		return null;
	}

	private static boolean sameAction(AidTarget left,AidTarget right) {
		return Objects.equals(left.resolved().submodelId(),right.resolved().submodelId())
				&& left.resolved().owner().idShort().equals(right.resolved().owner().idShort())
				&& left.resolved().affordance().equals(right.resolved().affordance());
	}

	/**
	 * Status phase: status properties (read or subscribed) and events of the
	 * action's AID, the Lua that decides running/done/failed, and its time limit.
	 */
	private void compileStatus(MappingPlan mapping,AidTarget command,Plan plan,String routeId,RuntimeSettings runtime,
			Map<String,Object> trigger,List<DataSourceConfiguration> reads,List<Issue> issues) {
		String root="MappingConfigurations/value/"+mapping.listIndex();
		if(!mapping.statusSources().present()) {
			if(mapping.statusTransformation().present() || mapping.statusTimeout().present())
				issue(issues,mapping,root+"/StatusSources","StatusTransformation and StatusTimeout require StatusSources");
			return;
		}
		if(!twoPhase(mapping)) { issue(issues,mapping,root+"/StatusSources","StatusSources must not be empty");return; }
		if(!mapping.statusTransformation().present() || mapping.statusTransformation().value()==null)
			issue(issues,mapping,root+"/StatusTransformation","StatusSources require a StatusTransformation");
		statusTimeout(mapping,trigger,issues);
		List<Map<String,Object>> bindings=new ArrayList<>();
		List<EndpointPlan> sources=mapping.statusSources().value();
		for(int i=0;i<sources.size();i++) {
			EndpointPlan endpoint=sources.get(i);
			String path=root+"/StatusSources/"+endpoint.id();
			if(!statusSource(endpoint)
					|| !Objects.equals(((AidTarget)endpoint.target()).resolved().submodelId(),command.resolved().submodelId())) {
				issue(issues,mapping,path,"a StatusSource must be a property or event of the action's AID");continue;
			}
			DataSourceConfiguration read=source(endpoint,mapping,200000+i,plan,routeId,runtime,issues);
			if(read==null)continue;
			boolean event="event".equalsIgnoreCase(((AidTarget)endpoint.target()).resolved().affordance().kind());
			boolean polled="sync".equalsIgnoreCase(read.getDelivery());
			if(event && polled) { issue(issues,mapping,path,"an event has no current value to read; it can only be subscribed");continue; }
			reads.add(read);
			Map<String,Object> binding=new LinkedHashMap<>();binding.put("sourceId",endpoint.id());
			binding.put("dataSourceId",read.getUniqueId());
			binding.put("acquisition",event?"event":polled?"read":"observe");
			// An OPC UA monitored item always reports the current value first (OPC 10000-4 §5.12.1.4);
			// an MQTT subscription only does for a retained message.
			if(!event && !polled && ((AidTarget)endpoint.target()).resolved().owner().protocolFamilies().contains(WireModel.ProtocolFamily.OPCUA))
				binding.put("initialValue",true);
			if(polled)binding.put("pollingIntervalMs",
					read.getPollingIntervalMs()>0?read.getPollingIntervalMs():pollingMillis(mapping,issues).orElse(5000));
			bindings.add(binding);
		}
		trigger.put(ProtocolBridgeRouteConfiguration.STATUS_SOURCES,List.copyOf(bindings));
	}

	/** StatusTimeout (seconds), required with StatusSources. */
	private static void statusTimeout(MappingPlan mapping,Map<String,Object> trigger,List<Issue> issues) {
		String path="MappingConfigurations/value/"+mapping.listIndex()+"/StatusTimeout";
		if(!mapping.statusTimeout().present()) { issue(issues,mapping,path,"StatusSources require a StatusTimeout");return; }
		double seconds;
		try { seconds=Double.parseDouble(String.valueOf(mapping.statusTimeout().value())); }
		catch(RuntimeException invalid) { seconds=Double.NaN; }
		if(!Double.isFinite(seconds) || seconds<=0 || seconds*1000d>Long.MAX_VALUE || seconds*1000d<1) {
			issue(issues,mapping,path,"expected a positive timeout in seconds");return;
		}
		trigger.put(ProtocolBridgeRouteConfiguration.STATUS_TIMEOUT_MS,(long)(seconds*1000d));
	}

	/**
	 * Response phase. A ResponseSource is the command's output ({@code reply}),
	 * an element that is also a StatusSource ({@code status}: the value that
	 * ended the wait) or a property of the action's AID ({@code read}: read
	 * once). ResponseSinks addressing the device's AID are {@code device}
	 * writes, made before the reply.
	 */
	private void compileResponseBindings(MappingPlan mapping,EndpointPlan operation,AidTarget command,
			Plan plan,String routeId,RuntimeSettings runtime,Map<String,Object> trigger,
			List<DataSinkConfiguration> destinations,List<DataSourceConfiguration> reads,List<Issue> issues) {
		String path="MappingConfigurations/value/"+mapping.listIndex()+"/response";
		if(!mapping.responseTransformation().present() || mapping.responseTransformation().value()==null)
			issue(issues,mapping,path,"explicit response bindings require a ResponseTransformation");
		Map<List<Key>,String> statusIds=new LinkedHashMap<>();
		if(mapping.statusSources().value()!=null)
			for(EndpointPlan status:mapping.statusSources().value())statusIds.put(Aas.keys(status.reference()),status.id());
		List<Map<String,Object>> sourceBindings=new ArrayList<>();
		Set<String> sourcePaths=new LinkedHashSet<>();
		if(mapping.responseSources().present()) {
			if(mapping.responseSources().value()==null)issue(issues,mapping,path,"ResponseSources must not be null");
			else for(EndpointPlan endpoint:mapping.responseSources().value()) {
				Map<String,Object> binding=new LinkedHashMap<>();binding.put("sourceId",endpoint.id());
				String statusId=statusIds.get(Aas.keys(endpoint.reference()));
				if(statusId!=null) {
					binding.put("kind","status");binding.put("statusSourceId",statusId);
					sourceBindings.add(binding);continue;
				}
				if(endpoint.target() instanceof AidTarget target && sameAction(command,target)
						&& Aas.keys(endpoint.reference()).stream().anyMatch(key -> "output".equals(key.getValue()))) {
					String pointer=jsonPointer(target.resolved().schemaPath());
					if(!sourcePaths.add(pointer==null?"":pointer))issue(issues,mapping,path,"duplicate response member path");
					binding.put("kind","reply");binding.put("valuePath",pointer);
					sourceBindings.add(binding);continue;
				}
				if(endpoint.target() instanceof AidTarget target && Objects.equals(target.resolved().submodelId(),command.resolved().submodelId())
						&& "property".equalsIgnoreCase(target.resolved().affordance().kind())) {
					DataSourceConfiguration read=source(endpoint,mapping,400000+reads.size(),plan,routeId,runtime,issues);
					if(read==null)continue;
					if(!"sync".equalsIgnoreCase(read.getDelivery())) {
						issue(issues,mapping,path,"a response property is read once; a subscribed value can be used as a StatusSource");continue;
					}
					reads.add(read);
					binding.put("kind","read");binding.put("dataSourceId",read.getUniqueId());
					sourceBindings.add(binding);continue;
				}
				issue(issues,mapping,path,endpoint.target() instanceof AidTarget target && "event".equalsIgnoreCase(target.resolved().affordance().kind())
						? "an event is a ResponseSource only when it is also a StatusSource"
						: "ResponseSource must select the action output, a property of the action's AID or a StatusSource");
			}
		}
		if(sourcePaths.contains("") && sourcePaths.size()>1)issue(issues,mapping,path,"whole and member response sources overlap");
		if(sourceBindings.isEmpty())sourceBindings.add(Map.of("sourceId",operation.id(),"kind","reply"));
		trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_SOURCE_BINDINGS,List.copyOf(sourceBindings));
		String callerId=null;
		List<Map<String,Object>> sinkBindings=new ArrayList<>();
		if(mapping.responseSinks().present()) {
			if(mapping.responseSinks().value()==null)issue(issues,mapping,path,"ResponseSinks must not be null");
			else for(int i=0;i<mapping.responseSinks().value().size();i++) {
				EndpointPlan endpoint=mapping.responseSinks().value().get(i);
				if(endpoint.target() instanceof DirectTarget direct && direct.resolved().element() instanceof Operation) {
					DirectTarget original=(DirectTarget)operation.target();
					if(!direct.resolved().submodel().getId().equals(original.resolved().submodel().getId())
							|| !Aas.keys(endpoint.reference()).subList(1,Aas.keys(endpoint.reference()).size()).equals(
									Aas.keys(operation.reference()).subList(1,Aas.keys(operation.reference()).size())) || callerId!=null)
						issue(issues,mapping,path,"only one ResponseSink may select the exact caller Operation");
					else callerId=endpoint.id();
					continue;
				}
				DataSinkConfiguration sink=sink(endpoint,mapping,100000+i,plan,routeId,runtime,issues);
				if(sink==null)continue;
				payloadBinding(sink,endpoint,mapping,issues);
				destinations.add(sink);
				sinkBindings.add(sinkBinding(endpoint,sink));
			}
		}
		trigger.put(ProtocolBridgeRouteConfiguration.CALLER_REPLY_SINK_ID,callerId==null?operation.id():callerId);
		trigger.put(ProtocolBridgeRouteConfiguration.RESPONSE_SINK_BINDINGS,List.copyOf(sinkBindings));
	}

	/** One write target of an operation mapping: an AAS element, or a property of the device's AID ({@code device}). */
	private static Map<String,Object> sinkBinding(EndpointPlan endpoint,DataSinkConfiguration sink) {
		Map<String,Object> binding=new LinkedHashMap<>();binding.put("sinkId",endpoint.id());
		binding.put("dataSinkId",sink.getUniqueId());binding.put("schema",sink.getSchema());
		boolean aas=endpoint.target() instanceof DirectTarget;
		binding.put("aas",aas);binding.put("device",!aas);
		binding.put("jsonPayload",!aas && (sink instanceof HttpDataSinkConfiguration || sink instanceof MqttDataSinkConfiguration));
		if(!aas && endpoint.target() instanceof AidTarget target) {
			binding.put("valuePath",jsonPointer(target.resolved().schemaPath()));
			binding.put("payloadGroup",sink.getPayloadGroup());
			binding.put("payloadSchema",sink.getPayloadSchema());
		}
		return binding;
	}

	private static void validateCapabilities(MappingPlan mapping,List<Issue> issues) {
		if(mapping.sources().isEmpty() || mapping.sinks().isEmpty())issue(issues,mapping,
				"MappingConfigurations/value/"+mapping.listIndex(),"mapping requires nonempty sources and sinks");
		List<EndpointPlan> endpoints=new ArrayList<>(mapping.sources());endpoints.addAll(mapping.sinks());
		if(mapping.responseSources().value()!=null)endpoints.addAll(mapping.responseSources().value());
		if(mapping.responseSinks().value()!=null)endpoints.addAll(mapping.responseSinks().value());
		if(mapping.statusSources().value()!=null)endpoints.addAll(mapping.statusSources().value());
		for(EndpointPlan endpoint:endpoints)if(endpoint.target() instanceof AidTarget target) {
			String path="MappingConfigurations/value/"+mapping.listIndex()+"/endpoint/"+endpoint.id();
			List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
			if(families.size()!=1) { issue(issues,mapping,path,"ambiguous protocol bindings require an exact single binding");continue; }
			WireModel.ProtocolFamily family=families.get(0);
			boolean read=mapping.sources().contains(endpoint)
					|| mapping.responseSources().value()!=null && mapping.responseSources().value().contains(endpoint)
					|| mapping.statusSources().value()!=null && mapping.statusSources().value().contains(endpoint);
			for(SubmodelElement child:Aas.children(target.resolved().affordance().element())) {
				String name=child.getIdShort();
				if(!List.of("readOnly","writeOnly","synchronous","observable").contains(name==null?"":name) || Aas.value(child)==null)continue;
				String text=Aas.value(child);
				if(!List.of("true","false","1","0").contains(text.toLowerCase(java.util.Locale.ROOT))) {
					issue(issues,mapping,path+"/"+name,"capability must be a declared boolean");continue;
				}
				boolean enabled="true".equalsIgnoreCase(text) || "1".equals(text);
				if((read && "writeOnly".equals(name) || !read && "readOnly".equals(name)) && enabled)
					issue(issues,mapping,path+"/"+name,"mapping role contradicts authored capability");
				if("synchronous".equals(name) && !enabled && "action".equals(target.resolved().affordance().kind())
						&& !twoPhase(mapping) && !(family==WireModel.ProtocolFamily.MQTT && target.resolved().affordance().forms().stream()
								.anyMatch(form -> "true".equalsIgnoreCase(mqttTerm(form,"mqv_request_reply")))))
					issue(issues,mapping,path+"/"+name,"non-synchronous actions are not supported by the request/reply bridge");
			}
			if("event".equals(target.resolved().affordance().kind()))
				for(SubmodelElement child:Aas.children(target.resolved().affordance().element()))
					if(List.of("subscription","dataResponse","cancellation").contains(child.getIdShort()==null?"":child.getIdShort()))
						issue(issues,mapping,path+"/"+child.getIdShort(),"event subscription, response and cancellation payloads are not sent by the DMP");
			Set<String> terms=switch(family) {
				case MQTT -> Set.of("mqv_qos","mqv_retain","mqv_control_packet","mqv_request_reply","mqv_response_topic","mqv_reply_timeout","mqtt_responseTopic");
				case OPCUA -> Set.of("uav_browsePath");
				case MODBUS -> Set.of("modv_entity");
				default -> Set.of();
			};
			for(Form form:target.resolved().affordance().forms()) {
				for(SubmodelElement child:Aas.children(form.element())) {
					String name=child.getIdShort();
					if("security".equals(name) && !Aas.children(child).isEmpty())issue(issues,mapping,path,"authored form security selection is not represented by connector credentials");
					if(!Set.of("href","contentType","op","semanticProtocol","httpMethod","htv_methodName","security","response").contains(name==null?"":name)
							&& !terms.contains(name==null?"":name) && (Aas.value(child)!=null || !Aas.children(child).isEmpty()))
						issue(issues,mapping,path+"/forms/"+name,"unknown consumed form option");
				}
				Set<String> authored=new LinkedHashSet<>(form.bindingTerms().keySet());authored.addAll(form.mqttTerms().keySet());
				for(String term:authored)if(!terms.contains(term))issue(issues,mapping,path+"/forms/"+term,"authored protocol option is not represented by this connector");
				if(family==WireModel.ProtocolFamily.MQTT) {
					String packet=mqttTerm(form,"mqv_control_packet");
					if(packet!=null && !List.of("publish","subscribe").contains(packet.toLowerCase(java.util.Locale.ROOT)))
						issue(issues,mapping,path,"unsupported MQTT control packet");
					// Authored control packets describe the device-side leg (publish source, subscribe action).
					// Source retain metadata likewise describes the device publisher, not subscriber controls.
					mqttRetain(form,mapping,path,issues);
					for(SubmodelElement child:Aas.children(form.element()))if("response".equals(child.getIdShort())) {
						for(SubmodelElement option:Aas.children(child))if(Aas.value(option)!=null) {
							String name=option.getIdShort();String value=Aas.value(option);
							if("mqv_retain".equals(name) && !(value.equalsIgnoreCase("false") || value.equals("0")))
								issue(issues,mapping,path+"/forms/response","retained correlated replies are unsupported");
							else if("mqv_control_packet".equals(name) && !value.equalsIgnoreCase("publish"))
								issue(issues,mapping,path+"/forms/response","MQTT correlated response must describe publish");
							else if("contentType".equals(name) && !value.equalsIgnoreCase("application/json"))
								issue(issues,mapping,path+"/forms/response","MQTT correlated response requires JSON");
							else if(!List.of("href","mqv_retain","mqv_control_packet","contentType").contains(name==null?"":name))
								issue(issues,mapping,path+"/forms/response","authored reply options are not represented by the connector");
						}
					}
				}
				if(family!=WireModel.ProtocolFamily.HTTP && family!=WireModel.ProtocolFamily.IOLINK
						&& family!=WireModel.ProtocolFamily.MQTT) {
					// OPC 10101 §6.5.3: the default UA serialization is declared as application/octet-stream.
					if(form.contentType().present() && form.contentType().value()!=null
							&& !"application/json".equalsIgnoreCase(form.contentType().value())
							&& !(family==WireModel.ProtocolFamily.OPCUA && "application/octet-stream".equalsIgnoreCase(form.contentType().value())))
						issue(issues,mapping,path,"native connector supports decoded JSON values, not the authored content type");
					if(form.responseTopic().present())issue(issues,mapping,path,"reply topic is MQTT-only");
					String op=form.operation().value();
					// OPC 10101 §6.5.3: observeproperty is the UA Monitor service (a monitored item);
					// subscribeevent (DMP extension) an event monitored item.
					List<String> supportedOps=family==WireModel.ProtocolFamily.OPCUA
							? List.of("readproperty","writeproperty","observeproperty","subscribeevent","invokeaction")
							: List.of("readproperty","writeproperty","invokeaction");
					if(op!=null && !supportedOps.contains(op.toLowerCase(java.util.Locale.ROOT)))
						issue(issues,mapping,path,"unsupported protocol operation");
					String method=form.httpMethod().value();
				String expected="action".equals(target.resolved().affordance().kind())?"POST":read?"GET":"POST";
					if(method!=null && !expected.equalsIgnoreCase(method))issue(issues,mapping,path,"native binding method annotation contradicts the mapping role");
				}
				String op=form.operation().value();
				if(op!=null && (read && op.equalsIgnoreCase("writeproperty")
						|| !read && (op.equalsIgnoreCase("readproperty") || op.equalsIgnoreCase("observeproperty"))))
					issue(issues,mapping,path,"form operation contradicts mapping read/write role");
				if(read) {
					boolean managedObserve=family==WireModel.ProtocolFamily.MQTT
							|| family==WireModel.ProtocolFamily.OPCUA && op!=null
									&& (op.equalsIgnoreCase("observeproperty") || op.equalsIgnoreCase("subscribeevent"));
					if(op!=null && (op.equalsIgnoreCase("observeproperty") || op.equalsIgnoreCase("subscribeevent")) && !managedObserve)
						issue(issues,mapping,path,"declared Observe/event acquisition has no supported managed consumer");
				}
				if(mapping.sources().contains(endpoint)) {
					if(op!=null && op.equalsIgnoreCase("readproperty") && family==WireModel.ProtocolFamily.MQTT)
						issue(issues,mapping,path,"MQTT subscription queue is not fresh Read acquisition");
					if(endpoint.pollingInterval().present() && family==WireModel.ProtocolFamily.MQTT)
						issue(issues,mapping,path,"MQTT polling cannot be implemented as fresh Read");
				}
				if(!read && "event".equals(target.resolved().affordance().kind()))issue(issues,mapping,path,"event affordances cannot be writable destinations");
			}
			if(target.resolved().owner().endpointMetadata().value()!=null) {
				for(SubmodelElement child:Aas.children(target.resolved().owner().endpointMetadata().value()))
					if("security".equals(child.getIdShort()) && !Aas.children(child).isEmpty())
						issue(issues,mapping,path,"authored endpoint security selection is not represented by connector credentials");
			}
		}
	}

	private static void payloadBinding(DataSinkConfiguration sink,EndpointPlan endpoint,MappingPlan mapping,List<Issue> issues) {
		if(!(endpoint.target() instanceof AidTarget target))return;
		WireModel.Affordance affordance=target.resolved().affordance();
		var root=new AidTarget(new org.eclipse.digitaltwin.basyx.dmp.management.repository.AasReferenceResolver.AidTarget(
				target.resolved().owner(),affordance,affordance.schema(),List.of(),target.resolved().submodelId()));
		sink.setPayloadSchema(schemaForAidTarget(root,mapping,"MappingConfigurations/value/"+mapping.listIndex()+"/Sinks",issues));
		sink.setPayloadGroup(target.resolved().submodelId()+":"+target.resolved().owner().idShort()+":"+affordance.idShort().value());
	}

	private static String pathSegment(String value) {
		return java.net.URLEncoder.encode(value,StandardCharsets.UTF_8).replace("+","%20");
	}

	private static Map<String,String> schemaTypes(DataSchema schema) {
		if(schema==null)return Map.of();
		Map<String,String> types=new LinkedHashMap<>();
		schema.properties().forEach((name,child)->{
			String type=argumentType(child);
			if(type!=null)types.put(name,type);
		});
		return Map.copyOf(types);
	}
	private static String argumentType(DataSchema schema) {
		String type=schema.type().value();
		if(type==null)type=schemaType(schema.valueType().value());
		if(!"array".equals(type))return type;
		String item=schema.items().value()==null?null:argumentType(schema.items().value());
		return "array<"+(item==null?"unknown":item)+">";
	}

	private static List<String> schemaRequired(DataSchema schema) {
		return schema!=null && schema.required().present() && schema.required().value()!=null
				? schema.required().value():List.of();
	}

	private void addLua(List<DataTransformerConfiguration> transformers,String id,byte[] bytes,String identity,
			MappingPlan mapping,String path,List<Issue> issues) {
		try {
			LuaCompiledSource compiled=compiledLua(bytes,identity);
			transformers.add(new LuaTransformerConfiguration(id,compiled));
		} catch(RuntimeException failure) {
			issue(issues,mapping,path,"Lua source cannot be compiled: "+failure.getMessage());
		}
	}

	/** Compiles isolated per-mapping configurations suitable for DmpComponent.installBundle. */
	public BundleResult createBundles(Plan plan,RuntimeSettings runtime) {
		List<CompiledBundle> bundles=new ArrayList<>();
		List<Issue> issues=new ArrayList<>();
		for(MappingPlan mapping:plan.mappings()) {
			String bundleId=stableBundleId(plan,mapping,runtime);
			Plan single=new Plan(plan.aimcSubmodelId(),List.of(mapping));
			Result compiled;
			try { compiled=create(single,bundleId,runtime); }
			catch(RuntimeException invalid) {
				issue(issues,mapping,"MappingConfigurations/value/"+mapping.listIndex(),"mapping configuration preflight failed");continue;
			}
			if(compiled.issues().isEmpty()) {
				compiled.configuration().getRoutes().get(0).setRouteId(bundleId+":mapping");
				bundles.add(new CompiledBundle(mapping.identity(),mapping.listIndex(),bundleId,
						compiled.configuration().getRoutes().get(0).getRouteId(),compiled.configuration(),
						EffectiveFingerprint.of(mapping,runtime)));
			} else {
				issues.addAll(compiled.issues());
			}
		}
		return new BundleResult(bundles,issues);
	}

	/** Blocked resolution and connector outcomes keep their dependency sets beside valid bundles. */
	public List<DesiredBundle> createDesiredBundles(MappingPlanCompiler.DesiredPlan desired,RuntimeSettings runtime) {
		List<DesiredBundle> result=new ArrayList<>();
		for(var mapping:desired.mappings()) {
			if(!mapping.valid()) {
				result.add(new DesiredBundle(mapping,null,List.of(),EffectiveFingerprint.ofDesired(mapping,runtime,List.of())));
				continue;
			}
			var compiled=createBundles(new Plan(desired.aimcSubmodelId(),List.of(mapping.plan())),runtime);
			CompiledBundle bundle=compiled.bundles().isEmpty()?null:compiled.bundles().get(0);
			result.add(new DesiredBundle(mapping,bundle,compiled.issues(),bundle==null
					? EffectiveFingerprint.ofDesired(mapping,runtime,compiled.issues()) : bundle.fingerprint()));
		}
		return List.copyOf(result);
	}

	private static String stableBundleId(Plan plan,MappingPlan mapping,RuntimeSettings runtime) {
		String aimc=plan.aimcSubmodelId()==null?"":plan.aimcSubmodelId();
		String repository=runtime==null?"":runtime.aasRepositoryBaseUri().normalize().toString();
		String identity=repository+"\u0000"+aimc+"\u0000"+mapping.identity();
		try {
			byte[] digest=java.security.MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8));
			return "dmp-bundle-"+java.util.HexFormat.of().formatHex(digest);
		} catch(java.security.NoSuchAlgorithmException impossible) {
			throw new IllegalStateException("SHA-256 unavailable",impossible);
		}
	}

	private static DataSourceConfiguration source(EndpointPlan endpoint, MappingPlan mapping, int position,
			Plan plan, String routeId, RuntimeSettings runtime, List<Issue> issues) {
		String path = endpointPath(mapping, "Sources", position);
		if (endpoint.target() instanceof DirectTarget) {
			if(((DirectTarget)endpoint.target()).resolved().element() instanceof Operation) {
				issue(issues,mapping,path,"AAS Operation targets require a ProtocolBridge route and declared ingress contract");
				return null;
			}
			return aasSource(endpoint,mapping,position,(DirectTarget)endpoint.target(),runtime,issues);
		}
		AidTarget target = (AidTarget) endpoint.target();
		if("action".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"AID Action sources require Operation bridge compilation");
			return null;
		}
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if (families.equals(List.of(WireModel.ProtocolFamily.MQTT))) {
			return mqttSource(endpoint, mapping, position, target, plan, routeId, issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.OPCUA))) {
			return opcuaSource(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.MODBUS))) {
			return plc4xSource(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.BACNET))) {
			return plc4xSource(endpoint,mapping,position,target,issues);
		}
		if (!families.equals(List.of(WireModel.ProtocolFamily.HTTP))
				&& !families.equals(List.of(WireModel.ProtocolFamily.IOLINK))) {
			issue(issues,mapping,path,"unsupported or ambiguous source protocol family "+families);
			return null;
		}
		if (!selectHttpForm(target, mapping, path, issues)) return null;
		Form form = selectedForm(target);
		if (!validateForm(form, "GET", mapping, path, issues)) return null;
		URI uri = formUri(target,form, mapping, path, true, issues);
		if (uri == null) return null;
		if (!httpUri(uri, mapping, path, issues)) return null;
		String username = uri.getUserInfo() == null ? null : uri.getUserInfo().split(":", 2)[0];
		String[] credentials = uri.getUserInfo() == null ? new String[0] : uri.getUserInfo().split(":", 2);
		if (credentials.length == 1) {
			issue(issues, mapping, path + "/forms/href", "HTTP basic authentication requires both authored username and password");
			return null;
		}
		String password = credentials.length == 2 ? credentials[1] : null;
		URI credentialFreeUri;
		try { credentialFreeUri = new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()); }
		catch (java.net.URISyntaxException impossible) { throw new IllegalArgumentException("invalid projected endpoint URI", impossible); }
		HttpPollingDataSourceConfiguration result = new HttpPollingDataSourceConfiguration(
				entityId(mapping, "source", position), credentialFreeUri.toString(), effectivePort(uri), username, password);
		result.setSourceId(endpoint.id());
		result.setSchema(schemaForAidTarget(target,mapping,path,issues));
		result.setDelivery("sync");
		setValuePath(result,target);
		if (endpoint.pollingInterval().present()) {
			OptionalLong polling = endpointPollingMillis(endpoint.pollingInterval(), mapping, position, issues);
			if (polling.isPresent()) {
				result.setDelivery("sync");
				result.setPollingIntervalMs(polling.getAsLong());
			}
		}
		return result;
	}

	private static OptionalLong endpointPollingMillis(WireModel.Presence<String> presence,
			MappingPlan mapping, int position, List<Issue> issues) {
		Object value = presence.value();
		double seconds;
		try { seconds = value instanceof Number number ? number.doubleValue() : Double.parseDouble(String.valueOf(value)); }
		catch (RuntimeException invalid) {
			issue(issues, mapping, endpointPath(mapping,"Sources",position)+"/PollingInterval", "expected a positive numeric interval in authored seconds");
			return OptionalLong.empty();
		}
		if (!Double.isFinite(seconds) || seconds <= 0 || seconds * 1000d > Long.MAX_VALUE || seconds * 1000d < 1) {
			issue(issues, mapping, endpointPath(mapping,"Sources",position)+"/PollingInterval", "poll interval is outside the positive runtime millisecond range");
			return OptionalLong.empty();
		}
		return OptionalLong.of((long)(seconds*1000d));
	}

	private static DataSinkConfiguration sink(EndpointPlan endpoint, MappingPlan mapping, int position,
			Plan plan, String routeId, RuntimeSettings runtime, List<Issue> issues) {
		String path = endpointPath(mapping, "Sinks", position);
		if (endpoint.target() instanceof DirectTarget) {
			if(((DirectTarget)endpoint.target()).resolved().element() instanceof Operation) {
				issue(issues,mapping,path,"AAS Operation sinks are not supported (chained actions are unsafe)");
				return null;
			}
			return aasSink(endpoint,mapping,position,(DirectTarget)endpoint.target(),runtime,issues);
		}
		AidTarget target = (AidTarget) endpoint.target();
		if("action".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"AID Action sinks require Operation bridge compilation");
			return null;
		}
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if (families.equals(List.of(WireModel.ProtocolFamily.MQTT))) {
			return mqttSink(endpoint, mapping, position, target, plan, routeId, issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.OPCUA))) {
			return opcuaSink(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.MODBUS))) {
			return plc4xSink(endpoint,mapping,position,target,issues);
		}
		if (families.equals(List.of(WireModel.ProtocolFamily.BACNET))) {
			return plc4xSink(endpoint,mapping,position,target,issues);
		}
		if (!families.equals(List.of(WireModel.ProtocolFamily.HTTP))
				&& !families.equals(List.of(WireModel.ProtocolFamily.IOLINK))) {
			issue(issues,mapping,path,"unsupported or ambiguous sink protocol family "+families);
			return null;
		}
		if (!selectHttpForm(target, mapping, path, issues)) return null;
		Form form = selectedForm(target);
		if (!validateForm(form, "POST", mapping, path, issues)) return null;
		URI uri = formUri(target,form, mapping, path, false, issues);
		if (uri == null) return null;
		if (!httpUri(uri, mapping, path, issues)) return null;
		if (uri.getUserInfo() != null) {
			issue(issues, mapping, path + "/forms/href", "HTTP sink configuration has no auth fields; refusing to discard authored credentials");
			return null;
		}
		HttpDataSinkConfiguration result = new HttpDataSinkConfiguration(entityId(mapping, "sink", position),
				uri.getScheme(), uri.getHost(), uri.getPath(), effectivePort(uri));
		result.setQuery(uri.getQuery());
		result.setSinkId(endpoint.id());
		result.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(result,target);
		return result;
	}

	private static DataSourceConfiguration aasSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			DirectTarget target,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if(runtime==null) { issue(issues,mapping,path,"AAS source compilation requires explicit repository runtime settings");return null; }
		String elementPath=aasElementPath(endpoint,mapping,path,issues);
		if(elementPath==null)return null;
		String submodelEndpoint=submodelEndpoint(target.resolved().submodel().getId(),runtime,mapping,path,issues);
		if(submodelEndpoint==null)return null;
		if(target.resolved().element() instanceof Property && !endpoint.pollingInterval().present()
				&& !mapping.defaultPollingInterval().present()) {
			// Without an authored PollingInterval a Property follows the repository's change events.
			String idShortPath=AasReferenceResolver.idShortPath(Aas.keys(endpoint.reference()));
			AasEventDataSourceConfiguration source=new AasEventDataSourceConfiguration(entityId(mapping,"source",position),
					target.resolved().submodel().getId(),idShortPath,submodelEndpoint+"/submodel-elements/"
					+java.net.URLEncoder.encode(idShortPath,StandardCharsets.UTF_8).replace("+","%20"));
			source.setSourceId(endpoint.id());
			source.setSchema(schemaFromAasElement(target.resolved().element(),mapping,path,issues));
			return hasIssue(issues,mapping)?null:source;
		}
		URI base=runtime.aasRepositoryBaseUri();
		AASPollingDataSourceConfiguration source=new AASPollingDataSourceConfiguration(entityId(mapping,"source",position),
				base.getHost(),effectivePort(base),elementPath,submodelEndpoint);
		source.setSourceId(endpoint.id());
		source.setElementType(Aas.modelType(target.resolved().element()));
		source.setSchema(schemaFromAasElement(target.resolved().element(),mapping,path,issues));
		source.setDelivery("sync");
		if(endpoint.pollingInterval().present()) {
			OptionalLong interval=endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues);
			if(interval.isPresent())source.setPollingIntervalMs(interval.getAsLong());
		}
		return hasIssue(issues,mapping)?null:source;
	}

	private static DataSinkConfiguration aasSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			DirectTarget target,RuntimeSettings runtime,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		if(runtime==null) { issue(issues,mapping,path,"AAS sink compilation requires explicit repository runtime settings");return null; }
		String elementPath=aasElementPath(endpoint,mapping,path,issues);
		if(elementPath==null)return null;
		String submodelEndpoint=submodelEndpoint(target.resolved().submodel().getId(),runtime,mapping,path,issues);
		if(submodelEndpoint==null)return null;
		AASDatasinkConfiguration sink=new AASDatasinkConfiguration(submodelEndpoint,elementPath,
				entityId(mapping,"sink",position));
		sink.setSinkId(endpoint.id());
		sink.setSchema(schemaFromAasElement(target.resolved().element(),mapping,path,issues));
		sink.setValuePath(endpoint.id());
		return hasIssue(issues,mapping)?null:sink;
	}

	private static String aasElementPath(EndpointPlan endpoint,MappingPlan mapping,String path,List<Issue> issues) {
		List<Key> keys=Aas.keys(endpoint.reference());
		if(keys.size()<2 || !"Submodel".equals(Aas.keyType(keys.get(0)))) {
			issue(issues,mapping,path,"AAS reference must address an exact submodel element");return null;
		}
		return keys.subList(1,keys.size()).stream().map(Key::getValue)
				.collect(java.util.stream.Collectors.joining("/"));
	}

	private static String submodelEndpoint(String submodelId,RuntimeSettings runtime,MappingPlan mapping,
			String path,List<Issue> issues) {
		try {
			String base=runtime.aasRepositoryBaseUri().toString().replaceAll("/+$","");
			return base+"/submodels/"+AasRepositoryClient.base64UrlIdentifier(submodelId);
		} catch(RuntimeException invalid) {
			issue(issues,mapping,path,"AAS submodel endpoint cannot be encoded safely");return null;
		}
	}

	private static Map<String,Object> schemaFromAasElement(SubmodelElement element,MappingPlan mapping,
			String path,List<Issue> issues) {
		switch(Aas.modelType(element)) {
		case "Property": {
			String name=Aas.xsd(((Property)element).getValueType());
			String type=schemaType(name);
			if(type==null) { issue(issues,mapping,path,"unsupported AAS Property valueType "+name);return Map.of(); }
			return Map.of("type",type);
		}
		case "SubmodelElementCollection": {
			Map<String,Object> properties=new LinkedHashMap<>();
			for(SubmodelElement child:Aas.children(element)) {
				if(child.getIdShort()==null) {
					issue(issues,mapping,path,"AAS collection child has no authored idShort");return Map.of();
				}
				Map<String,Object> childSchema=schemaFromAasElement(child,mapping,path+"/"+child.getIdShort(),issues);
				properties.put(child.getIdShort(),childSchema);
			}
			return Map.of("type","object","properties",properties);
		}
		case "SubmodelElementList": {
			SubmodelElementList list=(SubmodelElementList)element;
			if(list.getTypeValueListElement()==AasSubmodelElements.PROPERTY && list.getValueTypeListElement()!=null) {
				String name=Aas.xsd(list.getValueTypeListElement());
				String type=schemaType(name);
				if(type==null) { issue(issues,mapping,path,"unsupported AAS list valueTypeListElement "+name);return Map.of(); }
				return Map.of("type","array","items",Map.of("type",type));
			}
			if(Aas.children(element).isEmpty()) { issue(issues,mapping,path,"AAS list schema has no typed member");return Map.of(); }
			return Map.of("type","array","items",schemaFromAasElement(Aas.children(element).get(0),mapping,path+"/items",issues));
		}
		default:
			issue(issues,mapping,path,"AAS element type cannot be represented as a JSON schema: "+Aas.modelType(element));
			return Map.of();
		}
	}

	private static String schemaType(String valueType) {
		if(valueType==null)return null;
		return switch(valueType) {
			case "xs:string","xs:normalizedString","xs:token","xs:anyURI" -> "string";
			case "xs:boolean" -> "boolean";
			case "xs:byte","xs:short","xs:int","xs:integer","xs:long","xs:unsignedByte","xs:unsignedShort","xs:unsignedInt","xs:unsignedLong" -> "integer";
			case "xs:float","xs:double","xs:decimal" -> "number";
			default -> null;
		};
	}

	/**
	 * Operation variables are Properties with a supported valueType, collections
	 * of uniquely named variables, or lists whose members all have the declared
	 * list type (the first member, or valueTypeListElement, is the member shape).
	 */
	private static boolean callerElementSupported(SubmodelElement element,boolean named) {
		if(named && (element.getIdShort()==null || element.getIdShort().isBlank()))return false;
		if(element instanceof Property property)return schemaType(Aas.xsd(property.getValueType()))!=null;
		if(element instanceof SubmodelElementCollection collection) {
			Set<String> names=new HashSet<>();
			for(SubmodelElement child:Aas.nonNull(collection.getValue()))
				if(!callerElementSupported(child,true) || !names.add(child.getIdShort()))return false;
			return true;
		}
		if(element instanceof SubmodelElementList list) {
			SubmodelElement item=OperationVariableValues.itemTemplate(list);
			if(item==null)return false;
			List<SubmodelElement> members=new ArrayList<>(Aas.nonNull(list.getValue()));
			if(members.isEmpty())members.add(item);
			for(SubmodelElement member:members)
				if(!listMemberMatches(list,member) || !callerElementSupported(member,false))return false;
			return true;
		}
		return false;
	}

	/** AASd-108/109: a member has the list's declared element type and, for Properties, value type. */
	private static boolean listMemberMatches(SubmodelElementList list,SubmodelElement member) {
		AasSubmodelElements declared=list.getTypeValueListElement();
		boolean kind=declared==null || switch(declared) {
			case PROPERTY -> member instanceof Property;
			case SUBMODEL_ELEMENT_COLLECTION -> member instanceof SubmodelElementCollection;
			case SUBMODEL_ELEMENT_LIST -> member instanceof SubmodelElementList;
			default -> false;
		};
		return kind && (!(member instanceof Property property) || list.getValueTypeListElement()==null
				|| list.getValueTypeListElement()==property.getValueType());
	}

	private static boolean hasIssue(List<Issue> issues, MappingPlan mapping) {
		return issues.stream().anyMatch(issue -> issue.mappingId().equals(mapping.identity()));
	}

	private static boolean validateForm(Form form, String expectedMethod, MappingPlan mapping, String path,
			List<Issue> issues) {
		// Existing HTTP source/sink components have fixed GET/POST methods when
		// the optional WOT htv_methodName binding is not authored.
		if (form.httpMethod().present() && (form.httpMethod().value()==null
				|| !expectedMethod.equalsIgnoreCase(form.httpMethod().value()))) {
			issue(issues, mapping, path + "/forms/httpMethod", "HTTP endpoint method must be " + expectedMethod);
			return false;
		}
		if (form.semanticProtocol().present() && form.semanticProtocol().value()!=null
				&& !form.semanticProtocol().value().contains("http")) {
			issue(issues,mapping,path+"/forms/semanticProtocol","HTTP form declares a different protocol");
			return false;
		}
		if (form.operation().present() && form.operation().value()!=null) {
			String op=form.operation().value().toLowerCase(java.util.Locale.ROOT);
			if(!(expectedMethod.equals("GET") && List.of("readproperty","observeproperty").contains(op))
					&& !(expectedMethod.equals("POST") && List.of("writeproperty","invokeaction").contains(op))) {
				issue(issues,mapping,path+"/forms/op","HTTP form operation is not represented by the connector route");
				return false;
			}
		}
		if (form.contentType().present() && form.contentType().value()!=null
				&& !"application/json".equalsIgnoreCase(form.contentType().value())) {
			issue(issues,mapping,path+"/forms/contentType","HTTP connector only binds JSON request/response bodies");
			return false;
		}
		if(!form.mqttTerms().isEmpty() || form.responseTopic().present()) {
			issue(issues,mapping,path+"/forms","HTTP form contains MQTT-only binding terms");
			return false;
		}
		return true;
	}

	private static boolean selectHttpForm(AidTarget target, MappingPlan mapping, String path, List<Issue> issues) {
		List<WireModel.ProtocolFamily> families = target.resolved().owner().protocolFamilies();
		if (families.size() != 1 || (families.get(0) != WireModel.ProtocolFamily.HTTP
				&& families.get(0) != WireModel.ProtocolFamily.IOLINK)) {
			issue(issues, mapping, path, "unsupported or ambiguous protocol family " + families);
			return false;
		}
		if (target.resolved().affordance().forms().size() != 1) {
			issue(issues, mapping, path + "/forms", "expected exactly one authored form; missing or ambiguous forms are rejected");
			return false;
		}
		return true;
	}

	private static Form selectedForm(AidTarget target) { return target.resolved().affordance().forms().get(0); }
	private static URI formUri(AidTarget target, Form form, MappingPlan mapping, String path,
			boolean allowBaseForEmptyHref,List<Issue> issues) {
		if (!form.href().present() || form.href().value() == null || form.href().value().isBlank()) {
			WireModel.Presence<SubmodelElement> endpoint=target.resolved().owner().endpointMetadata();
			String base=endpoint.present() && endpoint.value()!=null?elementString(endpoint.value(),"base"):null;
			if(allowBaseForEmptyHref && base!=null && !base.isBlank()) {
				try { return URI.create(base); }
				catch(IllegalArgumentException malformed) { issue(issues,mapping,path+"/EndpointMetadata/base","invalid HTTP endpoint base URI");return null; }
			}
			issue(issues, mapping, path + "/forms/href", "required endpoint href is absent"); return null;
		}
		try {
			URI href=URI.create(form.href().value());
			if(href.isAbsolute())return href;
			WireModel.Presence<SubmodelElement> metadata=target.resolved().owner().endpointMetadata();
			String base=metadata.present() && metadata.value()!=null ? elementString(metadata.value(),"base") : null;
			if(base==null || base.isBlank()) {
				issue(issues,mapping,path+"/EndpointMetadata/base","relative HTTP href requires an authored endpoint base");
				return null;
			}
			URI endpoint=URI.create(base);
			if(!("http".equalsIgnoreCase(endpoint.getScheme()) || "https".equalsIgnoreCase(endpoint.getScheme()))
					|| endpoint.getHost()==null) {
				issue(issues,mapping,path+"/EndpointMetadata/base","relative HTTP href requires an http(s) endpoint base");
				return null;
			}
			String prefix=endpoint.getPath()==null?"":endpoint.getPath().replaceAll("/+$","");
			String suffix=href.getPath()==null?"":href.getPath().replaceAll("^/+","");
			String fullPath=prefix+(suffix.isEmpty()?"":"/"+suffix);
			String query=href.getRawQuery();
			if(query==null)query=endpoint.getRawQuery();
			if(query!=null && endpoint.getRawQuery()!=null && href.getRawQuery()!=null) {
				issue(issues,mapping,path+"/forms/href","query parameters cannot be declared in both HTTP base and form href");
				return null;
			}
			return new URI(endpoint.getScheme(),endpoint.getUserInfo(),endpoint.getHost(),endpoint.getPort(),
					fullPath,query,href.getRawFragment());
		} catch (IllegalArgumentException | java.net.URISyntaxException malformed) {
			issue(issues, mapping, path + "/forms/href", "endpoint href is not a valid URI");
			return null;
		}
	}
	private static boolean httpUri(URI uri, MappingPlan mapping, String path, List<Issue> issues) {
		if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
				|| uri.getHost() == null || uri.getHost().isBlank() || uri.getRawFragment()!=null) {
			issue(issues, mapping, path + "/forms/href", "HTTP binding requires an absolute http(s) URI with an endpoint host");
			return false;
		}
		return true;
	}

	private static String elementString(SubmodelElement parent,String idShort) {
		SubmodelElement child=Aas.child(parent,idShort);
		return child==null?null:Aas.value(child);
	}

	private record MqttConnection(String host,int port,String prefix,String username,String password) { }

	private static MqttConnection mqttConnection(AidTarget target,MappingPlan mapping,String path,List<Issue> issues) {
		WireModel.Presence<SubmodelElement> metadata=target.resolved().owner().endpointMetadata();
		String base=metadata.present() && metadata.value()!=null ? elementString(metadata.value(),"base") : null;
		if(base==null || base.isBlank()) { issue(issues,mapping,path+"/EndpointMetadata/base","MQTT broker base URI is required"); return null; }
		URI uri;
		try { uri=URI.create(base); } catch(IllegalArgumentException malformed) {
			issue(issues,mapping,path+"/EndpointMetadata/base","invalid MQTT broker base URI"); return null;
		}
		if(!"mqtt".equalsIgnoreCase(uri.getScheme()) || uri.getHost()==null || uri.getPort()<1
				|| uri.getRawQuery()!=null || uri.getRawFragment()!=null) {
			issue(issues,mapping,path+"/EndpointMetadata/base","MQTT requires an mqtt://host:port base without query or fragment");
			return null;
		}
		String username=null,password=null;
		if(uri.getUserInfo()!=null) {
			int separator=uri.getUserInfo().indexOf(':');
			if(separator<1 || separator==uri.getUserInfo().length()-1) {
				issue(issues,mapping,path+"/EndpointMetadata/base","MQTT userinfo must contain a username and password"); return null;
			}
			username=uri.getUserInfo().substring(0,separator); password=uri.getUserInfo().substring(separator+1);
		}
		return new MqttConnection(uri.getHost(),uri.getPort(),uri.getPath(),username,password);
	}

	private static String mqttTopic(MqttConnection base,Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String href=form.href().present()?form.href().value():null;
		return mqttTopic(base,href,mapping,path,issues);
	}

	private static String mqttTopic(MqttConnection base,String href,MappingPlan mapping,String path,List<Issue> issues) {
		if(href==null || href.isBlank()) {
			issue(issues,mapping,path+"/forms/href","MQTT topic href is required"); return null;
		}
		URI pathUri;
		try { pathUri=URI.create(href); } catch(IllegalArgumentException malformed) {
			issue(issues,mapping,path+"/forms/href","invalid MQTT topic href"); return null;
		}
		if(pathUri.isAbsolute() || pathUri.getPath()==null || pathUri.getRawQuery()!=null || pathUri.getRawFragment()!=null) {
			issue(issues,mapping,path+"/forms/href","MQTT href must be a relative topic path"); return null;
		}
		String prefix=base.prefix()==null?"":base.prefix().replaceAll("^/+|/+$","");
		String suffix=pathUri.getPath().replaceAll("^/+|/+$","");
		String topic=prefix.isEmpty()?suffix:(suffix.isEmpty()?prefix:prefix+"/"+suffix);
		if(topic.isBlank()) { issue(issues,mapping,path+"/forms/href","MQTT topic must not be empty"); return null; }
		return topic;
	}

	private static Form mqttForm(AidTarget target,MappingPlan mapping,String path,String leg,List<Issue> issues) {
		return mqttForm(target,mapping,path,leg,false,issues);
	}
	private static Form mqttForm(AidTarget target,MappingPlan mapping,String path,String leg,boolean bridge,List<Issue> issues) {
		List<WireModel.ProtocolFamily> families=target.resolved().owner().protocolFamilies();
		if(!families.equals(List.of(WireModel.ProtocolFamily.MQTT))) {
			issue(issues,mapping,path,"MQTT requires a single unambiguous MQTT binding, found "+families); return null;
		}
		List<Form> forms=target.resolved().affordance().forms();
		if(forms.size()!=1) { issue(issues,mapping,path+"/forms","MQTT connector requires exactly one form"); return null; }
		Form form=forms.get(0);
		String operation=form.operation().value();
		if(operation!=null && !operation.isBlank()) {
			boolean supported=bridge && operation.equalsIgnoreCase("invokeaction")
					|| (leg.equals("source")
							? List.of("subscribeevent","observeproperty","readproperty").contains(operation.toLowerCase(java.util.Locale.ROOT))
							: List.of("publishproperty","writeproperty").contains(operation.toLowerCase(java.util.Locale.ROOT)));
			if(!supported) { issue(issues,mapping,path+"/forms/op","unsupported MQTT "+leg+" operation "+operation); return null; }
		}
		if(form.contentType().present()) { issue(issues,mapping,path+"/forms/contentType","MQTT content type is not represented by the selected connector version"); return null; }
		return form;
	}

	private static boolean hasMqttReplyTerms(Form form) {
		return form.responseTopic().present() || form.mqttTerms().containsKey("mqv_request_reply")
				|| form.mqttTerms().containsKey("mqv_response_topic") || form.mqttTerms().containsKey("mqtt_responseTopic");
	}

	private static String mqttTerm(Form form,String name) {
		return form.mqttTerms().get(name);
	}

	private static Integer mqttQos(Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String value=mqttTerm(form,"mqv_qos");
		if(value==null)return null;
		try { int qos=Integer.parseInt(value); if(qos>=0 && qos<=2)return qos; } catch(NumberFormatException ignored) { }
		issue(issues,mapping,path+"/forms/mqv_qos","MQTT QoS must be between 0 and 2");
		return null;
	}

	private static Boolean mqttRetain(Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String value=mqttTerm(form,"mqv_retain"); if(value==null)return null;
		if("true".equalsIgnoreCase(value) || "1".equals(value))return true;
		if("false".equalsIgnoreCase(value) || "0".equals(value))return false;
		issue(issues,mapping,path+"/forms/mqv_retain","MQTT retain must be true, false, 1 or 0"); return null;
	}

	private static String stableMqttClientId(Plan plan,String routeId,MappingPlan mapping,String endpoint,String leg) {
		String aimc=plan.aimcSubmodelId()==null?routeId:plan.aimcSubmodelId();
		String identity=routeId+"\u0000"+aimc+"\u0000"+mapping.identity()+"\u0000"+leg+"\u0000"+endpoint;
		try { return "dmp-"+Base64.getUrlEncoder().withoutPadding().encodeToString(
				java.security.MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8))); }
		catch(java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 unavailable",impossible); }
	}

	private static DataSourceConfiguration mqttSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,Plan plan,String routeId,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		Form form=mqttForm(target,mapping,path,"source",issues);
		if(form==null)return null;
		if(hasMqttReplyTerms(form)) { issue(issues,mapping,path+"/forms","MQTT request/reply sources require an Operation route"); return null; }
		MqttConnection connection=mqttConnection(target,mapping,path,issues);
		String topic=connection==null?null:mqttTopic(connection,form,mapping,path,issues);
		Integer qos=mqttQos(form,mapping,path,issues);
		if(connection==null || topic==null || hasIssue(issues,mapping))return null;
		MqttDataSourceConfiguration source=new MqttDataSourceConfiguration(entityId(mapping,"source",position),
				connection.host(),connection.port(),stableMqttClientId(plan,routeId,mapping,endpoint.id(),"source"),
				topic,connection.username()==null?null:new BasicCredential(connection.username(),connection.password()));
		source.setSourceId(endpoint.id()); source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		if(qos!=null)source.setQos(qos);
		if(endpoint.pollingInterval().present()) {
			OptionalLong interval=endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues);
			if(interval.isPresent()) { source.setDelivery("sync"); source.setPollingIntervalMs(interval.getAsLong()); }
		}
		return hasIssue(issues,mapping)?null:source;
	}

	private static DataSinkConfiguration mqttSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,Plan plan,String routeId,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		Form form=mqttForm(target,mapping,path,"sink",issues); if(form==null)return null;
		if("action".equalsIgnoreCase(target.resolved().affordance().kind())) { issue(issues,mapping,path,"MQTT InvokeAction needs ProtocolBridge command compilation"); return null; }
		if(hasMqttReplyTerms(form)) { issue(issues,mapping,path+"/forms","MQTT request/reply sink needs an Operation route"); return null; }
		MqttConnection connection=mqttConnection(target,mapping,path,issues);
		String topic=connection==null?null:mqttTopic(connection,form,mapping,path,issues);
		Integer qos=mqttQos(form,mapping,path,issues); Boolean retain=mqttRetain(form,mapping,path,issues);
		if(connection==null || topic==null || hasIssue(issues,mapping))return null;
		MqttDataSinkConfiguration sink=new MqttDataSinkConfiguration(entityId(mapping,"sink",position),connection.host(),
				connection.port(),topic,stableMqttClientId(plan,routeId,mapping,endpoint.id(),"sink"),
				connection.username()==null?"":connection.username(),connection.password()==null?"":connection.password());
		sink.setSinkId(endpoint.id()); sink.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(sink,target);
		if(qos!=null)sink.setQos(qos); if(retain!=null)sink.setRetained(retain);
		return sink;
	}

	private record ProtocolBase(String host,int port,String servicePath,String username,String password,URI uri) { }

	private static DataSourceConfiguration opcuaSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if("event".equalsIgnoreCase(target.resolved().affordance().kind()))return opcuaEventSource(endpoint,mapping,position,target,issues);
		if(!"property".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"OPC UA actions require a Method bridge");return null;
		}
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		ProtocolBase base=protocolBase(target,"opc.tcp",mapping,path,issues);if(base==null)return null;
		String node=opcuaNode(form,mapping,path,issues);if(node==null)return null;
		Map<String,String> configuration=new LinkedHashMap<>();
		OptionalLong interval=endpoint.pollingInterval().present()?endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues):OptionalLong.empty();
		// observeproperty (OPC 10101 §6.4.1) is a monitored item: the server samples the variable at the
		// sampling interval and reports changes; PollingInterval sets both sampling and publishing interval.
		boolean observed="observeproperty".equalsIgnoreCase(form.operation().value());
		if(interval.isPresent())configuration.put("requestedPublishingInterval",Long.toString(interval.getAsLong()));
		if(interval.isPresent() && observed)configuration.put("samplingInterval",Long.toString(interval.getAsLong()));
		OpcuaDataSourceConfiguration source=new OpcuaDataSourceConfiguration(entityId(mapping,"source",position),
				base.host(),base.port(),base.servicePath(),node,base.username(),base.password(),configuration);
		source.setSourceId(endpoint.id()); source.setDelivery(observed?"async":"sync");
		source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		if(interval.isPresent() && !observed) source.setPollingIntervalMs(interval.getAsLong());
		return hasIssue(issues,mapping)?null:source;
	}

	/** A field name of an event notification, and one qualified name of a browse path ({@code 2:Name}). */
	private static final java.util.regex.Pattern EVENT_FIELD_NAME=java.util.regex.Pattern.compile("[A-Za-z][A-Za-z0-9_-]*");
	private static final java.util.regex.Pattern EVENT_PATH_SEGMENT=java.util.regex.Pattern.compile("(?:\\d+:)?[^/:,=()]+");

	/**
	 * The events of an OPC UA notifier (DMP extension; OPC 10101 v1.00 has no event vocabulary):
	 * the form {@code href} names the notifier, each property of the {@code data} schema is one
	 * select clause (its {@code uav_eventField} browse path, or its own name in namespace 0), and
	 * the affordance-level {@code uav_eventType} is the where clause (OfType). PollingInterval sets
	 * the publishing interval. Delivered like an MQTT event: one field map per notification.
	 */
	private static DataSourceConfiguration opcuaEventSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		String op=form.operation().value();
		if(op!=null && !"subscribeevent".equalsIgnoreCase(op)) { issue(issues,mapping,path+"/forms/op","an OPC UA event is subscribed (op subscribeevent)");return null; }
		ProtocolBase base=protocolBase(target,"opc.tcp",mapping,path,issues);if(base==null)return null;
		String notifier=opcuaNode(form,mapping,path,issues);if(notifier==null)return null;
		Presence<WireModel.DataSchema> data=target.resolved().affordance().schema();
		if(!data.present() || data.value().properties().isEmpty()
				|| data.value().type().present() && !"object".equals(data.value().type().value())) {
			issue(issues,mapping,path+"/data","an OPC UA event needs an object data schema whose properties select the event fields");return null;
		}
		List<String> fields=new ArrayList<>();
		for(Map.Entry<String,WireModel.DataSchema> field:data.value().properties().entrySet()) {
			String name=field.getKey();
			String browsePath=Aas.children(field.getValue().element()).stream()
					.filter(child -> "uav_eventField".equals(child.getIdShort())).map(Aas::value).findFirst().orElse(name);
			boolean valid=EVENT_FIELD_NAME.matcher(name).matches() && browsePath!=null && !browsePath.isBlank()
					&& java.util.Arrays.stream(browsePath.replaceFirst("^/","").split("/",-1)).allMatch(segment -> EVENT_PATH_SEGMENT.matcher(segment).matches());
			if(!valid) { issue(issues,mapping,path+"/data/properties/"+name,"an event field needs a browse path of qualified names, e.g. 0:ToState/0:Id");return null; }
			fields.add(name+"="+browsePath.replaceFirst("^/",""));
		}
		String eventType=Aas.children(target.resolved().affordance().element()).stream()
				.filter(child -> "uav_eventType".equals(child.getIdShort())).map(Aas::value).findFirst().orElse(null);
		if(eventType!=null && (eventType.isBlank() || eventType.contains(")"))) {
			issue(issues,mapping,path+"/uav_eventType","uav_eventType must be an EventType NodeId");return null;
		}
		Map<String,String> configuration=new LinkedHashMap<>();
		configuration.put("eventFields","RAW("+String.join(",",fields)+")");
		if(eventType!=null)configuration.put("eventType","RAW("+eventType.trim()+")");
		OptionalLong interval=endpoint.pollingInterval().present()?endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues):OptionalLong.empty();
		if(interval.isPresent())configuration.put("requestedPublishingInterval",Long.toString(interval.getAsLong()));
		OpcuaDataSourceConfiguration source=new OpcuaDataSourceConfiguration(entityId(mapping,"source",position),
				base.host(),base.port(),base.servicePath(),notifier,base.username(),base.password(),configuration);
		source.setSourceId(endpoint.id()); source.setDelivery("async");
		source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		return hasIssue(issues,mapping)?null:source;
	}

	/**
	 * The Object owning a UA Method: {@code uav_componentOf} on the action affordance (OPC 10101
	 * §6.5.4, §7.1.3), a NodeId or a list holding exactly one; a Call addresses one owner.
	 */
	private static String componentOf(AidTarget target,MappingPlan mapping,String path,List<Issue> issues) {
		SubmodelElement term=Aas.children(target.resolved().affordance().element()).stream()
				.filter(child -> "uav_componentOf".equals(child.getIdShort())).findFirst().orElse(null);
		List<String> owners=new ArrayList<>();
		if(term instanceof org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList list) {
			for(SubmodelElement member:Aas.children(list))if(Aas.value(member)!=null)owners.add(Aas.value(member));
		} else if(term!=null && Aas.value(term)!=null) owners.add(Aas.value(term));
		if(owners.size()!=1 || owners.get(0).isBlank()) {
			issue(issues,mapping,path,"OPC UA Method needs uav_componentOf on the action naming its one owning Object");return null;
		}
		return owners.get(0);
	}

	private static DataSinkConfiguration opcuaSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		if(!"property".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"OPC UA Method invocation requires ProtocolBridge command compilation");return null;
		}
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		ProtocolBase base=protocolBase(target,"opc.tcp",mapping,path,issues);if(base==null)return null;
		String node=opcuaNode(form,mapping,path,issues);if(node==null)return null;
		OpcuaDataSinkConfiguration sink=new OpcuaDataSinkConfiguration(entityId(mapping,"sink",position),base.host(),
				base.port(),base.servicePath(),node,base.username(),base.password(),new LinkedHashMap<>());
		sink.setSinkId(endpoint.id()); sink.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(sink,target);
		return sink;
	}

	private static DataSourceConfiguration plc4xSource(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sources",position);
		if(!"property".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"Modbus sources must address a property endpoint");return null;
		}
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		if(formTerm(form,"modv_function")!=null) { issue(issues,mapping,path+"/forms/modv_function","PLC4X connector configuration does not represent modv_function");return null; }
		if(formTerm(form,"modv_zeroBasedAddressing")!=null) { issue(issues,mapping,path+"/forms/modv_zeroBasedAddressing","PLC4X connector configuration does not represent zero-based addressing mode");return null; }
		String scheme=target.resolved().owner().protocolFamilies().contains(WireModel.ProtocolFamily.BACNET)?"bacnet":"tcp";
		ProtocolBase base=protocolBase(target,scheme,mapping,path,issues);if(base==null)return null;
		String address=modbusAddress(form,mapping,path,issues);if(address==null)return null;
		List<Option> options=plcOptions(base.uri(),mapping,path,issues);
		if(hasIssue(issues,mapping))return null;
		List<Tag> tags=List.of(new Tag(target.resolved().affordance().idShort().value(),address));
		String driver="bacnet".equals(scheme)?"bacnet-ip":"modbus-tcp";
		Plc4xDataSourceConfiguration source=new Plc4xDataSourceConfiguration(entityId(mapping,"source",position),
				base.host(),base.port(),driver,base.servicePath(),options,tags);
		source.setSourceId(endpoint.id()); source.setDelivery("sync"); source.setSchema(schemaForAidTarget(target,mapping,path,issues));
		setValuePath(source,target);
		if(endpoint.pollingInterval().present()) {
			OptionalLong interval=endpointPollingMillis(endpoint.pollingInterval(),mapping,position,issues);
			if(interval.isPresent())source.setPollingIntervalMs(interval.getAsLong());
		}
		return hasIssue(issues,mapping)?null:source;
	}

	private static DataSinkConfiguration plc4xSink(EndpointPlan endpoint,MappingPlan mapping,int position,
			AidTarget target,List<Issue> issues) {
		String path=endpointPath(mapping,"Sinks",position);
		if(!"property".equalsIgnoreCase(target.resolved().affordance().kind())) {
			issue(issues,mapping,path,"Modbus command invocation is not represented by the PLC4X tag-write connector");return null;
		}
		Plc4xDataSinkConfiguration sink=plc4xTagSink(target,entityId(mapping,"sink",position),
				target.resolved().affordance().idShort().value(),mapping,path,issues);
		if(sink==null)return null;
		sink.setSinkId(endpoint.id()); sink.setSchema(schemaForAidTarget(target,mapping,path,issues)); setValuePath(sink,target);
		return sink;
	}

	/** A PLC4X write of the one tag that the affordance's form addresses. */
	private static Plc4xDataSinkConfiguration plc4xTagSink(AidTarget target,String uniqueId,String tagName,
			MappingPlan mapping,String path,List<Issue> issues) {
		Form form=protocolForm(target,mapping,path,issues);if(form==null)return null;
		if(formTerm(form,"modv_function")!=null) { issue(issues,mapping,path+"/forms/modv_function","PLC4X connector configuration does not represent modv_function");return null; }
		if(formTerm(form,"modv_zeroBasedAddressing")!=null) { issue(issues,mapping,path+"/forms/modv_zeroBasedAddressing","PLC4X connector configuration does not represent zero-based addressing mode");return null; }
		String scheme=target.resolved().owner().protocolFamilies().contains(WireModel.ProtocolFamily.BACNET)?"bacnet":"tcp";
		ProtocolBase base=protocolBase(target,scheme,mapping,path,issues);if(base==null)return null;
		String address=modbusAddress(form,mapping,path,issues);
		List<Option> options=plcOptions(base.uri(),mapping,path,issues);
		if(address==null || hasIssue(issues,mapping))return null;
		String driver="bacnet".equals(scheme)?"bacnet-ip":"modbus-tcp";
		return new Plc4xDataSinkConfiguration(uniqueId,base.host(),base.port(),driver,base.servicePath(),options,
				List.of(new Tag(tagName,address)));
	}

	private static ProtocolBase protocolBase(AidTarget target,String requiredScheme,MappingPlan mapping,String path,
			List<Issue> issues) {
		WireModel.Presence<SubmodelElement> metadata=target.resolved().owner().endpointMetadata();
		String base=metadata.present() && metadata.value()!=null?elementString(metadata.value(),"base"):null;
		if(base==null || base.isBlank()) { issue(issues,mapping,path+"/EndpointMetadata/base","endpoint base URI is required");return null; }
		try {
			URI uri=URI.create(base);
			if(!requiredScheme.equalsIgnoreCase(uri.getScheme()) || uri.getHost()==null || uri.getPort()<1) {
				issue(issues,mapping,path+"/EndpointMetadata/base","unsupported endpoint scheme or missing host/port");return null;
			}
			if(uri.getRawFragment()!=null || "opc.tcp".equals(requiredScheme) && uri.getRawQuery()!=null) {
				issue(issues,mapping,path+"/EndpointMetadata/base","endpoint query/fragment options are not represented by this connector");return null;
			}
			String username=null,password=null;
			if(uri.getUserInfo()!=null) {
				int sep=uri.getUserInfo().indexOf(':');
				if(sep<1 || sep==uri.getUserInfo().length()-1) { issue(issues,mapping,path+"/EndpointMetadata/base","endpoint credentials require username and password");return null; }
				username=uri.getUserInfo().substring(0,sep);password=uri.getUserInfo().substring(sep+1);
			}
			return new ProtocolBase(uri.getHost(),uri.getPort(),uri.getPath()==null?"":uri.getPath().replaceAll("^/+|/+$",""),username,password,uri);
		} catch(IllegalArgumentException malformed) {
			issue(issues,mapping,path+"/EndpointMetadata/base","endpoint base URI is malformed");return null;
		}
	}

	private static Form protocolForm(AidTarget target,MappingPlan mapping,String path,List<Issue> issues) {
		List<Form> forms=target.resolved().affordance().forms();
		if(forms.size()!=1) { issue(issues,mapping,path+"/forms","expected exactly one authored protocol form");return null; }
		return forms.get(0);
	}

	private static String opcuaNode(Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String browse=formTerm(form,"uav_browsePath");
		if(browse!=null && !browse.isBlank()) {
			if(form.href().value()!=null && !form.href().value().isBlank()) {
				issue(issues,mapping,path,"simultaneous OPC UA href and browsePath are ambiguous");return null;
			}
			return browse;
		}
		if(!form.href().present() || form.href().value()==null || form.href().value().isBlank()) {
			issue(issues,mapping,path+"/forms/href","OPC UA node href is required");return null;
		}
		String href=form.href().value().trim();
		int id=href.toLowerCase(java.util.Locale.ROOT).indexOf("?id=");
		if(id>=0)return href.substring(id+4).replace("%23","#").replace("%26","&");
		return href;
	}

	private static String modbusAddress(Form form,MappingPlan mapping,String path,List<Issue> issues) {
		String entity=formTerm(form,"modv_entity");
		if(entity!=null && !entity.isBlank()) {
			if(form.href().value()!=null && !form.href().value().isBlank()
					&& !form.href().value().replaceFirst("^/","").equals(entity.replaceFirst("^/",""))) {
				issue(issues,mapping,path,"conflicting PLC4X href and entity addresses");return null;
			}
			return entity;
		}
		if(form.href().present() && form.href().value()!=null && !form.href().value().isBlank())return form.href().value();
		issue(issues,mapping,path+"/forms/modv_entity","Modbus tag address is absent");return null;
	}

	private static List<Option> plcOptions(URI base,MappingPlan mapping,String path,List<Issue> issues) {
		List<Option> options=new ArrayList<>();
		String query=base.getRawQuery();
		if(query==null || query.isBlank())return options;
		Set<String> names=new LinkedHashSet<>();
		for(String pair:query.split("&")) {
			int separator=pair.indexOf('=');
			if(separator<=0) { issue(issues,mapping,path+"/EndpointMetadata/base","PLC4X base query options must use name=value");return List.of(); }
			String name=java.net.URLDecoder.decode(pair.substring(0,separator),StandardCharsets.UTF_8);
			String value=java.net.URLDecoder.decode(pair.substring(separator+1),StandardCharsets.UTF_8);
			if(!names.add(name)) { issue(issues,mapping,path+"/EndpointMetadata/base","duplicate PLC4X query option "+name);return List.of(); }
			options.add(new Option(name,value));
		}
		return List.copyOf(options);
	}

	private static String formTerm(Form form,String name) {
		return form.bindingTerms().get(name);
	}

	private static void setValuePath(DataSourceConfiguration configuration,AidTarget target) {
		String pointer=jsonPointer(target.resolved().schemaPath()); if(pointer!=null)configuration.setValuePath(pointer);
	}
	private static void setValuePath(DataSinkConfiguration configuration,AidTarget target) {
		String pointer=jsonPointer(target.resolved().schemaPath()); if(pointer!=null)configuration.setValuePath(pointer);
	}
	private static String jsonPointer(List<String> tokens) {
		if(tokens.isEmpty())return null;
		if(tokens.stream().anyMatch(token -> token.isEmpty() || token.chars().anyMatch(c -> ".[]/ ~".indexOf(c)>=0)))
			return "/"+tokens.stream().map(token -> token.replace("~","~0").replace("/","~1")).collect(java.util.stream.Collectors.joining("/"));
		return "$."+String.join(".",tokens);
	}
	private static int effectivePort(URI uri) {
		return uri.getPort() >= 0 ? uri.getPort() : ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
	}
	private static OptionalLong pollingMillis(MappingPlan mapping, List<Issue> issues) {
		if (!mapping.defaultPollingInterval().present()) return OptionalLong.empty();
		Object value = mapping.defaultPollingInterval().value();
		double seconds;
		try { seconds = value instanceof Number number ? number.doubleValue() : Double.parseDouble(String.valueOf(value)); }
		catch (RuntimeException invalid) {
			issue(issues, mapping, "MappingConfigurations/value/" + mapping.listIndex() + "/DefaultPollingInterval",
					"expected a positive numeric interval in authored seconds");
			return OptionalLong.empty();
		}
		double millis = seconds * 1000d;
		if (!Double.isFinite(seconds) || seconds <= 0 || millis > Long.MAX_VALUE || millis < 1) {
			issue(issues, mapping, "MappingConfigurations/value/" + mapping.listIndex() + "/DefaultPollingInterval",
					"poll interval is outside the runtime millisecond range");
			return OptionalLong.empty();
		}
		return OptionalLong.of((long) millis);
	}
	private static Object schema(WireModel.Presence<DataSchema> presence) {
		return presence.present() && presence.value() != null ? schema(presence.value()) : null;
	}

	private static Object schemaOptional(DataSchema schema) { return schema == null ? null : schema(schema); }

	private static Object schemaForAidTarget(AidTarget target,MappingPlan mapping,String path,List<Issue> issues) {
		Object base=schema(target.resolved().schema());
		Map<String,Object> result=new LinkedHashMap<>();
		if(base instanceof Map<?,?> map)map.forEach((key,value) -> result.put(String.class.cast(key),value));
		for(Qualifier qualifier:Aas.nonNull(target.resolved().affordance().element().getQualifiers())) {
			if(!behaviorQualifier(qualifier))continue;
			if(!"RequiredProperties".equals(qualifier.getType())) {
				issue(issues,mapping,path+"/qualifiers/"+qualifier.getType(),"unsupported AID qualifier semantics");
				continue;
			}
			if(qualifier.getValue()==null) {
				issue(issues,mapping,path+"/qualifiers/RequiredProperties","required property list must be explicitly authored");
				continue;
			}
			Object raw=qualifier.getValue();
			if(raw instanceof String encoded && encoded.startsWith("[")) {
				try { raw=JsonCodec.parseJson(encoded); }
				catch(RuntimeException malformed) { issue(issues,mapping,path+"/qualifiers/RequiredProperties","invalid JSON property list");continue; }
			}
			if(!(raw instanceof List<?> items) || items.stream().anyMatch(item -> !(item instanceof String))) {
				issue(issues,mapping,path+"/qualifiers/RequiredProperties","expected a list of property names");
				continue;
			}
			List<String> required=new ArrayList<>();
			Object existing=result.get("required");
			if(existing instanceof List<?> values) for(Object value:values) if(value instanceof String name && !required.contains(name)) required.add(name);
			for(Object item:items) if(!required.contains((String)item)) required.add((String)item);
			if(target.resolved().schemaPath().isEmpty())result.put("required",List.copyOf(required));
		}
		return result.isEmpty()?null:java.util.Collections.unmodifiableMap(result);
	}
	private static boolean behaviorQualifier(Qualifier qualifier) {
		return qualifier.getKind()!=QualifierKind.TEMPLATE_QUALIFIER;
	}
	private static Map<String, Object> schema(DataSchema schema) {
		Map<String, Object> out = new LinkedHashMap<>();
		put(out, "type", schema.type()); put(out, "valueType", schema.valueType());
		if(!schema.type().present() && schema.valueType().value()!=null) {
			String type=schemaType(schema.valueType().value());
			if(type==null)throw new IllegalArgumentException("unsupported typed schema Property valueType");
			out.put("type",type);
		}
		put(out, "contentType", schema.contentType()); put(out, "valueFormat", schema.valueFormat());
		if (!schema.properties().isEmpty()) {
			Map<String, Object> properties = new LinkedHashMap<>();
			schema.properties().forEach((key, value) -> properties.put(key, schema(value)));
			out.put("properties", properties);
		}
		if (schema.items().present() && schema.items().value() != null) out.put("items", schema(schema.items().value()));
		if (schema.required().present()) out.put("required", schema.required().value());
		out.putAll(schema.constraints());
		return out;
	}
	private static void put(Map<String, Object> map, String key, WireModel.Presence<String> value) {
		if (value.present()) map.put(key, value.value());
	}
	private static String endpointPath(MappingPlan mapping, String collection, int position) {
		return "MappingConfigurations/value/" + mapping.listIndex() + "/" + collection + "/value/" + position;
	}
	private static String entityId(MappingPlan mapping, String kind, int position) {
		try {
			return "managed:"+java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(
					(mapping.identity()+"\u0000"+kind+"\u0000"+position).getBytes(StandardCharsets.UTF_8)));
		} catch(java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
	}
	private static void issue(List<Issue> issues, MappingPlan mapping, String path, String message) {
		issues.add(new Issue(path, mapping.identity(), message));
	}
}
