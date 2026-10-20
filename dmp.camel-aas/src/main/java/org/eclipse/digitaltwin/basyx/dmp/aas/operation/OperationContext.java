package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Per-request AAS Operation context; target connector settings remain opaque. */
public final class OperationContext {
	public final String action;
	public final String requestLuaUri;
	public final List<String> requestTransformerUris;
	public final String responseLuaUri;
	public final String commandSinkUri;
	public final Map<String, String> parameterTypes;
	public final Map<String, String> responseTypes;
	public final Map<String, String> callerResponseTypes;
	public final List<String> callerOutputOrder;
	/** Declared caller input and InOut variables, by idShort; they type the request values. */
	public final Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> callerInputTemplates;
	/** Declared caller InOut and output variables, by idShort; the reply is rebuilt from them. */
	public final Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> callerOutputTemplates;
	public final String operationSourceId;
	public final String commandSinkId;
	public final String callerReplySinkId;
	public final List<String> commandSinkIds;
	public final Map<String, String> commandSinkValuePaths;
	public final boolean explicitResponseMapping;
	public final Set<String> parameterRequired;
	public final Set<String> responseRequired;
	public final Map<String, Object> targetOptions;
	public final Map<String, List<String>> sinkTransformerUris;

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri) {
		this(action, requestLuaUri, responseLuaUri, commandSinkUri,
				Map.of(), Map.of());
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, Map<String, String> parameterTypes,
			Map<String, String> responseTypes) {
		this(action, requestLuaUri, responseLuaUri, commandSinkUri,
				parameterTypes, responseTypes, List.of(), action, action, action, List.of(action),
				Map.of(), false, List.of(), List.of(), Map.of(), List.of(requestLuaUri), Map.of());
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, Map<String, String> parameterTypes,
			Map<String, String> responseTypes, List<String> callerOutputOrder,
			String operationSourceId, String commandSinkId, String callerReplySinkId,
			List<String> commandSinkIds, Map<String, String> commandSinkValuePaths,
			boolean explicitResponseMapping, List<String> parameterRequired,
			List<String> responseRequired, Map<String, Object> targetOptions) {
		this(action, requestLuaUri, responseLuaUri, commandSinkUri, parameterTypes, responseTypes,
				callerOutputOrder, operationSourceId, commandSinkId, callerReplySinkId, commandSinkIds,
				commandSinkValuePaths, explicitResponseMapping, parameterRequired, responseRequired,
				targetOptions, List.of(requestLuaUri), Map.of());
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, Map<String, String> parameterTypes,
			Map<String, String> responseTypes, List<String> callerOutputOrder,
			String operationSourceId, String commandSinkId, String callerReplySinkId,
			List<String> commandSinkIds, Map<String, String> commandSinkValuePaths,
			boolean explicitResponseMapping, List<String> parameterRequired,
			List<String> responseRequired, Map<String, Object> targetOptions,
			List<String> requestTransformerUris, Map<String, List<String>> sinkTransformerUris) {
		// Persisted standalone descriptors used one type map for both boundaries.
		this(action, requestLuaUri, responseLuaUri, commandSinkUri, parameterTypes, responseTypes,
				callerOutputOrder, operationSourceId, commandSinkId, callerReplySinkId, commandSinkIds,
				commandSinkValuePaths, explicitResponseMapping, parameterRequired, responseRequired,
				targetOptions, requestTransformerUris, sinkTransformerUris, responseTypes);
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, Map<String, String> parameterTypes,
			Map<String, String> responseTypes, List<String> callerOutputOrder,
			String operationSourceId, String commandSinkId, String callerReplySinkId,
			List<String> commandSinkIds, Map<String, String> commandSinkValuePaths,
			boolean explicitResponseMapping, List<String> parameterRequired,
			List<String> responseRequired, Map<String, Object> targetOptions,
			List<String> requestTransformerUris, Map<String, List<String>> sinkTransformerUris,
			Map<String, String> callerResponseTypes) {
		this(action, requestLuaUri, responseLuaUri, commandSinkUri, parameterTypes, responseTypes,
				callerOutputOrder, operationSourceId, commandSinkId, callerReplySinkId, commandSinkIds,
				commandSinkValuePaths, explicitResponseMapping, parameterRequired, responseRequired,
				targetOptions, requestTransformerUris, sinkTransformerUris, callerResponseTypes, Map.of(), Map.of());
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, Map<String, String> parameterTypes,
			Map<String, String> responseTypes, List<String> callerOutputOrder,
			String operationSourceId, String commandSinkId, String callerReplySinkId,
			List<String> commandSinkIds, Map<String, String> commandSinkValuePaths,
			boolean explicitResponseMapping, List<String> parameterRequired,
			List<String> responseRequired, Map<String, Object> targetOptions,
			List<String> requestTransformerUris, Map<String, List<String>> sinkTransformerUris,
			Map<String, String> callerResponseTypes,
			Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> callerInputTemplates,
			Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> callerOutputTemplates) {
		this.action = action;
		this.requestLuaUri = requestLuaUri;
		this.requestTransformerUris = requestTransformerUris == null || requestTransformerUris.isEmpty()
				? List.of(requestLuaUri) : List.copyOf(requestTransformerUris);
		this.responseLuaUri = responseLuaUri;
		this.commandSinkUri = commandSinkUri;
		this.parameterTypes = parameterTypes == null ? Map.of() : Map.copyOf(parameterTypes);
		this.responseTypes = responseTypes == null ? Map.of() : Map.copyOf(responseTypes);
		this.callerResponseTypes = callerResponseTypes == null ? Map.of() : Map.copyOf(callerResponseTypes);
		this.callerOutputOrder = callerOutputOrder == null ? List.of() : List.copyOf(callerOutputOrder);
		this.callerInputTemplates = callerInputTemplates == null ? Map.of() : Map.copyOf(callerInputTemplates);
		this.callerOutputTemplates = callerOutputTemplates == null ? Map.of() : Map.copyOf(callerOutputTemplates);
		this.operationSourceId = operationSourceId == null ? action : operationSourceId;
		this.commandSinkId = commandSinkId == null ? action : commandSinkId;
		// Null: no ResponseSink selects the Operation, which then answers with no outputs.
		this.callerReplySinkId = callerReplySinkId;
		this.commandSinkIds = commandSinkIds == null ? List.of() : List.copyOf(commandSinkIds);
		this.commandSinkValuePaths = commandSinkValuePaths == null ? Map.of() : Map.copyOf(commandSinkValuePaths);
		this.explicitResponseMapping = explicitResponseMapping;
		this.parameterRequired = parameterRequired == null ? Set.of() : Set.copyOf(parameterRequired);
		this.responseRequired = responseRequired == null ? Set.of() : Set.copyOf(responseRequired);
		this.targetOptions = targetOptions == null ? Map.of() : Map.copyOf(targetOptions);
		this.sinkTransformerUris = sinkTransformerUris == null ? Map.of() : Map.copyOf(sinkTransformerUris);
	}
}
