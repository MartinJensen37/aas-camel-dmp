/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.opcua.component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.digitaltwin.basyx.dmp.opcua.canonical.OpcUaCanonical;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.FilterOperator;
import org.eclipse.milo.opcua.stack.core.types.structured.ContentFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.ContentFilterElement;
import org.eclipse.milo.opcua.stack.core.types.structured.EventFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.LiteralOperand;
import org.eclipse.milo.opcua.stack.core.types.structured.SimpleAttributeOperand;

/**
 * The fields an event subscription selects and the EventType it is filtered to
 * (OPC 10000-4 §7.22.3 EventFilter). Endpoint options:
 *
 * <ul>
 * <li>{@code eventFields}: comma-separated {@code name=path} entries, one per
 * notification field; {@code path} is the field's browse path relative to the
 * event, {@code /}-separated qualified names ({@code 0:ToState/0:Id}); a name
 * without {@code <namespaceIndex>:} is in namespace 0, and an entry without
 * {@code =path} selects the namespace-0 field of the same name;</li>
 * <li>{@code eventType} (optional): the EventType NodeId; the where clause is
 * {@code OfType} it, so subtypes pass.</li>
 * </ul>
 *
 * A notification becomes a map from field name to its JSON value.
 */
final class EventSelection {
	/** BaseEventType (i=2041): a select clause applies to every event when no EventType is given. */
	private static final NodeId BASE_EVENT_TYPE = NodeId.parse("i=2041");
	private static final Pattern NAME = Pattern.compile("[A-Za-z][A-Za-z0-9_-]*");
	private static final Pattern QUALIFIED = Pattern.compile("(?:(\\d+):)?([^/:,=]+)");

	record Field(String name, QualifiedName[] path) { }

	private final List<Field> fields;
	private final String eventType;

	private EventSelection(List<Field> fields, String eventType) {
		this.fields = List.copyOf(fields);
		this.eventType = eventType;
	}

	/** Parses the endpoint options; rejects a malformed field list at endpoint creation. */
	static EventSelection parse(String eventFields, String eventType) {
		List<Field> fields = new ArrayList<>();
		for (String entry : eventFields.split(",")) {
			String trimmed = entry.trim();
			int split = trimmed.indexOf('=');
			String name = split < 0 ? trimmed : trimmed.substring(0, split);
			String path = split < 0 ? trimmed : trimmed.substring(split + 1);
			if (!NAME.matcher(name).matches())
				throw new IllegalArgumentException("event field name '" + name + "' is not a valid field name");
			if (fields.stream().anyMatch(field -> field.name().equals(name)))
				throw new IllegalArgumentException("event field '" + name + "' is selected twice");
			fields.add(new Field(name, browsePath(name, path)));
		}
		if (fields.isEmpty()) throw new IllegalArgumentException("an event subscription selects at least one field");
		return new EventSelection(fields, eventType == null || eventType.isBlank() ? null : eventType.trim());
	}

	private static QualifiedName[] browsePath(String name, String path) {
		String[] segments = path.replaceFirst("^/", "").split("/");
		QualifiedName[] names = new QualifiedName[segments.length];
		for (int i = 0; i < segments.length; i++) {
			Matcher matcher = QUALIFIED.matcher(segments[i]);
			if (!matcher.matches())
				throw new IllegalArgumentException("event field '" + name + "' has a malformed browse path '" + path + "'");
			int namespace = matcher.group(1) == null ? 0 : Integer.parseInt(matcher.group(1));
			names[i] = new QualifiedName(namespace, matcher.group(2));
		}
		return names;
	}

	List<Field> fields() { return fields; }

	String eventType() { return eventType; }

	/**
	 * The EventFilter: one select clause per field and {@code OfType(eventType)} as where clause.
	 * Select clauses name BaseEventType as their type, so a browse path reaches fields of any
	 * subtype (OPC 10000-4 §7.7.4.5); the EventType restriction is the where clause alone.
	 */
	EventFilter filter(OpcUaClient client) throws org.eclipse.milo.opcua.stack.core.UaException {
		SimpleAttributeOperand[] select = fields.stream()
				.map(field -> new SimpleAttributeOperand(BASE_EVENT_TYPE, field.path(), AttributeId.Value.uid(), null))
				.toArray(SimpleAttributeOperand[]::new);
		ContentFilterElement[] where = new ContentFilterElement[0];
		if (eventType != null) {
			NodeId type = ExpandedNodeId.parse(eventType).toNodeIdOrThrow(client.getNamespaceTable());
			where = new ContentFilterElement[] { new ContentFilterElement(FilterOperator.OfType, new ExtensionObject[] {
					ExtensionObject.encode(client.getStaticEncodingContext(), new LiteralOperand(new Variant(type))) }) };
		}
		return new EventFilter(select, new ContentFilter(where));
	}

	/** One notification: field name → JSON value ({@code null} for a field the event does not have). */
	Map<String, Object> notification(Variant[] values) {
		Map<String, Object> notification = new LinkedHashMap<>();
		for (int i = 0; i < fields.size(); i++) {
			Variant value = values == null || i >= values.length ? null : values[i];
			notification.put(fields.get(i).name(), value == null ? null : OpcUaCanonical.jsonValue(value.getValue()));
		}
		return notification;
	}
}
