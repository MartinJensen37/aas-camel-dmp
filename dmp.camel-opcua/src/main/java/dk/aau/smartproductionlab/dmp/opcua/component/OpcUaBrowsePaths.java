/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.component;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowsePath;
import org.eclipse.milo.opcua.stack.core.types.structured.BrowsePathResult;
import org.eclipse.milo.opcua.stack.core.types.structured.RelativePath;
import org.eclipse.milo.opcua.stack.core.types.structured.RelativePathElement;

/**
 * A node named by an NodeId or by an AID {@code uav_browsePath}: "an absolute path of a datapoint,
 * starting from the root node of an OPC UA address space" (AID 1.2 §2.24.4), e.g.
 * {@code /Root/Objects/2:Machine/2:Pressure}. A browse path starts with {@code /} (no NodeId string
 * does); its segments are qualified names ({@code <namespaceIndex>:<name>}, namespace 0 without a
 * prefix), each followed along hierarchical references. The leading {@code Root} names the starting
 * node. The UA TranslateBrowsePathsToNodeIds service resolves it.
 */
public final class OpcUaBrowsePaths {
	private static final NodeId ROOT_FOLDER = NodeId.parse("i=84");
	private static final NodeId HIERARCHICAL_REFERENCES = NodeId.parse("i=33");

	private OpcUaBrowsePaths() { }

	public static boolean isBrowsePath(String node) {
		return node != null && node.startsWith("/");
	}

	/** The node a NodeId string or browse path names on the server {@code client} is connected to. */
	public static NodeId resolve(OpcUaClient client, String node) throws UaException {
		if (!isBrowsePath(node)) return ExpandedNodeId.parse(node).toNodeIdOrThrow(client.getNamespaceTable());
		BrowsePathResult result = client.translateBrowsePaths(List.of(new BrowsePath(ROOT_FOLDER, relativePath(node))))
				.getResults()[0];
		if (result.getStatusCode() == null || !result.getStatusCode().isGood() || result.getTargets() == null
				|| result.getTargets().length != 1)
			throw new IllegalStateException("OPC UA browse path " + node + " does not name exactly one node ("
					+ result.getStatusCode() + ")");
		return result.getTargets()[0].getTargetId().toNodeIdOrThrow(client.getNamespaceTable());
	}

	/** The path's segments after the Root node, as hierarchical steps. */
	static RelativePath relativePath(String browsePath) {
		List<String> segments = new ArrayList<>(List.of(browsePath.substring(1).split("/", -1)));
		if (!segments.isEmpty() && List.of("Root", "0:Root").contains(segments.get(0))) segments.remove(0);
		if (segments.isEmpty() || segments.stream().anyMatch(String::isEmpty))
			throw new IllegalArgumentException("OPC UA browse path " + browsePath + " names no node below Root");
		RelativePathElement[] elements = new RelativePathElement[segments.size()];
		for (int i = 0; i < segments.size(); i++)
			elements[i] = new RelativePathElement(HIERARCHICAL_REFERENCES, false, true, qualifiedName(segments.get(i)));
		return new RelativePath(elements);
	}

	static QualifiedName qualifiedName(String segment) {
		int colon = segment.indexOf(':');
		if (colon > 0 && segment.substring(0, colon).chars().allMatch(Character::isDigit))
			return new QualifiedName(Integer.parseInt(segment.substring(0, colon)), segment.substring(colon + 1));
		return new QualifiedName(0, segment);
	}
}
