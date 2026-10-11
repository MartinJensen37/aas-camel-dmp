package org.eclipse.digitaltwin.basyx.dmp.core.util;

/** Marks a decoded value whose connector wire contract requires JSON encoding. */
public record ConnectorJsonPayload(Object value) { }
