package dk.aau.smartproductionlab.dmp.core.util;

/** Marks a decoded value whose connector wire contract requires JSON encoding. */
public record ConnectorJsonPayload(Object value) { }
