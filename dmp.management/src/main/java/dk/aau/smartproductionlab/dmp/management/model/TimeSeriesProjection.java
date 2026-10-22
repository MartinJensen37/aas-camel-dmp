/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.model;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.HasSemantics;
import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxValues;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;

/**
 * Projects a TimeSeries LinkedSegment (IDTA 02008, this project's DMP profile) and its series container into what
 * the DMP writes and reads. Elements are recognised by semanticId only, never by idShort: the series' Record is the
 * row shape, each Record field names its InfluxDB column in a {@code Column} qualifier, and the segment's
 * {@code InfluxDb2Target} names bucket, measurement and tags. Nothing missing is filled in.
 */
public final class TimeSeriesProjection {
	public static final String IDTA_TIME_SERIES = "https://admin-shell.io/idta/TimeSeries/1/1";
	public static final String IDTA_METADATA = "https://admin-shell.io/idta/TimeSeries/Metadata/1/1";
	public static final String IDTA_RECORD = "https://admin-shell.io/idta/TimeSeries/Record/1/1";
	public static final String IDTA_SEGMENTS = "https://admin-shell.io/idta/TimeSeries/Segments/1/1";
	public static final String IDTA_LINKED_SEGMENT = "https://admin-shell.io/idta/TimeSeries/Segments/LinkedSegment/1/1";
	public static final String IDTA_ENDPOINT = "https://admin-shell.io/idta/TimeSeries/Endpoint/1/1";
	public static final String IDTA_UTC_TIME = "https://admin-shell.io/idta/TimeSeries/UtcTime/1/1";
	public static final String IDTA_START_TIME = "https://admin-shell.io/idta/TimeSeries/Segment/StartTime/1/1";
	public static final String IDTA_END_TIME = "https://admin-shell.io/idta/TimeSeries/Segment/EndTime/1/1";
	public static final String IDTA_STATE = "https://admin-shell.io/idta/TimeSeries/Segment/State/1/1";
	/** The IDTA segment states: a running segment, and one whose period is over. */
	public static final String IN_PROGRESS = "InProgress";
	public static final String COMPLETED = "Completed";
	/** Time semantics a row can carry; only UtcTime places a point on InfluxDB's absolute time axis. */
	static final Set<String> IDTA_TIMES = Set.of(IDTA_UTC_TIME, "https://admin-shell.io/idta/TimeSeries/TaiTime/1/1",
			"https://admin-shell.io/idta/TimeSeries/RelativePointInTime/1/1",
			"https://admin-shell.io/idta/TimeSeries/RelativeTimeDuration/1/1");

	private static final String BASE = "https://smartproductionlab.aau.dk";
	public static final String SERIES = BASE + "/variables/TimeSeries/1/0";
	public static final String RECORD_GROUP = BASE + "/timeseries/RecordGroup/1/0";
	public static final String COLUMN = BASE + "/timeseries/Column/1/0";
	public static final String INFLUX_TARGET = BASE + "/timeseries/InfluxDb2Target/1/0";
	public static final String BUCKET = BASE + "/timeseries/InfluxDb2Target/Bucket/1/0";
	public static final String MEASUREMENT = BASE + "/timeseries/InfluxDb2Target/Measurement/1/0";
	public static final String TAGS = BASE + "/timeseries/InfluxDb2Target/Tags/1/0";
	public static final String TAG = BASE + "/timeseries/InfluxDb2Target/Tag/1/0";

	/**
	 * What one LinkedSegment writes and reads: one point per row of its series' Record. {@code start} and {@code end}
	 * are the segment's authored period ({@code null} when not authored); a segment is open for writes unless it is
	 * {@code Completed} or has an end.
	 */
	public record Series(URI endpoint, String bucket, String measurement, Map<String, String> tags, String timePath,
			List<RecordColumn> columns, Instant start, Instant end, boolean open) {
		public Series { tags = Map.copyOf(tags); columns = List.copyOf(columns); }
	}

	private TimeSeriesProjection() { }

	/** Whether an element is a TimeSeries LinkedSegment. */
	public static boolean isLinkedSegment(SubmodelElement element) {
		return element instanceof SubmodelElementCollection && Aas.hasSemantic(element, IDTA_LINKED_SEGMENT);
	}

	/** Whether an element is a TimeSeries container: this project's series SMC, or one carrying the IDTA id. */
	public static boolean isSeries(SubmodelElement element) {
		return element instanceof SubmodelElementCollection
				&& (Aas.hasSemantic(element, SERIES) || semanticOrSupplemental(element, IDTA_TIME_SERIES));
	}

	/** Projects a segment of {@code series}; {@code segments} is the Segments collection between them. */
	public static Series project(SubmodelElement series, SubmodelElement segments, SubmodelElement segment) {
		if (!isSeries(series)) throw error("the segment's container is not a TimeSeries (" + SERIES + ")");
		if (!Aas.hasSemantic(segments, IDTA_SEGMENTS) || !Aas.children(series).contains(segments)
				|| !Aas.children(segments).contains(segment))
			throw error("a LinkedSegment must lie in the Segments of its TimeSeries");
		if (!isLinkedSegment(segment)) throw error("the target is not a TimeSeries LinkedSegment");
		SubmodelElement record = only(Aas.children(only(Aas.children(series), IDTA_METADATA, "Metadata")), IDTA_RECORD, "Record");

		String timePath = null;
		List<RecordColumn> columns = new ArrayList<>();
		for (SubmodelElement child : Aas.children(record)) {
			if (IDTA_TIMES.stream().anyMatch(time -> Aas.hasSemantic(child, time))) {
				if (!Aas.hasSemantic(child, IDTA_UTC_TIME))
					throw error("only UtcTime records can be written or read; " + child.getIdShort() + " has another time semantic");
				if (timePath != null) throw error("the Record has more than one time");
				if (!(child instanceof Property time) || !"xs:dateTime".equals(Aas.xsd(time.getValueType())))
					throw error("the Record's UtcTime must be an xs:dateTime Property");
				timePath = child.getIdShort();
				continue;
			}
			fields(child, "", columns);
		}
		if (timePath == null) throw error("the Record has no UtcTime");
		if (columns.isEmpty()) throw error("the Record has no field");
		Set<String> seen = new LinkedHashSet<>();
		for (RecordColumn column : columns)
			if (!seen.add(column.column())) throw error("Record column " + column.column() + " is declared twice");

		SubmodelElement target = only(Aas.children(segment), INFLUX_TARGET, "InfluxDb2Target");
		Map<String, String> tags = new LinkedHashMap<>();
		for (SubmodelElement tag : Aas.children(only(Aas.children(target), TAGS, "Tags"))) {
			if (!Aas.hasSemantic(tag, TAG)) throw error("InfluxDb2Target Tags holds an element that is not a Tag: " + tag.getIdShort());
			tags.put(tag.getIdShort(), value(tag, "Tag " + tag.getIdShort()));
		}
		URI endpoint;
		try { endpoint = URI.create(value(only(Aas.children(segment), IDTA_ENDPOINT, "Endpoint"), "Endpoint")); }
		catch (IllegalArgumentException invalid) { throw error("the segment's Endpoint is not a URI"); }

		Instant start = time(optional(Aas.children(segment), IDTA_START_TIME, "StartTime"), "StartTime");
		Instant end = time(optional(Aas.children(segment), IDTA_END_TIME, "EndTime"), "EndTime");
		if (start != null && end != null && !end.isAfter(start)) throw error("the segment's EndTime must lie after its StartTime");
		String state = optional(Aas.children(segment), IDTA_STATE, "State");
		if (state != null && !IN_PROGRESS.equals(state) && !COMPLETED.equals(state))
			throw error("the segment's State must be " + IN_PROGRESS + " or " + COMPLETED + ", not " + state);
		if (IN_PROGRESS.equals(state) && end != null) throw error("an InProgress segment has no EndTime");
		return new Series(endpoint, value(only(Aas.children(target), BUCKET, "Bucket"), "Bucket"),
				value(only(Aas.children(target), MEASUREMENT, "Measurement"), "Measurement"), tags, timePath, columns,
				start, end, end == null && !COMPLETED.equals(state));
	}

	/** The value of an optional segment property; an empty value counts as not authored. */
	private static String optional(List<SubmodelElement> children, String semantic, String what) {
		List<SubmodelElement> matches = children.stream().filter(child -> Aas.hasSemantic(child, semantic)).toList();
		if (matches.size() > 1) throw error(what + " is declared twice");
		if (matches.isEmpty()) return null;
		if (!(matches.get(0) instanceof Property property)) throw error(what + " must be a Property");
		return property.getValue() == null || property.getValue().isBlank() ? null : property.getValue().strip();
	}

	/** A segment time in the format of a UtcTime series: an xs:dateTime with an offset. */
	private static Instant time(String value, String what) {
		if (value == null) return null;
		try { return OffsetDateTime.parse(value).toInstant(); }
		catch (DateTimeParseException invalid) { throw error("the segment's " + what + " is not an xs:dateTime with an offset: " + value); }
	}

	private static void fields(SubmodelElement element, String prefix, List<RecordColumn> columns) {
		String path = prefix + element.getIdShort();
		if (element instanceof SubmodelElementCollection) {
			if (!Aas.hasSemantic(element, RECORD_GROUP)) throw error("Record group " + path + " is not a RecordGroup (" + RECORD_GROUP + ")");
			for (SubmodelElement child : Aas.children(element)) fields(child, path + ".", columns);
			return;
		}
		if (!(element instanceof Property property)) throw error("Record field " + path + " is a " + Aas.modelType(element) + ", not a Property");
		List<Qualifier> declared = Aas.nonNull(property.getQualifiers()).stream()
				.filter(qualifier -> qualifier.getSemanticId() != null && Aas.keys(qualifier.getSemanticId()).stream()
						.anyMatch(key -> COLUMN.equals(key.getValue())))
				.toList();
		if (declared.size() != 1 || declared.get(0).getValue() == null || declared.get(0).getValue().isBlank())
			throw error("Record field " + path + " needs exactly one Column (" + COLUMN + ")");
		String valueType = Aas.xsd(property.getValueType());
		if (InfluxValues.kind(valueType) == null) throw error("Record field " + path + " has unsupported valueType " + valueType);
		columns.add(new RecordColumn(path, declared.get(0).getValue(), valueType));
	}

	private static SubmodelElement only(List<SubmodelElement> children, String semantic, String what) {
		List<SubmodelElement> matches = children.stream().filter(child -> Aas.hasSemantic(child, semantic)).toList();
		if (matches.size() != 1) throw error(matches.isEmpty() ? what + " (" + semantic + ") is missing" : what + " is declared twice");
		return matches.get(0);
	}

	private static String value(SubmodelElement element, String what) {
		if (!(element instanceof Property property) || property.getValue() == null || property.getValue().isBlank())
			throw error(what + " must be a Property with a value");
		return property.getValue();
	}

	private static boolean semanticOrSupplemental(HasSemantics element, String semantic) {
		if (Aas.hasSemantic(element, semantic)) return true;
		for (Reference supplemental : Aas.nonNull(element.getSupplementalSemanticIds()))
			for (Key key : Aas.keys(supplemental)) if (semantic.equals(key.getValue())) return true;
		return false;
	}

	private static IllegalArgumentException error(String message) { return new IllegalArgumentException(message); }
}
