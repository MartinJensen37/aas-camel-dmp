package org.eclipse.digitaltwin.basyx.dmp.management.compiler;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel.Affordance;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel.Form;

/**
 * What an AID form serves and where it points: the WoT operations of a form (its {@code op}, else the
 * affordance's TD defaults; aas-model docs/aid-extension.md §3.4) and the resolution of a form's
 * {@code href} against the interface's {@code base} (AID 1.2 §2.23 and Annex B.2: RFC 3986).
 */
final class AidForms {
	static final String READ = "readproperty";
	static final String WRITE = "writeproperty";
	static final String OBSERVE = "observeproperty";
	static final String INVOKE = "invokeaction";
	static final String SUBSCRIBE = "subscribeevent";

	/** The well-known WoT TD 1.1 operation types. */
	static final Set<String> KNOWN = Set.of(READ, WRITE, OBSERVE, "unobserveproperty", INVOKE, "queryaction",
			"cancelaction", SUBSCRIBE, "unsubscribeevent", "readallproperties", "writeallproperties",
			"readmultipleproperties", "writemultipleproperties", "observeallproperties", "unobserveallproperties",
			"subscribeallevents", "unsubscribeallevents", "queryallactions");

	private AidForms() { }

	/**
	 * The operations a form serves. An authored {@code op} narrows the defaults: a property is
	 * readable unless {@code writeOnly}, writable unless {@code readOnly} and observable when
	 * {@code observable}; an action is invoked; an event is subscribed.
	 */
	static Set<String> operations(Affordance affordance, Form form) {
		if (!form.operations().isEmpty()) return Set.copyOf(form.operations());
		return switch (affordance.kind()) {
			case "action" -> Set.of(INVOKE);
			case "event" -> Set.of(SUBSCRIBE, "unsubscribeevent");
			default -> {
				Set<String> operations = new LinkedHashSet<>();
				if (!flag(affordance, "writeOnly")) operations.add(READ);
				if (!flag(affordance, "readOnly")) operations.add(WRITE);
				if (flag(affordance, "observable")) {
					operations.add(OBSERVE);
					operations.add("unobserveproperty");
				}
				yield Set.copyOf(operations);
			}
		};
	}

	/** A boolean term of the affordance ({@code readOnly}, {@code observable}, …); absent is false. */
	static boolean flag(Affordance affordance, String name) {
		SubmodelElement term = Aas.child(affordance.element(), name);
		String value = term == null ? null : Aas.value(term);
		return value != null && ("true".equalsIgnoreCase(value.trim()) || "1".equals(value.trim()));
	}

	/**
	 * {@code href} resolved against {@code base} (RFC 3986 §5.2). Unlike {@link URI#resolve}, empty
	 * path segments survive: AID writes an MQTT topic with a leading {@code /} as {@code .//topic}.
	 */
	static URI resolve(String base, String href) {
		URI reference = URI.create(href == null ? "" : href);
		if (reference.getScheme() != null) return reference;
		if (base == null || base.isBlank()) throw new IllegalArgumentException("a relative href needs a base");
		URI baseUri = URI.create(base);
		if (baseUri.getScheme() == null || baseUri.getRawAuthority() == null)
			throw new IllegalArgumentException("the base must be an absolute URI with an authority");
		String referencePath = reference.getRawPath() == null ? "" : reference.getRawPath();
		String basePath = baseUri.getRawPath() == null ? "" : baseUri.getRawPath();
		String path;
		String query;
		if (referencePath.isEmpty()) {
			path = basePath;
			query = reference.getRawQuery() != null ? reference.getRawQuery() : baseUri.getRawQuery();
		} else {
			path = referencePath.startsWith("/") ? referencePath
					: basePath.isEmpty() ? "/" + referencePath
					: basePath.substring(0, basePath.lastIndexOf('/') + 1) + referencePath;
			query = reference.getRawQuery();
		}
		return URI.create(baseUri.getScheme() + "://" + baseUri.getRawAuthority() + removeDotSegments(path)
				+ (query == null ? "" : "?" + query));
	}

	/** RFC 3986 §5.2.4, keeping empty segments. */
	static String removeDotSegments(String path) {
		StringBuilder input = new StringBuilder(path);
		StringBuilder output = new StringBuilder();
		while (input.length() > 0) {
			String in = input.toString();
			if (in.startsWith("../")) input.delete(0, 3);
			else if (in.startsWith("./")) input.delete(0, 2);
			else if (in.startsWith("/./")) input.replace(0, 3, "/");
			else if (in.equals("/.")) input.replace(0, 2, "/");
			else if (in.startsWith("/../") || in.equals("/..")) {
				input.replace(0, in.startsWith("/../") ? 4 : 3, "/");
				int last = output.lastIndexOf("/");
				output.setLength(Math.max(last, 0));
			} else if (in.equals(".") || in.equals("..")) input.setLength(0);
			else {
				int next = in.indexOf('/', in.startsWith("/") ? 1 : 0);
				if (next < 0) next = in.length();
				output.append(in, 0, next);
				input.delete(0, next);
			}
		}
		return output.toString();
	}

	/** Decodes {@code %XX} escapes (UTF-8); a {@code +} stays a {@code +} (an MQTT wildcard). */
	static String percentDecode(String raw) {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		for (int i = 0; i < raw.length(); i++) {
			char c = raw.charAt(i);
			if (c == '%' && i + 2 < raw.length()) {
				bytes.write(Integer.parseInt(raw.substring(i + 1, i + 3), 16));
				i += 2;
			} else {
				bytes.writeBytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
			}
		}
		return bytes.toString(StandardCharsets.UTF_8);
	}
}
