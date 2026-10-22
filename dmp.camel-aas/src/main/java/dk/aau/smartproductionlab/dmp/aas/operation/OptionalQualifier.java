package dk.aau.smartproductionlab.dmp.aas.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;

/**
 * The project's {@code Optional} qualifier on Operation variables: whether a value may be
 * absent at invocation. An input or InOut variable marked optional may be left out by the
 * caller; an output or InOut variable marked optional may be left out of the reply. Without
 * the qualifier a variable is required. It applies to top-level variables and to members of
 * collection variables; a list's members share one template, so a list is marked as a whole.
 *
 * <p>The qualifier is recognised by its semantic ID; its value is an {@code xs:boolean}.
 * (The IDTA {@code SMT/Cardinality} qualifier is not used: it constrains how many elements a
 * template instance contains, not which values a call carries.)
 */
public final class OptionalQualifier {

	public static final String SEMANTIC_ID = "https://smartproductionlab.aau.dk/aas/OperationVariable/Optional/1/0";
	public static final String TYPE = "Optional";

	private OptionalQualifier() {
	}

	/** True when {@code element} is marked optional; an unreadable qualifier is reported by {@link #problems}. */
	public static boolean optional(SubmodelElement element) {
		Qualifier qualifier = qualifier(element);
		return qualifier != null && booleanValue(qualifier.getValue()) == Boolean.TRUE;
	}

	/** The authoring errors of the qualifier on a variable and its members, each prefixed with its path. */
	public static List<String> problems(SubmodelElement variable) {
		List<String> problems = new ArrayList<>();
		collect(variable, variable.getIdShort(), false, problems);
		return problems;
	}

	private static void collect(SubmodelElement element, String path, boolean listMember, List<String> problems) {
		Qualifier qualifier = qualifier(element);
		if (qualifier != null && booleanValue(qualifier.getValue()) == null)
			problems.add(path + ": the Optional qualifier needs an xs:boolean value, got " + qualifier.getValue());
		if (qualifier != null && listMember)
			problems.add(path + ": list members share one template; mark the list itself Optional");
		if (element instanceof SubmodelElementCollection collection)
			for (SubmodelElement child : Aas.nonNull(collection.getValue()))
				collect(child, path + "/" + child.getIdShort(), false, problems);
		if (element instanceof SubmodelElementList list)
			for (SubmodelElement item : Aas.nonNull(list.getValue())) collect(item, path + "/items", true, problems);
	}

	/**
	 * Fails when a required value is absent: the variable itself ({@code present} false, or a
	 * null value) or, for a present value, a required member of a declared collection, also
	 * inside list items. Values whose shape contradicts the template are left to the reply
	 * builder, which reports them.
	 */
	public static void requirePresent(SubmodelElement template, boolean present, Object value, String path,
			Function<String, RuntimeException> failure) {
		boolean absent = !present || value == null || value == JsonPointer.JSON_NULL;
		if (absent) {
			if (!optional(template)) throw failure.apply(path);
			return;
		}
		if (template instanceof SubmodelElementCollection collection && value instanceof Map<?, ?> members) {
			for (SubmodelElement child : Aas.nonNull(collection.getValue()))
				requirePresent(child, members.containsKey(child.getIdShort()), members.get(child.getIdShort()),
						path + "/" + child.getIdShort(), failure);
		}
		if (template instanceof SubmodelElementList list && value instanceof List<?> items) {
			SubmodelElement item = OperationVariableValues.itemTemplate(list);
			if (item == null) return;
			for (int index = 0; index < items.size(); index++)
				requirePresentItem(item, items.get(index), path + "/" + index, failure);
		}
	}

	/** A list item is always required (it is an element of the list); its members follow their own marks. */
	private static void requirePresentItem(SubmodelElement item, Object value, String path,
			Function<String, RuntimeException> failure) {
		if (value == null || value == JsonPointer.JSON_NULL) throw failure.apply(path);
		requirePresent(item, true, value, path, failure);
	}

	private static Qualifier qualifier(SubmodelElement element) {
		for (Qualifier qualifier : Aas.nonNull(element.getQualifiers()))
			if (SEMANTIC_ID.equals(firstKey(qualifier.getSemanticId()))) return qualifier;
		return null;
	}

	private static String firstKey(Reference reference) {
		return reference == null || reference.getKeys() == null || reference.getKeys().isEmpty() ? null
				: reference.getKeys().get(0).getValue();
	}

	/** The xs:boolean lexical forms; null for anything else. */
	private static Boolean booleanValue(String value) {
		if (value == null) return null;
		return switch (value.trim()) {
			case "true", "1" -> Boolean.TRUE;
			case "false", "0" -> Boolean.FALSE;
			default -> null;
		};
	}
}
