/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.management.model;

import java.util.Arrays;
import java.util.List;

import org.eclipse.digitaltwin.aas4j.v3.model.Key;
import org.eclipse.digitaltwin.aas4j.v3.model.Operation;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.Qualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.QualifierKind;
import org.eclipse.digitaltwin.aas4j.v3.model.Reference;
import org.eclipse.digitaltwin.aas4j.v3.model.ReferenceTypes;
import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultKey;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperation;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultQualifier;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultReference;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodel;
import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementCollection;

/** Compact AAS4J builders for compiler and resolver tests. */
public final class AasFixtures {
	private AasFixtures() { }

	public static Property property(String idShort, String valueType, String value) {
		return new DefaultProperty.Builder().idShort(idShort).valueType(Aas.xsd(valueType)).value(value).build();
	}

	/** An {@code xs:string} Property. */
	public static Property property(String idShort, String value) {
		return property(idShort, "xs:string", value);
	}

	public static SubmodelElementCollection collection(String idShort, SubmodelElement... children) {
		return new DefaultSubmodelElementCollection.Builder().idShort(idShort).value(Arrays.asList(children)).build();
	}

	public static OperationVariable variable(SubmodelElement value) {
		return new DefaultOperationVariable.Builder().value(value).build();
	}

	public static Operation operation(String idShort, List<OperationVariable> inputs, List<OperationVariable> outputs,
			List<OperationVariable> inoutputs) {
		return new DefaultOperation.Builder().idShort(idShort).inputVariables(inputs).outputVariables(outputs)
				.inoutputVariables(inoutputs).build();
	}

	/** The BaSyx invocationDelegation qualifier pointing an Operation at {@code url}. */
	public static Qualifier delegation(String url) {
		return new DefaultQualifier.Builder().type("invocationDelegation").valueType(Aas.xsd("xs:string")).value(url)
				.kind(QualifierKind.CONCEPT_QUALIFIER).build();
	}

	public static Submodel submodel(String id, String idShort, SubmodelElement... elements) {
		return new DefaultSubmodel.Builder().id(id).idShort(idShort).submodelElements(Arrays.asList(elements)).build();
	}

	/** A ModelReference from alternating key types and values, e.g. {@code "Submodel", "urn:x", "Property", "State"}. */
	public static Reference reference(String... typesAndValues) {
		List<Key> keys = new java.util.ArrayList<>();
		for (int i = 0; i < typesAndValues.length; i += 2)
			keys.add(new DefaultKey.Builder().type(Aas.keyType(typesAndValues[i])).value(typesAndValues[i + 1]).build());
		return new DefaultReference.Builder().type(ReferenceTypes.MODEL_REFERENCE).keys(keys).build();
	}
}
