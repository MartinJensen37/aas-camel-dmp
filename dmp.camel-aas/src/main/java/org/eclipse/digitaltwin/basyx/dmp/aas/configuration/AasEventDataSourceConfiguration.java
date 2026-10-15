/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.aas.configuration;

import java.util.LinkedHashMap;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.aas.event.AasEventComponent;
import org.eclipse.digitaltwin.basyx.dmp.aas.event.AasPropertyEventAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;

/**
 * An AAS Property source driven by repository change events instead of polling. Each matching element event
 * contributes the Property's new value; an event without one is enriched by reading {@code readUrl}.
 */
public class AasEventDataSourceConfiguration extends DataSourceConfiguration {
	private final String submodelId;
	private final String idShortPath;

	public AasEventDataSourceConfiguration(String uniqueId, String submodelId, String idShortPath, String readUrl) {
		super(uniqueId, null, 0);
		this.submodelId = submodelId;
		this.idShortPath = idShortPath;
		setSourceKind(AasPropertyEventAdapter.SOURCE_KIND);
		setDelivery("async");
		Map<String, Object> filter = new LinkedHashMap<>();
		filter.put("submodelId", submodelId);
		filter.put("elementPath", idShortPath);
		filter.put("modelType", "Property");
		filter.put("readUrl", readUrl);
		setEventFilter(filter);
	}

	public String getSubmodelId() { return submodelId; }
	public String getIdShortPath() { return idShortPath; }

	@Override
	public String getConnectionURI() {
		return AasEventComponent.uri(submodelId, idShortPath);
	}
}
