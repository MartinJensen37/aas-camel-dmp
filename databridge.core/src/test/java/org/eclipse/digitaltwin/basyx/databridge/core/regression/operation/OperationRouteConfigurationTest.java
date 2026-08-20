/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.databridge.core.regression.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.RestDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.operation.OperationRouteConfiguration;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

/**
 * Unit tests for the REST operation-delegation route configuration (M7).
 *
 * @author aas-camel-dmp
 */
class OperationRouteConfigurationTest {

	private static final String ROUTE_JSON = "{"
			+ "\"trigger\":\"operation\","
			+ "\"routeId\":\"halt_operation\","
			+ "\"datasource\":\"halt_rest\","
			+ "\"transformers\":[\"halt_request_transformer\"],"
			+ "\"datasinks\":[\"halt_command_sink\"],"
			+ "\"triggerData\":{"
			+ "\"responseDatasource\":\"halt_response_source\","
			+ "\"responseTransformer\":\"halt_response_transformer\","
			+ "\"timeoutSeconds\":30"
			+ "}}";

	@Test
	void restDataSourceBuildsDescriptiveUri() {
		RestDataSourceConfiguration source = new RestDataSourceConfiguration(
				"halt_rest", "POST", "/operations/syntegonStopperingSystemAAS/Halt", "application/json");

		assertEquals("POST", source.getRestMethod());
		assertEquals("/operations/syntegonStopperingSystemAAS/Halt", source.getRestPath());
		assertEquals("rest:POST:/operations/syntegonStopperingSystemAAS/Halt", source.getConnectionURI());
	}

	@Test
	void operationRouteConfigurationCarriesResponseHalfInTriggerData() {
		// Gson deserializes the routes.json entry (timeoutSeconds becomes a
		// Double), exactly as the DMP runtime sees it.
		RouteConfiguration base = new Gson().fromJson(ROUTE_JSON, RouteConfiguration.class);
		OperationRouteConfiguration config = new OperationRouteConfiguration(base);

		assertEquals(OperationRouteConfiguration.ROUTE_TRIGGER, config.getRouteTrigger());
		assertEquals("halt_operation", config.getRouteId());
		assertEquals("halt_rest", config.getDatasource());
		assertEquals("halt_response_source", config.getResponseDatasource());
		assertEquals("halt_response_transformer", config.getResponseTransformer());
		assertEquals(30L, config.getTimeoutSeconds());
		assertEquals("halt_command_sink", config.getDatasinks().get(0));
	}

	@Test
	void timeoutSecondsDefaultsToThirtyWhenAbsent() {
		String json = ROUTE_JSON.replace(",\"timeoutSeconds\":30", "");
		RouteConfiguration base = new Gson().fromJson(json, RouteConfiguration.class);
		OperationRouteConfiguration config = new OperationRouteConfiguration(base);

		assertEquals(30L, config.getTimeoutSeconds());
	}

	@Test
	void routeIdFromTriggerDataIsPresent() {
		RouteConfiguration base = new Gson().fromJson(ROUTE_JSON, RouteConfiguration.class);
		OperationRouteConfiguration config = new OperationRouteConfiguration(base);

		assertTrue(config.getResponseDatasource().equals("halt_response_source"));
		assertTrue(config.getResponseTransformer().equals("halt_response_transformer"));
	}
}
