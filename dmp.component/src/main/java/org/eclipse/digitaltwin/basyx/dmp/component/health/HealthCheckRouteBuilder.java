/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
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
package org.eclipse.digitaltwin.basyx.dmp.component.health;


import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.eclipse.digitaltwin.basyx.dmp.component.ManagedRuntimeStatus;

/**
 * Builds the health check route via the consolidated camel-rest surface
 * (M7). The DMP exposes BOTH {@code /health} and the REST operation-delegation
 * interfaces of the AID on ONE HTTP surface (camel-rest backed by
 * camel-jetty), so the health route is a plain {@code GET /health} rest
 * definition sharing the rest configuration with the operation routes.
 *
 * @author danish, aas-camel-dmp
 */
public class HealthCheckRouteBuilder extends RouteBuilder {

	@Override
	public void configure() throws Exception {
		restConfiguration()
				.component("jetty")
				.host("0.0.0.0")
				.port(HealthCheckUtils.getHealthCheckPort())
				.inlineRoutes(false)
				.bindingMode(RestBindingMode.off);

		rest("/health")
				.get()
				.produces("application/json")
				.to("direct:health");
		rest("/management").get("/status")
				.produces("application/json")
				.to("direct:managed-status");
		from("direct:managed-status")
			.routeId("managed-runtime-status")
			.process(exchange -> exchange.getMessage().setBody(ManagedRuntimeStatus.asMap()))
			.marshal().json(JsonLibrary.Gson);

		// Camel merges the rest consumer and the direct route it targets into a
		// single route; the direct route's id (the canonical health route id) wins.
		from("direct:health")
				.routeId(HealthCheckUtils.ROUTE_ID)
				.setHeader(Exchange.CONTENT_TYPE).constant("application/json")
				.bean(HealthCheckProcessor.class)
				.marshal().json(JsonLibrary.Gson);
	}
}
