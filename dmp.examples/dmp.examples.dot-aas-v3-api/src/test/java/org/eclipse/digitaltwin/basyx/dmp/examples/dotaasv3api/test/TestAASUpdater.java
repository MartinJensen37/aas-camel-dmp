/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
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
package org.eclipse.digitaltwin.basyx.dmp.examples.dotaasv3api.test;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.http.HttpStatus;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.factory.AASDataSinkDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.component.DmpComponent;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.factory.RoutesConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.factory.LuaDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.factory.MqttDefaultConfigurationFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.broker.Server;
import io.moquette.broker.config.ClasspathResourceLoader;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.IResourceLoader;
import io.moquette.broker.config.ResourceLoaderConfig;

public class TestAASUpdater {

	private static final String PROPERTY_INTEGER_VALUE = "0.75";
	private static final String PROPERTY_INTEGER_VALUE_PATH = "/submodels/c3VibW9kZWxJZA==/submodel-elements/DotAASV3ConformantApiSMC.DotAASV3ConformantApiProperty/$value";
	private static final String PROPERTY_STRING_VALUE = "\"Bowler Hat\"";
	private static final String PROPERTY_STRING_VALUE_PATH = "/submodels/c3VibW9kZWxJZA==/submodel-elements/DotAASV3ConformantApiSMC.DotAASV3ConformantApiStringProperty/$value";

	private static Logger logger = LoggerFactory.getLogger(TestAASUpdater.class);

	private static DmpComponent updater;
	protected static Server mqttBroker;

	private static ClientAndServer mockServer;

	@BeforeClass
	public static void setUp() throws IOException {
		configureAndStartMockserver();

		configureAndStartMqttBroker();

		configureAndStartUpdaterComponent();
	}

	@AfterClass
	public static void tearDown() {
		updater.stopComponent();
		mockServer.close();
	}

	@Test
	public void getDotAASV3ConformantPropertyIntegerValue() throws MqttSecurityException, MqttPersistenceException, MqttException, InterruptedException {
		publishNewDatapoint("DotAASV3ConformantProperty");

		waitForPropagation();

		verifyPropertyValueUpdateRequestIntegerValue();
	}

	@Test
	public void getDotAASV3ConformantPropertyStringValue() throws MqttSecurityException, MqttPersistenceException, MqttException, InterruptedException {
		publishNewDatapoint("DotAASV3ConformantPropertyStringValue");

		waitForPropagation();

		verifyPropertyValueUpdateRequestStringValue();
	}

	private static void configureAndStartMockserver() {
		mockServer = ClientAndServer.startClientAndServer(4001);

		createExpectationForPatchRequestForIntegerValue();

		createExpectationForPatchRequestForStringValue();
	}

	private static void configureAndStartUpdaterComponent() {
		ClassLoader loader = TestAASUpdater.class.getClassLoader();
		RoutesConfiguration configuration = new RoutesConfiguration();

		RoutesConfigurationFactory routesFactory = new RoutesConfigurationFactory(loader);
		configuration.addRoutes(routesFactory.create());

		MqttDefaultConfigurationFactory mqttConfigFactory = new MqttDefaultConfigurationFactory(loader);
		configuration.addDatasources(mqttConfigFactory.create());

		AASDataSinkDefaultConfigurationFactory aasConfigFactory = new AASDataSinkDefaultConfigurationFactory(loader);
		configuration.addDatasinks(aasConfigFactory.create());

		LuaDefaultConfigurationFactory luaConfigFactory = new LuaDefaultConfigurationFactory(loader);
		configuration.addTransformers(luaConfigFactory.create());

		updater = new DmpComponent(configuration);
		updater.startComponent();
	}

	private void waitForPropagation() throws InterruptedException {
		Thread.sleep(5000);
	}

	private void publishNewDatapoint(String topic) throws MqttException, MqttSecurityException, MqttPersistenceException {
		logger.info("Publishing event to {}", topic);

		String json = "{\"Account\":{\"Account Name\":\"Firefly\",\"Order\":[{\"OrderID\":\"order103\",\"Product\":[{\"Product Name\":\"Bowler Hat\",\"ProductID\":858383,\"SKU\":\"0406654608\",\"Description\":{\"Colour\":\"Purple\",\"Width\":300,\"Height\":200,\"Depth\":210,\"Weight\":0.75},\"Price\":34.45,\"Quantity\":2},{\"Product Name\":\"Trilby hat\",\"ProductID\":858236,\"SKU\":\"0406634348\",\"Description\":{\"Colour\":\"Orange\",\"Width\":300,\"Height\":200,\"Depth\":210,\"Weight\":0.6},\"Price\":21.67,\"Quantity\":1}]},{\"OrderID\":\"order104\",\"Product\":[{\"Product Name\":\"Bowler Hat\",\"ProductID\":858383,\"SKU\":\"040657863\",\"Description\":{\"Colour\":\"Purple\",\"Width\":300,\"Height\":200,\"Depth\":210,\"Weight\":0.75},\"Price\":34.45,\"Quantity\":4},{\"ProductID\":345664,\"SKU\":\"0406654603\",\"Product Name\":\"Cloak\",\"Description\":{\"Colour\":\"Black\",\"Width\":30,\"Height\":20,\"Depth\":210,\"Weight\":2},\"Price\":107.99,\"Quantity\":1}]}]}}";
		MqttClient mqttClient = new MqttClient("tcp://localhost:1884", "testClient", new MemoryPersistence());
		mqttClient.connect();
		mqttClient.publish(topic, new MqttMessage(json.getBytes()));
		mqttClient.disconnect();
		mqttClient.close();
	}

	private static void configureAndStartMqttBroker() throws IOException {
		mqttBroker = new Server();
		IResourceLoader classpathLoader = new ClasspathResourceLoader();
		final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);
		mqttBroker.startServer(classPathConfig);
	}

	@SuppressWarnings("resource")
	private static void createExpectationForPatchRequestForIntegerValue() {
		new MockServerClient("localhost", 4001).when(request().withMethod("PATCH")
				.withPath(PROPERTY_INTEGER_VALUE_PATH)
				.withHeader("Content-Type", "application/json"))
				.respond(response().withStatusCode(HttpStatus.SC_CREATED)
						.withHeaders(new Header("Content-Type", "application/json; charset=utf-8")));
	}

	@SuppressWarnings("resource")
	private static void createExpectationForPatchRequestForStringValue() {
		new MockServerClient("localhost", 4001).when(request().withMethod("PATCH")
				.withPath(PROPERTY_STRING_VALUE_PATH)
				.withHeader("Content-Type", "application/json"))
				.respond(response().withStatusCode(HttpStatus.SC_CREATED)
						.withHeaders(new Header("Content-Type", "application/json; charset=utf-8")));
	}

	@SuppressWarnings("resource")
	private void verifyPropertyValueUpdateRequestIntegerValue() {
		verifyPropertyValueUpdateRequest(PROPERTY_INTEGER_VALUE_PATH, PROPERTY_INTEGER_VALUE);
	}

	@SuppressWarnings("resource")
	private void verifyPropertyValueUpdateRequestStringValue() {
		verifyPropertyValueUpdateRequest(PROPERTY_STRING_VALUE_PATH, PROPERTY_STRING_VALUE);
	}

	private void verifyPropertyValueUpdateRequest(String path, String expectedBody) {
		org.mockserver.model.HttpRequest[] requests = new MockServerClient("localhost", 4001)
				.retrieveRecordedRequests(request().withMethod("PATCH").withPath(path));
		assertEquals("expected one PATCH to " + path, 1, requests.length);
		String actual = new String(requests[0].getBodyAsRawBytes(), java.nio.charset.StandardCharsets.UTF_8);
		assertEquals("wrong physical JSON value for " + path,
				JsonCodec.parseJson(expectedBody), JsonCodec.parseJson(actual));
	}
}
