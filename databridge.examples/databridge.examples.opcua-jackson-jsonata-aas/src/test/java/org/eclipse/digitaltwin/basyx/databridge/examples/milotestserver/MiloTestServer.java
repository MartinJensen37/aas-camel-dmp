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
package org.eclipse.digitaltwin.basyx.databridge.examples.milotestserver;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.sdk.core.WriteMask;
import org.eclipse.milo.opcua.sdk.server.EndpointConfig;
import org.eclipse.milo.opcua.sdk.server.ManagedNamespaceWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.OpcUaServerConfigBuilder;
import org.eclipse.milo.opcua.sdk.server.identity.AnonymousIdentityValidator;
import org.eclipse.milo.opcua.sdk.server.identity.CompositeValidator;
import org.eclipse.milo.opcua.sdk.server.identity.UsernameIdentityValidator;
import org.eclipse.milo.opcua.sdk.server.items.DataItem;
import org.eclipse.milo.opcua.sdk.server.items.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.security.CertificateFactory;
import org.eclipse.milo.opcua.stack.core.security.DefaultApplicationGroup;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.DefaultServerCertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.MemoryCertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.MemoryCertificateStore;
import org.eclipse.milo.opcua.stack.core.security.MemoryTrustListManager;
import org.eclipse.milo.opcua.stack.core.security.RsaSha256CertificateFactory;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.transport.TransportProfile;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.structured.BuildInfo;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder;
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateGenerator;
import org.eclipse.milo.opcua.stack.transport.server.OpcServerTransportFactory;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransport;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransportConfig;

/**
 * Minimal OPC UA test server built NATIVELY on the Milo 1.1.6 API — the same
 * generation camel-milo 4.22 requires — replacing the Milo 0.6.x
 * {@code ExampleServer}, whose bytecode is incompatible with the 1.1.6 stack
 * ({@code DefaultCertificateManager}'s constructor changed).
 *
 * <p>
 * Exposes the classic ExampleServer shape the fixtures address:
 * {@code ns=2;s=HelloWorld/ScalarTypes/...} with a writable {@code Variant}
 * node plus static {@code Double}/{@code Integer} scalars. Two endpoints:
 * {@code None} (anonymous) and {@code Basic256Sha256}
 * (SignAndEncrypt, any username/password) — both backed by a real
 * self-signed certificate generated through the 1.1.6 certificate stack
 * ({@code DefaultApplicationGroup} + {@code MemoryCertificateStore} +
 * {@code RsaSha256CertificateFactory}).
 */
public class MiloTestServer {

	public static final String SECURITY_USERNAME = "demoUsername";
	public static final String SECURITY_PASSWORD = "demoPassword";

	private static final String NAMESPACE_URI = "urn:eclipse:digitaltwin:basyx:databridge:examples:milo";

	private final OpcUaServer server;
	private TestNamespace testNamespace;

	public MiloTestServer(int port) throws Exception {
		var certificateQuarantine = new MemoryCertificateQuarantine();

		var certificateGroup = DefaultApplicationGroup.createAndInitialize(
				new MemoryTrustListManager(),
				new MemoryCertificateStore(),
				new TestCertificateFactory(NAMESPACE_URI),
				new DefaultServerCertificateValidator(new MemoryTrustListManager(), certificateQuarantine));

		var certificateManager = new DefaultCertificateManager(certificateQuarantine, certificateGroup);

		X509Certificate certificate = certificateGroup.getCertificateEntries().stream()
				.findFirst()
				.map(entry -> entry.certificateChain[0])
				.orElseThrow(() -> new IllegalStateException("certificate group initialized without a certificate"));

		Set<EndpointConfig> endpoints = new LinkedHashSet<>();
		endpoints.add(endpoint(port, certificate, SecurityPolicy.None, MessageSecurityMode.None));
		endpoints.add(endpoint(port, certificate, SecurityPolicy.Basic256Sha256, MessageSecurityMode.SignAndEncrypt));

		var identityValidator = new CompositeValidator(new AnonymousIdentityValidator(),
				new UsernameIdentityValidator(authChallenge -> true));

		OpcUaServerConfig config = new OpcUaServerConfigBuilder()
				.setEndpoints(endpoints)
				.setApplicationName(LocalizedText.english("DMP MiloTestServer"))
				// MUST differ from the test namespace URI: the built-in
				// ServerNamespace registers the applicationUri in the
				// namespace table, and a collision would shift the test
				// namespace to ns=1 (the fixtures address ns=2)
				.setApplicationUri("urn:eclipse:digitaltwin:basyx:databridge:examples:milo:application")
				.setProductUri("urn:eclipse:digitaltwin:basyx:databridge:examples:product")
				.setBuildInfo(new BuildInfo(
						"urn:eclipse:digitaltwin:basyx:databridge:examples",
						"aas-camel-dmp",
						"MiloTestServer",
						"1.1.6", "", null))
				.setIdentityValidator(identityValidator)
				.setCertificateManager(certificateManager)
				// without a RoleMapper no session has any role and the
				// DefaultAccessController collapses every node's
				// UserAccessLevel/UserWriteMask to nothing (read-only) —
				// grant the well-known anonymous/authenticated roles so
				// test clients may write
				.setRoleMapper(identity -> List.of(
						NodeIds.WellKnownRole_Anonymous,
						NodeIds.WellKnownRole_AuthenticatedUser))
				.build();

		OpcServerTransportFactory transportFactory = transportProfile -> new OpcTcpServerTransport(
				OpcTcpServerTransportConfig.newBuilder().build());

		this.server = new OpcUaServer(config, transportFactory);
		this.testNamespace = new TestNamespace(server);
		// the server auto-starts only its built-in namespaces; a custom
		// namespace must be started explicitly to register its address
		// space and create its nodes before clients connect
		this.testNamespace.startup();
	}

	public OpcUaServer getServer() {
		return server;
	}

	public TestNamespace getNamespace() {
		return testNamespace;
	}

	public CompletableFuture<?> startup() {
		return server.startup();
	}

	public CompletableFuture<?> shutdown() {
		return server.shutdown();
	}

	private static EndpointConfig endpoint(int port, X509Certificate certificate, SecurityPolicy policy,
			MessageSecurityMode mode) {
		EndpointConfig.Builder builder = EndpointConfig.newBuilder()
				.setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
				.setBindAddress("127.0.0.1")
				.setBindPort(port)
				.setHostname("127.0.0.1")
				.setPath("milo")
				.setSecurityPolicy(policy)
				.setSecurityMode(mode);
		if (policy != SecurityPolicy.None) {
			builder.setCertificate(certificate);
		}
		return builder.build();
	}

	/**
	 * The test address space: {@code ns=2;s=HelloWorld/ScalarTypes/{Variant,
	 * Double, Integer}}. {@code Variant} starts at {@code 0} (the data-sink
	 * suites overwrite it); {@code Double}/{@code Integer} are the static
	 * scalars the data-source suites read.
	 */
	public static final class TestNamespace extends ManagedNamespaceWithLifecycle {

		private final SubscriptionModel subscriptionModel;
		private UaVariableNode variantNode;

		private TestNamespace(OpcUaServer server) {
			super(server, NAMESPACE_URI);

			this.subscriptionModel = new SubscriptionModel(server, this);
			getLifecycleManager().addLifecycle(subscriptionModel);
			getLifecycleManager().addStartupTask(this::createAndAddNodes);
		}

		@Override
		public void onDataItemsCreated(List<DataItem> dataItems) {
			subscriptionModel.onDataItemsCreated(dataItems);
		}

		@Override
		public void onDataItemsModified(List<DataItem> dataItems) {
			subscriptionModel.onDataItemsModified(dataItems);
		}

		@Override
		public void onDataItemsDeleted(List<DataItem> dataItems) {
			subscriptionModel.onDataItemsDeleted(dataItems);
		}

		@Override
		public void onMonitoringModeChanged(List<MonitoredItem> monitoredItems) {
			subscriptionModel.onMonitoringModeChanged(monitoredItems);
		}

		private void createAndAddNodes() {
			UaFolderNode root = new UaFolderNode(
					getNodeContext(),
					newNodeId("HelloWorld"),
					newQualifiedName("HelloWorld"),
					LocalizedText.english("HelloWorld"));

			getNodeManager().addNode(root);
			// Organizes from the Server object's ObjectsFolder → visible at the root
			root.addReference(new Reference(
					NodeIds.ObjectsFolder,
					NodeIds.Organizes,
					root.getNodeId().expanded(),
					Reference.Direction.INVERSE));

			UaFolderNode scalars = new UaFolderNode(
					getNodeContext(),
					newNodeId("HelloWorld/ScalarTypes"),
					newQualifiedName("ScalarTypes"),
					LocalizedText.english("ScalarTypes"));
			root.addComponent(scalars);
			getNodeManager().addNode(scalars);

			createScalar(scalars, "Variant", new Variant(0), NodeIds.BaseDataType);
			createScalar(scalars, "Double", new Variant(3.14d), NodeIds.Double);
			createScalar(scalars, "Integer", new Variant(32), NodeIds.Integer);
		}

		private void createScalar(UaFolderNode parent, String name, Variant value, NodeId dataType) {
			UaVariableNode node = UaVariableNode.builder(getNodeContext())
					.setNodeId(newNodeId("HelloWorld/ScalarTypes/" + name))
					.setBrowseName(newQualifiedName(name))
					.setDisplayName(LocalizedText.english(name))
					.setDataType(dataType)
					.setValueRank(-1)
					.setAccessLevel(AccessLevel.READ_WRITE)
					// the DefaultAccessController denies a write when the
					// (User)WriteMask does not advertise the attribute —
					// Milo's default is 0 (no writes permitted). The builder
					// also defaults userAccessLevel to read-only, so it must
					// be set explicitly for writes to pass.
					.setWriteMask(org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger.MAX)
					.setUserWriteMask(org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger.MAX)
					.setUserAccessLevel(AccessLevel.READ_WRITE)
					.setValue(new DataValue(value))
					.build();

			parent.addComponent(node);
			getNodeManager().addNode(node);
		}

		private void writeVariant(Object value) {
			if (variantNode == null) {
				variantNode = (UaVariableNode) getNodeManager()
						.getNode(new NodeId(2, "HelloWorld/ScalarTypes/Variant"))
						.orElseThrow(() -> new IllegalStateException("Variant node not initialized"));
			}
			variantNode.setValue(new DataValue(new Variant(value)));
		}
	}

	/**
	 * Concrete certificate factory: RsaSha256CertificateFactory implements
	 * keypair generation and signing-request handling; only the self-signed
	 * chain creation is application-specific.
	 */
	private static final class TestCertificateFactory extends RsaSha256CertificateFactory {

		private final String applicationUri;

		private TestCertificateFactory(String applicationUri) {
			this.applicationUri = applicationUri;
		}

		@Override
		protected X509Certificate[] createRsaSha256CertificateChain(KeyPair keyPair) throws Exception {
			// the generator adds the local host's IPs/DNS names as SANs by
			// default; the applicationUri links the cert to the endpoint
			X509Certificate certificate = new SelfSignedCertificateBuilder(keyPair)
					.setCommonName("DMP MiloTestServer")
					.setOrganization("aas-camel-dmp")
					.setOrganizationalUnit("dmp")
					.setApplicationUri(applicationUri)
					.build();
			return new X509Certificate[] { certificate };
		}
	}
}
