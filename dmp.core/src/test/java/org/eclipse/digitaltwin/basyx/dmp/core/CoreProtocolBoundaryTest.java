package org.eclipse.digitaltwin.basyx.dmp.core;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.eclipse.digitaltwin.basyx.dmp.core.component.DmpComponent;
import org.junit.jupiter.api.Test;

/** Guards the connector boundary without bringing a protocol analysis library into core. */
class CoreProtocolBoundaryTest {
	@Test
	void compiledCoreHasNoConnectorPackageReferences() throws Exception {
		Path classes = Path.of(DmpComponent.class.getProtectionDomain().getCodeSource()
				.getLocation().toURI());
		List<String> forbidden = List.of(
				"org/eclipse/digitaltwin/basyx/dmp/aas/",
				"org/eclipse/milo/opcua/",
				"org/apache/camel/component/paho/",
				"org/apache/camel/component/milo/",
				"org/apache/camel/component/plc4x/",
				"org/apache/http/");
		try (Stream<Path> files = Files.walk(classes)) {
			for (Path file : files.filter(path -> path.toString().endsWith(".class")).toList()) {
				String constantPool = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
				for (String connectorPackage : forbidden) {
					assertFalse(constantPool.contains(connectorPackage),
							file.getFileName() + " has connector dependency " + connectorPackage);
				}
			}
		}
	}
}
