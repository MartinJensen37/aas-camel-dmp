package org.eclipse.digitaltwin.basyx.dmp.jsonata.component;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The exact payload shape the dot-aas-v3 example feeds the transform. */
class StringPropertyValueOutputTest extends CamelTestSupport {

	@TempDir
	Path tempDir;

	private String run(String query, String input) throws Exception {
		Path expression = tempDir.resolve("q.jsonata");
		Files.writeString(expression, query, StandardCharsets.UTF_8);
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:probe")
						.to("jsonata:file:" + expression + "?inputType=JsonString&outputType=JsonString")
						.to("mock:result");
			}
		});
		return template.requestBody("direct:probe", input, String.class);
	}

	@Test
	void aStringPropertyInsideTheSourceEnvelopeStaysQuoted() throws Exception {
		String out = run("exampleAAS1.value",
				"{\"exampleAAS1\":{\"idShort\":\"pressure\",\"valueType\":\"xs:string\","
						+ "\"value\":\"103.5585973\",\"modelType\":\"Property\"}}");
		assertEquals("\"103.5585973\"", out);
	}

	@Test
	void theSameValueIsUnquotedWhenThePropertyCarriedANumber() throws Exception {
		String out = run("exampleAAS1.value",
				"{\"exampleAAS1\":{\"idShort\":\"pressure\",\"valueType\":\"xs:double\","
						+ "\"value\":103.5585973,\"modelType\":\"Property\"}}");
		assertEquals("103.5585973", out);
	}
}
