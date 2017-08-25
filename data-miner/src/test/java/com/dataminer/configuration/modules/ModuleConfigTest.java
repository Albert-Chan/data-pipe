package com.dataminer.configuration.modules;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ModuleConfigTest {

	@Test
	public void test() {
		ModuleConfig config = new ModuleConfig();
		config.setInputSchema("String", String.class);
		config.setInputSchema("Number", Double.class);
		config.setOutputSchema("StringOut", String.class);

		Class<?> clazz = config.getInputSchema("String");
		assertEquals("java.lang.String", clazz.getName());

	}
}
