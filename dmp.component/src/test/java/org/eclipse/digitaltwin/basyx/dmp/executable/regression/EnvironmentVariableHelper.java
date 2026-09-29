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


package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

/**
 * Enables test cases to work with environment variables
 * 
 * @author schnicke
 *
 */
public class EnvironmentVariableHelper {
	private static Map<String, String> originalEnvironment;

	/**
	 * Overwrites the given environment variables IN the process environment
	 * (merge semantics — never clears the whole map, so sibling tests in the
	 * same JVM keep a functional environment, e.g. PATH).
	 */
	public static void setEnvironmentVariablesForTesting(Map<String, String> newenv) {
		try {
			Class<?> processEnvironmentClass = getProcessEnvironmentClass();
			Field theEnvironment = getAccessibleField(processEnvironmentClass, "theEnvironment");
			setNewEnvironmentVariables(theEnvironment, newenv, false);
			Field theCaseInsensitiveEnvironmentField = getAccessibleField(processEnvironmentClass, "theCaseInsensitiveEnvironment");
			setNewEnvironmentVariables(theCaseInsensitiveEnvironmentField, newenv, false);
		} catch (NoSuchFieldException e) {
			setVariableToUnmodifiableMap(newenv);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Restores the process environment captured before the first mutation, so
	 * test classes that manipulate environment variables do not leak state
	 * into later suites of the same JVM run.
	 */
	public static void resetEnvironmentVariables() {
		if (originalEnvironment == null) {
			return;
		}
		try {
			Class<?> processEnvironmentClass = getProcessEnvironmentClass();
			try {
				setNewEnvironmentVariables(getAccessibleField(processEnvironmentClass, "theEnvironment"), originalEnvironment, true);
			} catch (NoSuchFieldException e) {
				// theEnvironment absent (or a non-HotSpot layout): fall through
				// to the UnmodifiableMap fallback below
			}
			try {
				setNewEnvironmentVariables(getAccessibleField(processEnvironmentClass, "theCaseInsensitiveEnvironment"), originalEnvironment, true);
			} catch (NoSuchFieldException e) {
				// Windows-only field — absent on Linux
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static Class<?> getProcessEnvironmentClass() throws ClassNotFoundException {
		Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
		return processEnvironmentClass;
	}

	private static Field getAccessibleField(Class<?> processEnvironmentClass, String fieldName) throws NoSuchFieldException, SecurityException {
		Field theEnvironmentField = processEnvironmentClass.getDeclaredField(fieldName);
		theEnvironmentField.setAccessible(true);
		return theEnvironmentField;
	}

	private static void setNewEnvironmentVariables(Field field, Map<String, String> newenv, boolean replace) throws IllegalArgumentException, IllegalAccessException {
		@SuppressWarnings("unchecked")
		Map<String, String> env = (Map<String, String>) field.get(null);
		if (originalEnvironment == null) {
			originalEnvironment = new java.util.LinkedHashMap<>(env);
		}
		if (replace) {
			env.clear();
			env.putAll(newenv);
		} else {
			env.putAll(newenv);
		}
	}

	private static void setVariableToUnmodifiableMap(Map<String, String> newenv) {
		try {
			Class<?>[] classes = Collections.class.getDeclaredClasses();
			Map<String, String> currentEnvironmentVariables = System.getenv();
			for (Class<?> cl : classes) {
				if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
					Field field = cl.getDeclaredField("m");
					field.setAccessible(true);
					Object obj = field.get(currentEnvironmentVariables);
					@SuppressWarnings("unchecked")
					Map<String, String> map = (Map<String, String>) obj;
					if (originalEnvironment == null) {
						originalEnvironment = new java.util.LinkedHashMap<>(map);
					}
					map.putAll(newenv);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
