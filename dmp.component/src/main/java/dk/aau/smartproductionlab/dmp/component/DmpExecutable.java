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
package dk.aau.smartproductionlab.dmp.component;

import dk.aau.smartproductionlab.dmp.core.audit.AuditLog;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;

/**
 * Starts the DMP. All routes are built at runtime from the AAS repository and
 * kept current from AAS/submodel events; there is no file-based configuration.
 *
 * @author danish
 */
public class DmpExecutable {

	private static DmpComponent component;
	private static ManagedDmpRuntime managedRuntime;
	private static InfluxAuditLog auditLog;
	private static boolean shutdownHookRegistered;

	public static void main(String[] args) throws IllegalArgumentException, SecurityException {
		if (args.length != 0) throw new IllegalArgumentException("the DMP takes no arguments; configure it through DMP_MANAGED_* environment variables");
		ManagedDmpRuntime runtime = new ManagedDmpRuntime(ManagedRuntimeSettings.fromEnvironment(System.getenv()),
				InfluxSettings.fromEnvironment(System.getenv()).orElse(null));
		startAuditLog(InfluxAuditLog.Settings.fromEnvironment(System.getenv()).orElse(null));
		try {
			runtime.start();
		} catch (RuntimeException startFailure) {
			stopAuditLog();
			throw startFailure;
		}
		managedRuntime = runtime;
		component = runtime.component();
		registerManagedShutdownHook();
	}

	public static DmpComponent getDmpComponent() {
		return component;
	}

	public static synchronized void stopManagedRuntime() {
		ManagedDmpRuntime runtime = managedRuntime;
		managedRuntime = null;
		try {
			if (runtime != null) runtime.close();
		} finally {
			component = null;
			stopAuditLog();
		}
	}

	/** Installs the AAS interaction log before any route runs; no settings means no log. */
	static synchronized void startAuditLog(InfluxAuditLog.Settings settings) {
		if (settings == null || auditLog != null) return;
		InfluxAuditLog log = new InfluxAuditLog(settings);
		auditLog = log;
		AuditLog.install(log);
		ManagedRuntimeStatus.trackAudit(log::status);
	}

	static synchronized void stopAuditLog() {
		InfluxAuditLog log = auditLog;
		auditLog = null;
		if (log == null) return;
		AuditLog.uninstall(log);
		ManagedRuntimeStatus.trackAudit(null);
		log.close();
	}

	private static synchronized void registerManagedShutdownHook() {
		if (shutdownHookRegistered) return;
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try { stopManagedRuntime(); } catch (RuntimeException ignored) { }
		}, "dmp-managed-shutdown"));
		shutdownHookRegistered = true;
	}
}
