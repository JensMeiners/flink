/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.r.api.streaming.plan;

import org.apache.flink.python.api.streaming.util.StreamPrinter;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;

import static org.apache.flink.r.api.RPlanBinder.FLINK_R_BINARY_PATH;
import static org.apache.flink.r.api.RPlanBinder.FLINK_R_PLAN_NAME;


/**
 * Generic class to exchange data during the plan phase.
 */
public class RPlanStreamer implements Serializable {
	protected RPlanSender sender;
	protected RPlanReceiver receiver;

	private Process process;
	private ServerSocket server;
	private Socket socket;

	public Object getRecord() throws IOException {
		return getRecord(false);
	}

	public Object getRecord(boolean normalize) throws IOException {
		return receiver.getRecord(normalize);
	}

	public void sendRecord(Object record) throws IOException {
		sender.sendRecord(record);
	}

	public void open(String tmpPath, String args) throws IOException {
		server = new ServerSocket(0);
		startR(tmpPath, args);
		socket = server.accept();
		sender = new RPlanSender(socket.getOutputStream());
		receiver = new RPlanReceiver(socket.getInputStream());
	}

	private void startR(String tmpPath, String args) throws IOException {
		try {
			Runtime.getRuntime().exec(FLINK_R_BINARY_PATH);
		} catch (IOException ex) {
			throw new RuntimeException(FLINK_R_BINARY_PATH + " does not point to a valid R binary.");
		}
		process = Runtime.getRuntime().exec(FLINK_R_BINARY_PATH + " " + tmpPath + FLINK_R_PLAN_NAME + args);

		new StreamPrinter(process.getInputStream()).start();
		new StreamPrinter(process.getErrorStream()).start();

		try {
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}

		try {
			int value = process.exitValue();
			if (value != 0) {
				throw new RuntimeException("Plan file caused an error. Check log-files for details.");
			}
			if (value == 0) {
				throw new RuntimeException("Plan file exited prematurely without an error.");
			}
		} catch (IllegalThreadStateException ise) {//Process still running
		}

		process.getOutputStream().write("plan\n".getBytes());
		process.getOutputStream().write((server.getLocalPort() + "\n").getBytes());
		process.getOutputStream().flush();
	}

	public void close() {
		try {
			process.exitValue();
		} catch (NullPointerException npe) { //exception occurred before process was started
		} catch (IllegalThreadStateException ise) { //process still active
			process.destroy();
		}
	}
}
