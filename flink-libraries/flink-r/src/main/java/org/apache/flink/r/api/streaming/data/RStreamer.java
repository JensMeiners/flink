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
package org.apache.flink.r.api.streaming.data;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.api.streaming.util.SerializationUtils;
import org.apache.flink.python.api.streaming.util.StreamPrinter;
import org.apache.flink.r.api.RPlanBinder;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;

import static org.apache.flink.r.api.RPlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.r.api.RPlanBinder.FLINK_R_DC_ID;
import static org.apache.flink.r.api.RPlanBinder.FLINK_R_BINARY_PATH;
import static org.apache.flink.r.api.RPlanBinder.PLANBINDER_CONFIG_BCVAR_COUNT;
import static org.apache.flink.r.api.RPlanBinder.PLANBINDER_CONFIG_BCVAR_NAME_PREFIX;
import static org.apache.flink.r.api.RPlanBinder.FLINK_R_PLAN_NAME;

/**
 * This streamer is used by functions to send/receive data to/from an external R process.
 */
public class RStreamer implements Serializable {
	protected static final Logger LOG = LoggerFactory.getLogger(RStreamer.class);
	private static final int SIGNAL_BUFFER_REQUEST = 0;
	private static final int SIGNAL_BUFFER_REQUEST_G0 = -3;
	private static final int SIGNAL_BUFFER_REQUEST_G1 = -4;
	private static final int SIGNAL_FINISHED = -1;
	private static final int SIGNAL_ERROR = -2;
	private static final byte SIGNAL_LAST = 32;

	private final int id;
	private final String planArguments;

	private String inputFilePath;
	private String outputFilePath;

	private Process process;
	private Thread shutdownThread;
	protected ServerSocket server;
	protected Socket socket;
	protected DataInputStream in;
	protected DataOutputStream out;
	protected int port;

	protected RSender sender;
	protected RReceiver receiver;

	protected StringBuilder msg = new StringBuilder();

	protected final AbstractRichFunction function;

	public RStreamer(AbstractRichFunction function, int id, boolean usesByteArray) {
		this.id = id;
		planArguments = RPlanBinder.arguments.toString();
		sender = new RSender();
		receiver = new RReceiver(usesByteArray);
		this.function = function;
	}

	/**
	 * Starts the R script.
	 *
	 * @throws IOException
	 */
	public void open() throws IOException {
		server = new ServerSocket(0);
		startR();
	}

	private void startR() throws IOException {
		this.outputFilePath = FLINK_TMP_DATA_DIR + "/" + id + this.function.getRuntimeContext().getIndexOfThisSubtask() + "output";
		this.inputFilePath = FLINK_TMP_DATA_DIR + "/" + id + this.function.getRuntimeContext().getIndexOfThisSubtask() + "input";

		sender.open(inputFilePath);
		receiver.open(outputFilePath);

		String path = function.getRuntimeContext().getDistributedCache().getFile(FLINK_R_DC_ID).getAbsolutePath();
		String planPath = path + FLINK_R_PLAN_NAME;


		try {
			Runtime.getRuntime().exec(FLINK_R_BINARY_PATH);
		} catch (IOException ex) {
			throw new RuntimeException(FLINK_R_BINARY_PATH + " does not point to a valid R binary.");
		}

		process = Runtime.getRuntime().exec(FLINK_R_BINARY_PATH + " " + planPath + planArguments);
		new StreamPrinter(process.getInputStream()).start();
		new StreamPrinter(process.getErrorStream(), true, msg).start();

		shutdownThread = new Thread() {
			@Override
			public void run() {
				try {
					destroyProcess();
				} catch (IOException ex) {
				}
			}
		};

		Runtime.getRuntime().addShutdownHook(shutdownThread);

		OutputStream processOutput = process.getOutputStream();
		processOutput.write("operator\n".getBytes());
		processOutput.write(("" + server.getLocalPort() + "\n").getBytes());
		processOutput.write((id + "\n").getBytes());
		processOutput.write((this.function.getRuntimeContext().getIndexOfThisSubtask() + "\n").getBytes());
		processOutput.write((inputFilePath + "\n").getBytes());
		processOutput.write((outputFilePath + "\n").getBytes());
		processOutput.flush();

		try { // wait a bit to catch syntax errors
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}
		try {
			process.exitValue();
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely." + msg);
		} catch (IllegalThreadStateException ise) { //process still active -> start receiving data
		}

		socket = server.accept();
		in = new DataInputStream(socket.getInputStream());
		out = new DataOutputStream(socket.getOutputStream());
	}

	/**
	 * Closes this streamer.
	 *
	 * @throws IOException
	 */
	public void close() throws IOException {
		try {
			socket.close();
			sender.close();
			receiver.close();
		} catch (Exception e) {
			LOG.error("Exception occurred while closing Streamer. :" + e.getMessage());
		}
		destroyProcess();
		if (shutdownThread != null) {
			Runtime.getRuntime().removeShutdownHook(shutdownThread);
		}
	}

	private void destroyProcess() throws IOException {
		try {
			process.exitValue();
		} catch (IllegalThreadStateException ise) { //process still active
			if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
				int pid;
				try {
					Field f = process.getClass().getDeclaredField("pid");
					f.setAccessible(true);
					pid = f.getInt(process);
				} catch (Throwable e) {
					process.destroy();
					return;
				}
				String[] args = new String[]{"kill", "-9", "" + pid};
				Runtime.getRuntime().exec(args);
			} else {
				process.destroy();
			}
		}
	}

	private void sendWriteNotification(int size, boolean hasNext) throws IOException {
		System.out.println("size = " + size + " - " + "hasNext = " + hasNext);
		out.writeInt(size);
		out.writeByte(hasNext ? 0 : SIGNAL_LAST);
		out.flush();
	}

	private void sendReadConfirmation() throws IOException {
		out.writeByte(1);
		out.flush();
	}

	/**
	 * Sends all broadcast-variables encoded in the configuration to the external process.
	 *
	 * @param config configuration object containing broadcast-variable count and names
	 * @throws IOException
	 */
	public final void sendBroadCastVariables(Configuration config) throws IOException {
		try {
			int broadcastCount = config.getInteger(PLANBINDER_CONFIG_BCVAR_COUNT, 0);

			String[] names = new String[broadcastCount];

			for (int x = 0; x < names.length; x++) {
				names[x] = config.getString(PLANBINDER_CONFIG_BCVAR_NAME_PREFIX + x, null);
			}

			out.write(new SerializationUtils.IntSerializer().serializeWithoutTypeInfo(broadcastCount));

			SerializationUtils.StringSerializer stringSerializer = new SerializationUtils.StringSerializer();
			for (String name : names) {
				Iterator bcv = function.getRuntimeContext().getBroadcastVariable(name).iterator();

				out.write(stringSerializer.serializeWithoutTypeInfo(name));

				while (bcv.hasNext()) {
					out.writeByte(1);
					out.write((byte[]) bcv.next());
				}
				out.writeByte(0);
			}
		} catch (SocketTimeoutException ste) {
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}

	/**
	 * Sends all values contained in the iterator to the external process and collects all results.
	 *
	 * @param i iterator
	 * @param c collector
	 * @throws IOException
	 */
	public final void streamBufferWithoutGroups(Iterator i, Collector c) throws IOException {
		try {
			int size;
			if (i.hasNext()) {
				while (true) {
					int sig = in.readInt();
					switch (sig) {
						case SIGNAL_BUFFER_REQUEST:
							if (i.hasNext() || sender.hasRemaining(0)) {
								size = sender.sendBuffer(i, 0);
								sendWriteNotification(size, sender.hasRemaining(0) || i.hasNext());
							} else {
								System.out.println("DELETE ME");
								throw new RuntimeException("External process requested data even though none is available.");
							}
							break;
						case SIGNAL_FINISHED:
							return;
						case SIGNAL_ERROR:
							try { //wait before terminating to ensure that the complete error message is printed
								Thread.sleep(2000);
							} catch (InterruptedException ex) {
							}
							throw new RuntimeException(
									"External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely due to an error." + msg);
						default:
							receiver.collectBuffer(c, sig);
							sendReadConfirmation();
							break;
					}
				}
			}
		} catch (SocketTimeoutException ste) {
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}

	/**
	 * Sends all values contained in both iterators to the external process and collects all results.
	 *
	 * @param i1 iterator
	 * @param i2 iterator
	 * @param c collector
	 * @throws IOException
	 */
	public final void streamBufferWithGroups(Iterator i1, Iterator i2, Collector c) throws IOException {
		try {
			int size;
			if (i1.hasNext() || i2.hasNext()) {
				while (true) {
					int sig = in.readInt();
					switch (sig) {
						case SIGNAL_BUFFER_REQUEST_G0:
							if (i1.hasNext() || sender.hasRemaining(0)) {
								size = sender.sendBuffer(i1, 0);
								sendWriteNotification(size, sender.hasRemaining(0) || i1.hasNext());
							}
							break;
						case SIGNAL_BUFFER_REQUEST_G1:
							if (i2.hasNext() || sender.hasRemaining(1)) {
								size = sender.sendBuffer(i2, 1);
								sendWriteNotification(size, sender.hasRemaining(1) || i2.hasNext());
							}
							break;
						case SIGNAL_FINISHED:
							return;
						case SIGNAL_ERROR:
							try { //wait before terminating to ensure that the complete error message is printed
								Thread.sleep(2000);
							} catch (InterruptedException ex) {
							}
							throw new RuntimeException(
									"External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely due to an error." + msg);
						default:
							receiver.collectBuffer(c, sig);
							sendReadConfirmation();
							break;
					}
				}
			}
		} catch (SocketTimeoutException ste) {
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}
}
