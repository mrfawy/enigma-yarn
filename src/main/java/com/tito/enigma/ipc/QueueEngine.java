package com.tito.enigma.ipc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tito.enigma.yarn.task.Tasklet;

public class QueueEngine extends Tasklet {
	private static final Log LOG = LogFactory.getLog(QueueEngine.class);

	Queue queue = new Queue();
	private int port;

	@Override
	public boolean init(CommandLine commandLine) {
		if (!commandLine.hasOption("queueEnginePort")) {
			LOG.error("Missing Port for QueueEngine");
			return false;
		}
		port = Integer.parseInt(commandLine.getOptionValue("queueEnginePort"));
		return true;

	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("queueEnginePort", true, "port to listen to");

	}

	@Override
	public boolean start() {
		List<Thread> clientHandlers = new ArrayList<>();

		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(port);
			while (true) {
				Socket clientSocket = serverSocket.accept();
				Thread clientHandler = new Thread(new ClientHandler(clientSocket));
				clientHandlers.add(clientHandler);
				clientHandler.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (serverSocket != null) {
					serverSocket.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}

}
