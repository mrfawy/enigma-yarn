package com.tito.easyyarn.ipc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class EchoServer {

	public static void main(String[] args) {
		List<Thread> clientHandlers = new ArrayList<>();

		int portNumber = 8080;

		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(portNumber);
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

	}

}
