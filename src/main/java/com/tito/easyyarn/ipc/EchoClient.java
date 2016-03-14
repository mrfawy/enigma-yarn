package com.tito.easyyarn.ipc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class EchoClient {
	public static void main(String[] args) throws IOException {

		String hostName = "localhost";
		int portNumber = 8080;

		int i=0;
		try (Socket echoSocket = new Socket(hostName, portNumber);
				ObjectOutputStream out = new ObjectOutputStream(echoSocket.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(echoSocket.getInputStream());

				BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in))) {
			String userInput;
			while ((userInput = stdIn.readLine()) != null) {
				Message m=new Message(new DestinationContext(DestinationType.P2P, "task1"));
				m.setOperation("Operation x");
				out.writeObject(m);
				System.out.println("echo: " + in.readObject().toString());
			}
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host " + hostName);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to " + hostName);
			System.exit(1);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
