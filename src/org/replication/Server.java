package org.replication;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {

	int port;
	ServerSocket serverSocket;
	Socket socket;
	ThreadSocket threadSocket;

	ReplicaGroup replicaGroup;

	public Server(int port, ReplicaGroup replicaGroup) {
		this.port = port;
		this.replicaGroup = replicaGroup;
	}

	public void start() throws IOException {
		this.serverSocket = new ServerSocket(this.port);
		while (true) {
			this.socket = this.serverSocket.accept();
			this.threadSocket = new ThreadSocket(this.socket, this.replicaGroup);
			this.threadSocket.start();
		}
	}

}
