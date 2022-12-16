package org.replication;

import java.io.IOException;

import org.replication.server.ReplicaGroup;
import org.replication.server.Server;

public class ReplicationLibrary {

	public static void main(String[] args) {
		ReplicaGroup replicaGroup = new ReplicaGroup("Replicator");

		// Servidor Socket
		int port = Integer.parseInt(args[0]);
		Server server = new Server(port, replicaGroup);
		try {
			server.start();
		} catch (IOException e) {
			System.out.println("Erro ao iniciar servidor Socker. Mensagem: " + e.getMessage());
		}
	}

}
