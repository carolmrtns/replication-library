package org.replication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.concurrent.Semaphore;

import org.replication.models.MessageSocket;

public class ThreadSocket extends Thread {

	private Socket socket;
	ReplicaGroup replicaGroup;
	DataInputStream inputStream;
	Semaphore semaphore;

	public ThreadSocket(Socket socket, ReplicaGroup replicaGroup) {
		this.socket = socket;
		this.replicaGroup = replicaGroup;
	}

	public void run() {
		try {
			handleMessageClient();
		} catch (IOException e) {
			System.out.println("Exceção ao criar objetos de leitura/escrita Socket");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		}
	}

	public void handleMessageClient() throws IOException {
		ObjectInputStream objectInputStream = new ObjectInputStream(this.socket.getInputStream());
		DataOutputStream outputStream = new DataOutputStream(this.socket.getOutputStream());
		try {
			Object message;
			while ((message = objectInputStream.readObject()) != null) {
				MessageSocket messageSocket = (MessageSocket) message;

				if (messageSocket.getMethod().equals("get")) {
					this.semaphore = new Semaphore(0);
					this.replicaGroup.setSemaphore(this.semaphore);
					this.replicaGroup.broadcast(messageSocket.convertToJGroupsMessage());
					this.semaphore.acquire();

					String response = this.replicaGroup.getResponse();
					outputStream.writeUTF(response);
					outputStream.flush();
				} else {
					this.replicaGroup.broadcast(messageSocket.convertToJGroupsMessage());
				}

			}
		} catch (Exception e) {
			System.out.println("Exceção ao tratar mensagem recebida via Socket");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		} finally {
//			outputStream.close();
//			inputStream.close();
			this.socket.close();
		}
	}

}
