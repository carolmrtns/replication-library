package org.replication.repository;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.replication.models.MessageSocket;

public class ReplicatedRocksDB implements MethodsRockDB{

	private Socket socket;
	private ObjectOutputStream objectOutputStream;
	private DataInputStream dataInputStream;

	public ReplicatedRocksDB(String address, int port) {
		try {
			this.socket = new Socket(address, port);
			this.objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
			this.dataInputStream = new DataInputStream(this.socket.getInputStream());
		} catch (IOException ioe) {
			System.out.println("Exceção instanciar ReplicatedRocksDB");
			System.out.println("Causa: " + ioe.getCause());
			System.out.println("Mensagem: " + ioe.getMessage());
			System.out.println("StackTrace: " + ioe.getStackTrace());
		}
	}

	public void open(String directory) {
		try {
			MessageSocket messageSocket = new MessageSocket("open");
			messageSocket.setDirectory(directory);
			this.objectOutputStream.writeObject(messageSocket);
		} catch (IOException ioe) {
			System.out.println("Exceção ao enviar solicitação para abrir diretório via Socket");
			System.out.println("Causa: " + ioe.getCause());
			System.out.println("Mensagem: " + ioe.getMessage());
			System.out.println("StackTrace: " + ioe.getStackTrace());
		}
	}

	public void put(String key, String value) {
		try {
			MessageSocket messageSocket = new MessageSocket("put");
			messageSocket.setKey(key);
			messageSocket.setValue(value);
			this.objectOutputStream.writeObject(messageSocket);
		} catch (IOException ioe) {
			System.out.println("Exceção ao enviar solicitação para salvar valor via Socket");
			System.out.println("Causa: " + ioe.getCause());
			System.out.println("Mensagem: " + ioe.getMessage());
			System.out.println("StackTrace: " + ioe.getStackTrace());
		}
	}

	public byte[] get(String key) {
		try {
			MessageSocket messageSocket = new MessageSocket("get");
			messageSocket.setKey(key);
			this.objectOutputStream.writeObject(messageSocket);
			String value = this.dataInputStream.readUTF();
			return value.getBytes();
		} catch (IOException ioe) {
			System.out.println("Exceção ao enviar solicitação para obter valor via Socket");
			System.out.println("Causa: " + ioe.getCause());
			System.out.println("Mensagem: " + ioe.getMessage());
			System.out.println("StackTrace: " + ioe.getStackTrace());
			return null;
		}
	}

	public void delete(String key) {
		try {
			MessageSocket messageSocket = new MessageSocket("delete");
			messageSocket.setKey(key);
			this.objectOutputStream.writeObject(messageSocket);
		} catch (IOException ioe) {
			System.out.println("Exceção ao enviar solicitação para excluir valor via Socket");
			System.out.println("Causa: " + ioe.getCause());
			System.out.println("Mensagem: " + ioe.getMessage());
			System.out.println("StackTrace: " + ioe.getStackTrace());
		}
	}

}
