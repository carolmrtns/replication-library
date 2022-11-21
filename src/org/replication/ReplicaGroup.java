package org.replication;

import java.util.List;
import java.util.concurrent.Semaphore;

import org.jgroups.Address;
import org.jgroups.CompositeMessage;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;

public class ReplicaGroup implements Receiver {

	JChannel channelReplica;
	RocksDatabase rocksDatabase;
	String response;
	Semaphore semaphore;

	public ReplicaGroup(String groupName) {
		try {
			this.rocksDatabase = new RocksDatabase();
			this.channelReplica = new JChannel().setReceiver(this).connect(groupName);
		} catch (Exception e) {
			System.out.println("Erro ao conectar no grupo. Mensagem: " + e.getMessage());
		}
	}

	public void viewAccepted(View newView) {
		System.out.println("** view: " + newView);
	}

	public void receive(Message msg) {
//		System.out.println(msg.getSrc() + ": " + msg.getObject());
		eventHandler(msg);
	}

	public void broadcast(Message msg) {
		try {
			this.channelReplica.send(msg);
		} catch (Exception e) {
			System.out.println("Exceção ao disseminar mensagem no grupo");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		}
	}

	public List<Address> getMembers() {
		View view = this.channelReplica.getView();
		return view.getMembers();
	}

	public int getIndexByMember() {
		List<Address> members = getMembers();
		for (int i = 0; i < members.size(); i++) {
			Address member = members.get(i);
			if (this.channelReplica.getAddress().compareTo(member) == 0) {
//				System.out.println("Index retornado: " + i);
				return i;
			}
		}
		return -1;
	}

	public void eventHandler(Message msg) {
		CompositeMessage compositeMessage = (CompositeMessage) msg;
		String method = compositeMessage.get(0).getObject();
		switch (method) {
		case "open":
			String name = compositeMessage.get(1).getObject();
			int number = getIndexByMember();
//			System.out.println(":::Valor para OPEN:::");
//			System.out.println("Name: " + name);
			this.rocksDatabase.open("rocks-db-" + number + "/" + name);
			break;
		case "delete":
			String key = compositeMessage.get(1).getObject();
//			System.out.println(":::Valor para DELETE:::");
//			System.out.println("Chave: " + key);
			this.rocksDatabase.delete(key);
			break;
		case "put":
			key = compositeMessage.get(1).getObject();
			String value = compositeMessage.get(2).getObject();
//			System.out.println(":::Valores para PUT:::");
//			System.out.println("Key: " + key + " | Value: " + value);
			this.rocksDatabase.put(key, value);
			break;
		case "get":
			key = compositeMessage.get(1).getObject();
			String response = this.rocksDatabase.get(key);
			setResponse(response);
			this.semaphore.release();
			System.out.println(":::Retorno do GET:::");
			System.out.println("Chave: " + key + " | Valor: " + response);
			break;
		case "close":
			this.rocksDatabase.close();
		default:
			break;
		}
	}

	public void setResponse(String response) {
		this.response = response;
	}

	public String getResponse() {
		return this.response;
	}

	public void setSemaphore(Semaphore semaphore) {
		this.semaphore = semaphore;
	}

	public Message setMessage(String message) {
		return new ObjectMessage(null, message);
	}

}
