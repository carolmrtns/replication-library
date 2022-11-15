package org.replication;

import java.util.List;

import org.jgroups.Address;
import org.jgroups.CompositeMessage;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;

public class ReplicaGroup implements Receiver {

	JChannel channelReplica;
	ClientGroup clientGroup;
	RocksDatabase rocksDatabase;
	ChannelClient channelClientInstance;

	public ReplicaGroup() {
		this.rocksDatabase = new RocksDatabase();
		this.channelClientInstance = ChannelClient.getInstance();
		this.channelClientInstance.setChannel("Client");
//		this.clientGroup = ClientGroup.getInstance();
	}

//	public static ReplicaGroup getInstance() {
//		if(instance == null) {
//			instance = new ReplicaGroup();
//		}
//		return instance;
//	}

	public void connectChannel() {
		try {
			this.channelReplica = new JChannel().setReceiver(this).connect("Replicator");
//			this.clientGroup.setReplicaChannel(this.channelReplica);
			this.channelClientInstance.setReplicaChannel(this.channelReplica);
		} catch (Exception e) {
			System.out.println("Erro ao conectar no grupo. Mensagem: " + e.getMessage());
		}
	}

	public void viewAccepted(View newView) {
		System.out.println("** view: " + newView);
	}

	public void receive(Message msg) {
//		eventHandler(msg);
		System.out.println("::::ReplicaGroup::::");
		System.out.println(msg.getSrc() + ": " + msg.getObject());
	}

	public void open(String name) {
		Message method = new ObjectMessage(null, "open");
		Message nameMsg = new ObjectMessage(null, name);
		Message msg = new CompositeMessage(null).add(method).add(nameMsg);
		try {
			this.channelReplica.send(msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void put(String key, String value) {
		Message method = new ObjectMessage(null, "put");
		Message keyMsg = new ObjectMessage(null, key);
		Message valueMsg = new ObjectMessage(null, value);
		Message msg = new CompositeMessage(null).add(method).add(keyMsg).add(valueMsg);
		try {
			this.channelReplica.send(msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void get(String key) {
		Message method = new ObjectMessage(null, "get");
		Message keyMsg = new ObjectMessage(null, key);
		Message msg = new CompositeMessage(null).add(method).add(keyMsg);
		try {
			this.channelReplica.send(msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void delete(String key) {
		Message method = new ObjectMessage(null, "delete");
		Message keyMsg = new ObjectMessage(null, key);
		Message msg = new CompositeMessage(null).add(method).add(keyMsg);
		try {
			this.channelReplica.send(msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				System.out.println("Index retornado: " + i);
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
			this.rocksDatabase.open(name + "-" + number);
			break;
		case "delete":
			String key = compositeMessage.get(1).getObject();
			this.rocksDatabase.delete(key);
			break;
		case "put":
			key = compositeMessage.get(1).getObject();
			String value = compositeMessage.get(2).getObject();
			this.rocksDatabase.put(key, value);
			break;
		case "get":
			key = compositeMessage.get(1).getObject();
			this.rocksDatabase.get(key);
			break;
		default:
			break;
		}
	}
}
