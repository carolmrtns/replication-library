package org.replication;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;

public class ClientGroup implements Receiver {

	JChannel channelClient;
	JChannel replicaChannel;
	private static ClientGroup instance;

	public ClientGroup() {
	}

	public static ClientGroup getInstance() {
		if (instance == null) {
			instance = new ClientGroup();
		}
		return instance;
	}

	public void viewAccepted(View newView) {
		System.out.println("** view: " + newView);
	}

	public void receive(Message msg) {
		System.out.println("::::ClientGroup::::");
		System.out.println(msg.getSrc() + ": " + msg.getObject());
//		if (this.replicaChannel != null) {
//			try {
//				System.out.println("Enviando para réplicas...");
//				this.replicaChannel.send(msg);
//			} catch (Exception e) {
//				System.out.println("Erro ao enviar mensagem ao canal das réplicas. Mensagem: " + e.getMessage());
//			}
//		} else {
//			try {
//				this.replicaChannel = new JChannel().connect("Replicator");
//				this.replicaChannel.send(msg);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			System.out.println("Mensagem não enviada ao canal: this.replicaChannel = " + this.replicaChannel);
//		}
	}

	public JChannel getChannel() {
		return this.channelClient;
	}

	public void setChannel(String groupName) {
		try {
			this.channelClient = new JChannel().setReceiver(this).connect(groupName);
		} catch (Exception e) {
			System.out.println("Erro ao conectar no grupo. Mensagem: " + e.getMessage());
		}
	}

	public void setReplicaChannel(JChannel channel) {
		this.replicaChannel = channel;
	}

	public void close() {
		this.channelClient.close();
	}

}
