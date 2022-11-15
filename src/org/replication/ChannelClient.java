package org.replication;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;

public class ChannelClient implements Receiver {

	JChannel channel;
	JChannel replicaChannel;
	private static ChannelClient instance;

	public ChannelClient() {

	}

	public static ChannelClient getInstance() {
		if (instance == null) {
			instance = new ChannelClient();
		}
		return instance;
	}

	public void viewAccepted(View newView) {
		System.out.println("** view (ChannelClient): " + newView);
	}

	public void receive(Message msg) {
		System.out.println("::::ChannelClient::::");
		System.out.println(msg.getSrc() + ": " + msg.getObject());
		if (this.replicaChannel != null) {
			try {
				this.replicaChannel.send(msg);
			} catch (Exception e) {
				System.out.println("Erro ao enviar mensagem ao grupo. Mensagem: " + e.getMessage());
			}
		} else {
			System.out.println("Mensagem não foi enviada: this.replicaChannel = " + this.replicaChannel);
		}
	}

	public void setChannel(String groupName) {
		try {
			this.channel = new JChannel().connect(groupName);
		} catch (Exception e) {
			System.out.println("Erro ao conectar no grupo. Mensagem: " + e.getMessage());
		}
	}

	public void setReplicaChannel(JChannel channel) {
		this.replicaChannel = channel;
	}

	public JChannel getChannel() {
		return this.channel;
	}

}
