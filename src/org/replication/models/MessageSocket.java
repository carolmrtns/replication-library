package org.replication.models;

import java.io.Serializable;
import java.util.concurrent.Semaphore;

import org.jgroups.CompositeMessage;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;

public class MessageSocket implements Serializable {

	private static final long serialVersionUID = 3009679584768448351L;
	private String method;
	private String directory;
	private String key;
	private String value;

	public MessageSocket(String method) {
		this.method = method;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getMethod() {
		return this.method;
	}

	public String getDirectory() {
		return this.directory;
	}

	public String getKey() {
		return this.key;
	}

	public String getValue() {
		return this.value;
	}

	public Message convertToJGroupsMessage() {
		String method = getMethod();
		Message methodMessage = new ObjectMessage(null, method);
		switch (method) {
		case "open":
			Message directory = new ObjectMessage(null, getDirectory());
			return new CompositeMessage(null).add(methodMessage).add(directory);
		case "put":
			Message keyMsg = new ObjectMessage(null, getKey());
			Message valueMsg = new ObjectMessage(null, getValue());
			return new CompositeMessage(null).add(methodMessage).add(keyMsg).add(valueMsg);
		case "get":
		case "delete":
			keyMsg = new ObjectMessage(null, getKey());
			return new CompositeMessage(null).add(methodMessage).add(keyMsg);
		case "close":
			return new CompositeMessage(null).add(methodMessage);
		default:
			return null;
		}
	}

}
