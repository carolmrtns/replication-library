package org.replication;

public class ReplicationLibrary {
	
	public static void main(String[] args) {
		ReplicaGroup replicaGroup = new ReplicaGroup();
		replicaGroup.connectChannel();
//		ReplicaGroup.getInstance().connectChannel();
	}

}
