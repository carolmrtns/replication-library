package org.replication.repository;

public interface MethodsRockDB {
	public void open(String directory);
	public byte[] get(String key);
	public void put(String key, String value);
	public void delete(String key);
}
