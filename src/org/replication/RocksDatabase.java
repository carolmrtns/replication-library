package org.replication;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Semaphore;

import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDatabase {

	private File dbDir;
	private RocksDB db;

	public RocksDatabase() {
		RocksDB.loadLibrary();
	}

	public void open(String name) {
		final Options options = new Options();
		options.setCreateIfMissing(true);
		this.dbDir = new File("/tmp/", name);
		try {
			Files.createDirectories(dbDir.getParentFile().toPath());
			Files.createDirectories(dbDir.getAbsoluteFile().toPath());
			this.db = RocksDB.open(options, this.dbDir.getAbsolutePath());
		} catch (IOException | RocksDBException e) {
			System.out.println("Exceção ao inicializar RocksDB, confira configurações e permissões");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		}
	}

	public synchronized void put(String key, String value) {
		try {
			this.db.put(key.getBytes(), value.getBytes());
		} catch (RocksDBException e) {
			System.out.println("Exceção ao salvar entrada no RocksDB");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		}
	}

	public String get(String key) {
		String result = "";
		try {
			byte[] bytes = this.db.get(key.getBytes());
			if (bytes == null) {
				return "";
			}
			result = new String(bytes);
		} catch (RocksDBException e) {
			System.out.println("Exceção ao recuperar valor do RocksDB");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		}
		return result;
	}

	public void delete(String key) {
		try {
			this.db.delete(key.getBytes());
		} catch (RocksDBException e) {
			System.out.println("Exceção ao deletar valor da chave do RocksDB");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		}
	}

	public void close() {
		this.db.close();
	}

}
