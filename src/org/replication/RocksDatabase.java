package org.replication;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

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
		this.dbDir = new File("/tmp/rocks-db", name);
		try {
			Files.createDirectories(dbDir.getParentFile().toPath());
			Files.createDirectories(dbDir.getAbsoluteFile().toPath());
			this.db = RocksDB.open(options, this.dbDir.getAbsolutePath());
		} catch (IOException | RocksDBException ex) {
			System.out.println("Erro ao inicializar RocksDB, confira configurações e permissões, excessão: "
					+ ex.getCause() + ", mensagem: " + ex.getMessage() + ", StackTrace: " + ex.getStackTrace());
		}
	}

	public synchronized void put(String key, String value) {
		try {
			this.db.put(key.getBytes(), value.getBytes());
		} catch (RocksDBException e) {
			System.out.println(
					"Erro ao salvar entrada no RocksDB, causa: " + e.getCause() + ", mensagem: " + e.getMessage());
		}
	}
	
	public void get(String key) {
		try {
			String value = new String(this.db.get(key.getBytes()));
			System.out.println("Chave: " + key + " | Valor: " + value);
		} catch (RocksDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void delete(String key) {
		System.out.println("Excluindo chave " + key);
		try {
			this.db.delete(key.getBytes());
		} catch (RocksDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
