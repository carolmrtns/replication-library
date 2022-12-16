package org.replication.repository;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDatabase implements MethodsRockDB {

	private File dbDir;
	private RocksDB db;
	private int counter = 0;
	private int totalCommands = 1000000;
	private BufferedWriter writer;

	public RocksDatabase() {
		RocksDB.loadLibrary();

		long time = System.currentTimeMillis();
		try {
			this.writer = new BufferedWriter(new FileWriter("../logs/throughput-" + time + ".txt"));
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		// Inicia thread com as estatisticas
		final Thread stats = new Thread() {
			private long last_time = System.nanoTime();
			private long last_commands_count = 0;
//			final AtomicLong stat_command = new AtomicLong();

			@Override
			public void run() {
				while (counter < totalCommands) {
					try {
						Thread.sleep(1000);
						long time = System.nanoTime();
						long commands_count = counter - last_commands_count;
						float t = (float) (time - last_time) / (1000000000);
						float count = commands_count / t;
						String timeThroughput = time + "|" + String.format("%.1f\n", count);
						System.out.printf(timeThroughput);
						try {
							writer.write(timeThroughput);
							writer.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
						last_commands_count += commands_count;
						last_time = time;
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
		};
		stats.start();

	}

	public void open(String name) {
		final Options options = new Options();
		options.setCreateIfMissing(true);
		this.dbDir = new File("/tmp/rocks-db/", name);
		try {
			Files.createDirectories(dbDir.getParentFile().toPath());
			Files.createDirectories(dbDir.getAbsoluteFile().toPath());
			this.db = RocksDB.open(options, this.dbDir.getAbsolutePath());
			this.counter++;

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
			this.counter++;
		} catch (RocksDBException e) {
			System.out.println("Exceção ao salvar entrada no RocksDB");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		}
	}

	public byte[] get(String key) {
		try {
			byte[] bytes = this.db.get(key.getBytes());
			if (bytes == null) {
				return null;
			}
			this.counter++;
			return bytes;
		} catch (RocksDBException e) {
			System.out.println("Exceção ao recuperar valor do RocksDB");
			System.out.println("Causa: " + e.getCause());
			System.out.println("Mensagem: " + e.getMessage());
			System.out.println("StackTrace: " + e.getStackTrace());
		}
		return null;
	}

	public void delete(String key) {
		try {
			this.db.delete(key.getBytes());
			this.counter++;
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
