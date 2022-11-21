package org.replication;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDatabase {

	private File dbDir;
	private RocksDB db;
	int totalCommands = 1000000000;
	int counter = 0;
	BufferedWriter writer;

	public RocksDatabase() {
		RocksDB.loadLibrary();

		long time = System.currentTimeMillis();
		try {
			writer = new BufferedWriter(new FileWriter("../logs/throughput-" + time + ".txt"));
//			for(int i = 0; i<10; i++) {
//				writer.write("testeeee"+i);
//			}
//			writer.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Inicia thread com as estatisticas
		final Thread stats = new Thread("StatsWriter") {
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
						// float t = (float)(time-last_time)/(1000*1000*1000);
						float t = (float) (time - last_time) / (1000000000);
						float count = commands_count / t;
						String timeThroughput = time + "|" + String.format("%.1f\n", count);
						System.out.printf(timeThroughput); // antes do count adicionar
//						System.out.println("counter: " + counter);
						try {
							writer.write(timeThroughput);
							writer.flush();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						last_commands_count += commands_count;
						last_time = time;
//						Thread.sleep(1000);
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
		this.dbDir = new File("/tmp/", name);
		try {
			Files.createDirectories(dbDir.getParentFile().toPath());
			Files.createDirectories(dbDir.getAbsoluteFile().toPath());
			this.db = RocksDB.open(options, this.dbDir.getAbsolutePath());
			counter++;
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
			counter++;
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
			counter++;
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
