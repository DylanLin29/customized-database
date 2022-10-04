package hw1;

import java.io.*;

import hw4.BufferPool;

/*
 * Student 1 name: Yifan Yuan
 * Student 2 name: Dylan Lin
 * Date: 
 */

/**
 * Database is a class that initializes a static
 * variable used by the database system (the catalog)
 * 
 * Provides a set of methods that can be used to access these variables
 * from anywhere.
 */

public class Database {
	private static Database _instance = new Database();
	private final Catalog _catalog;
	private static BufferPool _bufferPool;

	// private final

	private Database() {
		_catalog = new Catalog();
	}

	/** Return the catalog of the static Database instance */
	public static Catalog getCatalog() {
		return _instance._catalog;
	}

	// reset the database, used for unit tests only.
	public static void reset() {
		_instance = new Database();
	}

	public static BufferPool resetBufferPool(int numPages) {
		_bufferPool = new BufferPool(numPages);
		return _bufferPool;
	}

	public static BufferPool getBufferPool() {
		return _bufferPool;
	}
}