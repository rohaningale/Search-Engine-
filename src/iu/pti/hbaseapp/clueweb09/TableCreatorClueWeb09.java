package iu.pti.hbaseapp.clueweb09;

import java.util.ArrayList;

import iu.pti.hbaseapp.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

public class TableCreatorClueWeb09 {
	
	public static void createCw09DataTable(HBaseAdmin admin, Configuration hbaseConfig) throws Exception {
		// create the clueWeb09 data table	
		if (admin.tableExists(Constants.CW09_DATA_TABLE_BYTES)) {
			admin.disableTable(Constants.CW09_DATA_TABLE_BYTES);
			admin.deleteTable(Constants.CW09_DATA_TABLE_BYTES);
		}
		
		HTableDescriptor tableDes = new HTableDescriptor(Constants.CW09_DATA_TABLE_BYTES);
		HColumnDescriptor cfDes = new HColumnDescriptor(Constants.CF_DETAILS_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
		
		// test the put, read, and delete operations
		HTable table = new HTable(hbaseConfig, Constants.CW09_DATA_TABLE_BYTES);
		byte[] uri1Bytes = Bytes.toBytes("http://www.google.com");
		byte[] uri2Bytes = Bytes.toBytes("http://www.bing.com");
		byte[] content1Bytes = Bytes.toBytes("google search");
		byte[] content2Bytes = Bytes.toBytes("bing search");
		byte[] rowKey1 = Bytes.toBytes("1");
		byte[] rowKey2 = Bytes.toBytes("2");
		Put row1 = new Put(rowKey1);
		row1.add(Constants.CF_DETAILS_BYTES, Constants.QUAL_URI_BYTES, uri1Bytes);
		row1.add(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES, content1Bytes);
		Put row2 = new Put(rowKey2);
		row2.add(Constants.CF_DETAILS_BYTES, Constants.QUAL_URI_BYTES, uri2Bytes);
		row2.add(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES, content2Bytes);
		ArrayList<Row> ops = new ArrayList<Row>(2);
		ops.add(row1);
		ops.add(row2);
		Object[] res = table.batch(ops);
		for (int i=0; i<res.length; i++) {
			if (res[i] == null) {
				res[i] = "null (failure)";
			}
			System.out.println("results for row " + i + " in batch : " + res[i].toString()); 
		}
		table.flushCommits();
		
		ResultScanner rs = table.getScanner(Constants.CF_DETAILS_BYTES);
		System.out.println("scanning table " + Constants.CLUEWEB09_DATA_TABLE_NAME + "...");
		Result r = rs.next();
		while (r != null) {
			System.out.println(Bytes.toString(r.getRow()) + " " + Bytes.toString(r.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_URI_BYTES)) 
								+ " " + Bytes.toString(r.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES)));
			r = rs.next();
		}
		rs.close();
		
		Delete row1Del = new Delete(rowKey1);
		Delete row2Del = new Delete(rowKey2);
		ops.clear();
		ops.add(row1Del);
		ops.add(row2Del);
		res = table.batch(ops);
		for (int i=0; i<res.length; i++) {
			if (res[i] == null) {
				res[i] = "null (failure)";
			}
			System.out.println("results for deleting row " + i + " in batch : " + res[i].toString()); 
		}
		table.flushCommits();
		table.close();
	}
	
	public static void createWordCountTable(HBaseAdmin admin, Configuration hbaseConfig) throws Exception {
		// create the word count table
		if (admin.tableExists(Constants.WORD_COUNT_TABLE_BYTES)) {
			admin.disableTable(Constants.WORD_COUNT_TABLE_BYTES);
			admin.deleteTable(Constants.WORD_COUNT_TABLE_BYTES);
		}
		
		HTableDescriptor tableDes = new HTableDescriptor(Constants.WORD_COUNT_TABLE_BYTES);
		HColumnDescriptor cfDes = new HColumnDescriptor(Constants.CF_FREQUENCIES_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
	}
	
	public static void createCw09IndexTable(HBaseAdmin admin, Configuration hbaseConfig) throws Exception {
		// create the inverted index table
		if (admin.tableExists(Constants.CW09_INDEX_TABLE_BYTES)) {
			admin.disableTable(Constants.CW09_INDEX_TABLE_BYTES);
			admin.deleteTable(Constants.CW09_INDEX_TABLE_BYTES);
		}
		
		HTableDescriptor tableDes = new HTableDescriptor(Constants.CW09_INDEX_TABLE_BYTES);
		HColumnDescriptor cfDes = new HColumnDescriptor(Constants.CF_FREQUENCIES_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);		
	}
	
	public static void createCw09PageRankTable(HBaseAdmin admin, Configuration hbaseConfig) throws Exception {
		// create the page rank table
		if (admin.tableExists(Constants.CW09_PAGERANK_TABLE_BYTES)) {
			admin.disableTable(Constants.CW09_PAGERANK_TABLE_BYTES);
			admin.deleteTable(Constants.CW09_PAGERANK_TABLE_BYTES);
		}
		
		HTableDescriptor tableDes = new HTableDescriptor(Constants.CW09_PAGERANK_TABLE_BYTES);
		HColumnDescriptor cfDes = new HColumnDescriptor(Constants.CF_PAGERANK_BYTES);
		cfDes.setMaxVersions(1);
		cfDes.setCompressionType(Compression.Algorithm.GZ);
		
		tableDes.addFamily(cfDes);
		admin.createTable(tableDes);
	}
	
	public static void createAllTables(HBaseAdmin admin, Configuration hbaseConfig) throws Exception {
		createCw09DataTable(admin, hbaseConfig);
		createCw09IndexTable(admin, hbaseConfig);
		createWordCountTable(admin, hbaseConfig);
		createCw09PageRankTable(admin, hbaseConfig);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// You need a configuration object to tell the client where to connect.
		// But don't worry, the defaults are pulled from the local config file.
		Configuration hbaseConfig = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
		
		if (args.length == 0) {
			createAllTables(admin, hbaseConfig);
		} else {
			String tableName = args[0];
			if (tableName.equals(Constants.CLUEWEB09_DATA_TABLE_NAME)) {
				createCw09DataTable(admin, hbaseConfig);
			} else if (tableName.equals(Constants.CLUEWEB09_INDEX_TABLE_NAME)) {
				createCw09IndexTable(admin, hbaseConfig);
			} else if (tableName.equals(Constants.WORD_COUNT_TABLE_NAME)) {
				createWordCountTable(admin, hbaseConfig);
			} else if (tableName.equals(Constants.CLUEWEB09_PAGERANK_TABLE_NAME)) {
				createCw09PageRankTable(admin, hbaseConfig);
			} else {
				System.out.println("Usage: java iu.pti.hbaseapp.clueweb09.TableCreatorClueWeb09 [<table name>]");
				System.exit(1);
			}
		}	
	}

}
