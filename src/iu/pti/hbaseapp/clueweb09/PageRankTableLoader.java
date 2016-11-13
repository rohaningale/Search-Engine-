package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PageRankTableLoader {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length < 2) {
			System.out.println("Usage: java iu.pti.hbaseapp.clueweb09.PageRankTableLoader <docId to nodeId file> <nodeId to PageRank file>");
			System.exit(1);
		}
		
		String docIdToNodeIdPath = args[0];
		String pageRankPath = args[1];
		try {
			loadPageRankTable(docIdToNodeIdPath, pageRankPath);
		} catch (Exception e) {
			System.err.println("Error when loading page ranks to HBase table:");
			e.printStackTrace();
		}
	}
	
	public static void loadPageRankTable(String docIdToNodeIdPath, String pageRankPath) throws Exception {
		// get mapping from document ID to node ID		
		HashMap<byte[], Integer> docIdToNodeIdMap = new HashMap<byte[], Integer>();
		BufferedReader brDocNode = new BufferedReader(new FileReader(docIdToNodeIdPath));
		String line = brDocNode.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				// line is like "clueweb09-en0000-01-19826	348154"
				int idx = line.indexOf('\t');
				byte[] docId = DataLoaderClueWeb09.getRowKeyFromTrecId(line.substring(0, idx));
				int nodeId = Integer.valueOf(line.substring(idx + 1));
				docIdToNodeIdMap.put(docId, nodeId);
			}			
			line = brDocNode.readLine();
		}
		brDocNode.close();
		
		// get mapping from node Id to page rank
		BufferedReader brPr = new BufferedReader(new FileReader(pageRankPath));
		// first line is the number of <nodeID, pageRank> pairs
		line = brPr.readLine();
		int count = Integer.valueOf(line.trim());
		float[] pageRanks = new float[count];
		line = brPr.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				int idx = line.indexOf(' ');
				int nodeId = Integer.valueOf(line.substring(0, idx));
				float pageRank = Float.valueOf(line.substring(idx + 1));
				pageRanks[nodeId] = pageRank;
			}
			line = brPr.readLine();
		}
		brPr.close();
		
		//upload page ranks to the page rank table
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable prTable = new HTable(hbaseConfig, Constants.CW09_PAGERANK_TABLE_BYTES);
		count = 0;
		for (Map.Entry<byte[], Integer> e : docIdToNodeIdMap.entrySet()) {
			byte[] docId = e.getKey();
			int nodeId = e.getValue();
			float pr = pageRanks[nodeId];
			
			Put put = new Put(docId);
			put.add(Constants.CF_PAGERANK_BYTES, Bytes.toBytes(pr), null);
			prTable.put(put);
			
			count++;
			if (count % 1000 == 0) {
				System.out.println("Loaded " + count + " page ranks. Last record: " + Bytes.toString(docId) + ", " + pr);
			}
		}
		System.out.println("Loaded " + count + " page ranks in total.");
		prTable.close();		
	}

}
