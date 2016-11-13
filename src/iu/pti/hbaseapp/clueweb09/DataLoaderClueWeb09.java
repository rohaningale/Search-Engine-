package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;

import java.io.DataInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DataLoaderClueWeb09 {	
	static class DlMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		@Override
		public void map(LongWritable key, Text inputWarcFilePath, Context context) throws IOException {
			// Each map() is a single line, where the key is the line number
			// Each line is a local file system path to a .warc.gz file
			GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(inputWarcFilePath.toString()));
			DataInputStream inStream;
		    inStream = new DataInputStream(gzInputStream);
		    HTMLTextParser txtExtractor = new HTMLTextParser();
			
		    WarcRecord thisWarcRecord;
			while ((thisWarcRecord = WarcRecord.readNextWarcRecord(inStream)) != null) {
				// see if it's a response record
				if (thisWarcRecord.getHeaderRecordType().equals("response")) {
					// it is - create a WarcHTML record
					WarcHTMLResponseRecord htmlRecord = new WarcHTMLResponseRecord(thisWarcRecord);
					// get our TREC ID and target URI
					String thisTRECID = htmlRecord.getTargetTrecID();
					String thisTargetURI = htmlRecord.getTargetURI();
					String html = thisWarcRecord.getContentUTF8();
					int idx = html.indexOf("Content-Length");
					if (idx >= 0) {
						idx = html.indexOf('<', idx);
						if (idx < 0) {
							html = "";
						} else {
							html = html.substring(idx); 
						}
					}
					String content = txtExtractor.htmltoText(html);
					if (content == null) {
						continue;
					}
					
					byte[] rowKey = getRowKeyFromTrecId(thisTRECID);
					byte[] uriBytes = Bytes.toBytes(thisTargetURI);
					byte[] contentBytes = Bytes.toBytes(content);
					Put put = new Put(rowKey);
					put.add(Constants.CF_DETAILS_BYTES, Constants.QUAL_URI_BYTES, uriBytes);
					put.add(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES, contentBytes);
					
					try {
						context.write(new ImmutableBytesWritable(rowKey), put);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args)
			throws IOException {
		Path inputPath = new Path(args[0]);
		Job job = new Job(conf, "ClueWeb09 data loader");
		job.setJarByClass(DlMapper.class);
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(DlMapper.class);
		// No reducers. Just write straight to table. Call initTableReducerJob
		// because it sets up the TableOutputFormat.
		TableMapReduceUtil.initTableReducerJob(Constants.CLUEWEB09_DATA_TABLE_NAME, null, job);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		return job;
	}

	/**
	 * Main entry point.
	 * 
	 * @param args
	 *            The command line parameters.
	 * @throws Exception
	 *             When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Wrong number of arguments: " + otherArgs.length);
			System.err.println("Usage: DataLoaderClueWeb09 <input directory>");
			System.exit(-1);
		}
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
	
	/**
	 * generate a table row key from a trecId
	 * @param TrecId
	 * @return
	 */
	public static byte[] getRowKeyFromTrecId(String trecId) {
		// a treckId is like "clueweb09-en0040-54-00000"
		StringBuffer digitsSb = new StringBuffer();
		int idx = trecId.indexOf("en");
		if (idx < 0) {
			idx = 0;
		}
		while (idx < trecId.length()) {
			char c = trecId.charAt(idx);
			if (Character.isDigit(c)) {
				digitsSb.append(c);
			}
			idx++;
		}
		return Bytes.toBytes(digitsSb.toString());
	}
}
