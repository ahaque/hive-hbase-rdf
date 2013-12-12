import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRLoad {
	
	public static class Map extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tuple = value.toString().split(" ");
			String COLUMN_FAMILY = "p";

			// This code was adapted from a non-parallel java loader
			// Get the subject, predicate, object
			boolean objectIsAdjusted = false;
			boolean itemIsAdjusted = false;
			String predicate_original = null;
			String predicate_adjusted = null;
			String object_original = null;
			String object_adjusted = null;
			String object_value = null;
			StringBuilder build = new StringBuilder();
			int len = 0;
			int startIndex, endIndex = 0;

			byte[] subject = null, predicate = null, object = null;

			for (int j = 0; j < abbrvName.length; j++) {
				if (tuple[0].contains(fullName[j])) {
					subject = Bytes.toBytes(removeBannedChars(tuple[0].replace(
							fullName[j], abbrvName[j])));
					itemIsAdjusted = true;
					break;
				}
			}
			if (!itemIsAdjusted) {
				subject = Bytes.toBytes(tuple[0]);
			}
			itemIsAdjusted = false;

			/*
			 * Since HBase does not allow "#"" or ":" in the column families, we
			 * must remove these predicate_original is the predicate directly
			 * from the dataset.nt file predicate_adjusted is the predicate with
			 * ":"" and "#" replaced with "_"
			 */
			predicate_original = tuple[1];

			for (int j = 0; j < abbrvName.length; j++) {
				if (predicate_original.contains(fullName[j])) {
					predicate_adjusted = removeBannedChars(predicate_original
							.replace(fullName[j], abbrvName[j]));
					itemIsAdjusted = true;
					break;
				}
			}
			if (itemIsAdjusted) {
				predicate = Bytes
						.toBytes(removeBannedChars(predicate_adjusted));
			} else {
				predicate = Bytes
						.toBytes(removeBannedChars(predicate_original));
			}

			// Handles tuples with descriptions (have multiple spaces in the
			// string) as a single object
			len = tuple.length;
			if (len < 4) {
				object_original = tuple[2];
			} else {
				for (int i = 2; i < len - 1; i++) {
					build.append(tuple[i] + " ");
				}
				object_original = build.toString().substring(0,
						build.length() - 1);
			}

			if (object_original.contains("\"^^")) {
				startIndex = object_original.indexOf("\"", 0);
				endIndex = object_original.indexOf("\"", startIndex + 1);
				object_value = object_original.substring(startIndex + 1,
						endIndex);
				// Make sure we handle dates correctly
				if (object_value
						.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")) {
					object = Bytes.toBytes(object_value.substring(0, 10) + " "
							+ object_value.substring(11));
				} else {
					// Hive complains if there are any symbols in column names
					object = Bytes.toBytes(object_value);
				}
			} else {
				for (int j = 0; j < abbrvName.length; j++) {
					if (object_original.contains(fullName[j])) {
						object_adjusted = removeBannedChars(object_original
								.replace(fullName[j], abbrvName[j]));
						objectIsAdjusted = true;
						break;
					}
				}
				if (objectIsAdjusted) {
					object = Bytes.toBytes(object_adjusted);
				} else {
					object = Bytes.toBytes(object_original);
				}
			}

			Put put = new Put(subject);
			put.add(Bytes.toBytes(COLUMN_FAMILY), predicate, object);

			ImmutableBytesWritable ibKey = new ImmutableBytesWritable(subject);
			context.write(ibKey, put);

		}
	}

	static final String BANNED_CHARACTERS = "[-\\<>:#//@%&().]";

	public static String removeBannedChars(String s) {
		return s.replace(">", "").replaceAll(BANNED_CHARACTERS, "_");
	}


	static String[] fullName = {
			"" + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#",
			"<http://www.w3.org/2000/01/rdf-schema#",
			"<http://xmlns.com/foaf/0.1/", "<http://purl.org/dc/elements/1.1/",
			"<http://www.w3.org/2001/XMLSchema#",
			"<http://purl.org/stuff/rev#",
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/",
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/" };

	static String[] abbrvName = { "rdf_", "rdfs_", "foaf_", "dc_", "xsd_",
			"rev_", "bsbm_", "bsbm-inst_" };
		
//	static String[] fullName = {
//			"<http://dbpedia.org/resource/",
//			"<http://dbpedia.org/property/",
//			"<http://dbpedia.org/ontology/",
//			"<http://dbpedia.org/class/",
//			"<http://www.w3.org/",
//			"<http://xmlns.com/foaf/0.1/"
//	};
//
//	static String[] abbrvName = {"res_","prop_","ont_","class_","w3_","foaf_"};

	public static Job createMRJob(String[] args) throws Exception {
		// Create the HBase table
		Configuration hconfig = HBaseConfiguration.create();
		hconfig.clear();
		hconfig.set("hbase.zookeeper.quorum", args[1]);
		hconfig.set("hbase.zookeeper.property.clientPort", "2181");
		hconfig.set("hbase.master", args[1] + ":60000");
		
		HTable hTable = new HTable(hconfig, args[0]);
		/*
		HBaseAdmin admin = new HBaseAdmin(hconfig);
		cf.setCacheBloomsOnWrite(true);
		cf.setMaxVersions(100);
		cf.setBloomFilterType(BloomType.ROWCOL);
		hdesc.addFamily(cf);
		hdesc.setName(args[0].getBytes());
		try {
			//admin.createTable(hdesc, splitKeys);
			admin.createTable(hdesc);
		} catch (org.apache.hadoop.hbase.TableExistsException e) {
			System.err.println("HBase Table: " + args[0]
					+ " already exists!\nExiting...");
			System.exit(-1);
		}
		System.out.println("Successfully created table: " + args[0]);
		admin.close();
		*/

		// Arguments: <hbase table name> <zk quorum> <input path> <output path>
		Configuration conf = new Configuration();
		conf.set("hbase.mapred.outputtable", args[0]);
		conf.set("hbase.hstore.blockingStoreFiles", "25");
		conf.set("hbase.hregion.memstore.block.multiplier", "8");
		conf.set("hbase.regionserver.handler.count", "30");
		conf.set("hbase.regions.percheckin", "30");
		conf.set("hbase.regionserver.globalMemcache.upperLimit", "0.3");
		conf.set("hbase.regionserver.globalMemcache.lowerLimit", "0.15");
		conf.set("hbase.hregion.max.filesize", "10737418240");

		Job job = new Job(conf);
		job.setJobName("nt_BulkLoad");
		job.setJarByClass(MRLoad.class);
		job.setMapperClass(Map.class);

		TextInputFormat.setInputPaths(job, new Path(args[2]));
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(HFileOutputFormat.class);

		job.setReducerClass(PutSortReducer.class);
		// job.setNumReduceTasks(0);

		// FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		HFileOutputFormat.configureIncrementalLoad(job, hTable);

		return job;
	}

	public static void main(String[] args) throws Exception {

		String USAGE_MSG = "  Arguments: <hbase table name> <zk quorum> <input path> <output path>";

		if (args == null || args.length != 4) {
			System.out.println(USAGE_MSG);
			System.exit(0);
		}

		Job job = createMRJob(args);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}