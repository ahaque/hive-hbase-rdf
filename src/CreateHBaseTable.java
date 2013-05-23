import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateHBaseTable {

	/**
	 * @param args
	 * @throws IOException 
	 */

	public static byte[][] splitKeys = {
		// 1000M, 16 nodes
		//Bytes.toBytes("bsbm_inst_dataFromProducer39975_Product2022415"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite113_Review1136971"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite1701_Review17039926"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite2210_Review22153091"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite288_Review2834397"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite776_Review7711326"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor11240_Offer22447529"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor13743_Offer27484483"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor16256_Offer32544815"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor18917_Offer37889410"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor217_Offer424569"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor24583_Offer49251275"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor27175_Offer54460511"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor4349_Offer8596056"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor7159_Offer14180063")
		
		// 10M, 16 nodes
		//Bytes.toBytes("bsbm_inst_dataFromProducer366_Product17820"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite1_Review8670"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite15_Review143661"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite2_Review10971"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite24_Reviewer12512"),
		//Bytes.toBytes("bsbm_inst_dataFromRatingSite4_Review38014"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor101_Offer205115"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor131_Offer262360"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor160_Offer318496"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor191_Offer378686"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor222_Offer435129"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor25_Offer45595"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor28_Offer52132"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor41_Offer83450"),
		//Bytes.toBytes("bsbm_inst_dataFromVendor7_Offer12222")
		
		/* 100M, 16 nodes */ /*
		Bytes.toBytes("bsbm_inst_dataFromProducer3998_Product202204"),
		Bytes.toBytes("bsbm_inst_dataFromRatingSite116_Review1164235"),
		Bytes.toBytes("bsbm_inst_dataFromRatingSite167_Review1657072"),
		Bytes.toBytes("bsbm_inst_dataFromRatingSite219_Review2155416"),
		Bytes.toBytes("bsbm_inst_dataFromRatingSite270_Review2655159"),
		Bytes.toBytes("bsbm_inst_dataFromRatingSite61_Review602184"),
		Bytes.toBytes("bsbm_inst_dataFromVendor1062_Offer2088533"),
		Bytes.toBytes("bsbm_inst_dataFromVendor1340_Offer2643515"),
		Bytes.toBytes("bsbm_inst_dataFromVendor162_Offer323466"),
		Bytes.toBytes("bsbm_inst_dataFromVendor1907_Offer3761715"),
		Bytes.toBytes("bsbm_inst_dataFromVendor2191_Offer4320354"),
		Bytes.toBytes("bsbm_inst_dataFromVendor2471_Offer4877685"),
		Bytes.toBytes("bsbm_inst_dataFromVendor2755_Offer5435784"),
		Bytes.toBytes("bsbm_inst_dataFromVendor435_Offer856797"),
		Bytes.toBytes("bsbm_inst_dataFromVendor715_Offer1409138")
		*/
		
		// DBpedia 100M
		//Bytes.toBytes("res_Anonymous_Rex"),
		//Bytes.toBytes("res_Bo_Pho"),
		//Bytes.toBytes("res_Chris_Heintz"),
		//Bytes.toBytes("res_Dirofilaria"),
		//Bytes.toBytes("res_Foolad_Shahr_Stadium"),
		//Bytes.toBytes("res_Happy_Campers__282001_film_29"),
		//Bytes.toBytes("res_Jeff_Paulk"),
		//Bytes.toBytes("res_Lacmalac_2C_New_South_Wales"),
		//Bytes.toBytes("res_Marcus_Laurinaitis"),
		//Bytes.toBytes("res_Neola_2C_Utah"),
		//Bytes.toBytes("res_Peter_Milton__28Australian_politician_29"),
		//Bytes.toBytes("res_Robert_Sikoryak"),
		//Bytes.toBytes("res_Sk_C3_B6vde_AIK"),
		//Bytes.toBytes("res_The_Futureheads__28album_29"),
		//Bytes.toBytes("res_Vasil_Velev"),
			};
	
	public static void main(String[] args) throws IOException {
		String USAGE = "  Arguments: <table name> <column family> <hbase master>";
		if (args== null) {
			System.out.println(USAGE);
			System.exit(-1);
		}
		if (args.length !=3) {
			System.out.println(USAGE);
			System.exit(-1);
		}
		
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor tableDesc = new HTableDescriptor(args[0]);
		HColumnDescriptor colDesc = new HColumnDescriptor(args[1].getBytes());

		config.clear();

		config.set("hbase.zookeeper.quorum", args[2]);
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.master", args[2] + ":60000");

		// BloomType.NONE or BloomType.ROW or BloomType.ROWCOL
		colDesc.setBloomFilterType(BloomType.ROWCOL);
		colDesc.setMaxVersions(100);
		colDesc.setCacheBloomsOnWrite(true);
		tableDesc.addFamily(colDesc);
		tableDesc.setName(args[0].getBytes());
		try {
			//admin.createTable(tableDesc);
			tableDesc.addFamily(colDesc);
	        admin.createTable(tableDesc, splitKeys);
		} catch (org.apache.hadoop.hbase.TableExistsException e) {
			System.err.println("HBase Table: " + args[0]
					+ " already exists!\nExiting...");
			System.exit(-1);
		}
		System.out.println("\n\nSuccessfully created table: " + args[0] +"\n\n");
		admin.close();
	}

}
