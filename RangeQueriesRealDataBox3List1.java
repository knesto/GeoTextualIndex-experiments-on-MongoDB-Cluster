package gr.unipi.geotextualindexing.sfc.RealData;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.bson.Document;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.junit.*;
import static org.junit.Assert.*;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import gr.unipi.geotextualindexing.sfc.BoxRangeQueries;



public class RangeQueriesRealDataBox3List1 {
	
	 @Test
	 public void test1() throws Exception { 
		 // Mongodb initialization parameters.
	        int port_no = 27017;
	        String host_name = "localhost", db_name = "mongodb", db_coll_name = "real_data_collection";
	        //hilbertcollection
	        // Mongodb connection string.
	        String client_url = "mongodb://" + host_name + ":" + port_no + "/" + db_name;
	        MongoClientURI uri = new MongoClientURI(client_url);
	 
	        // Connecting to the mongodb server using the given client uri.
	        MongoClient mongo_client = new MongoClient(uri);
	 
	        // Fetching the database from the mongodb.
	        MongoDatabase db = mongo_client.getDatabase(db_name);

	        // Fetching the collection from the mongodb.
	        MongoCollection<Document> coll = db.getCollection(db_coll_name);
	        //1047
	        int bits = 5;
	        int dimensions = 2;
	        SmallHilbertCurve h = HilbertCurve.small().bits(bits).dimensions(dimensions);
	        long max = 1L << bits;
	        double lon1= -75.00390625;
	        double lat1= 39.66827914916014;
	        double lon2= -70.400390625;
	        double lat2= 39.774769485295465;
	        double maxlon=180d;
	        double minlon=-180d;
	        double maxlat=90d;
	        double minlat=-90d;
	        String[] keywords = {"steak","egg","plate"};
	 
	       BoxRangeQueries rq= new BoxRangeQueries(coll);
	       //rq.executeQuery(coll, Arrays.asList(Document.parse(rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h))));
	       //rq.executeQuery(coll, Arrays.asList(Document.parse(rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)),Document.parse(rq.getDoucumentsCount())));    
	       //rq.executeQuery(coll, Arrays.asList(Document.parse(rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)),Document.parse(rq.getDoucumentsGroupidfirst()),Document.parse(rq.getDoucumentsCountWithGroupid())));    
		 
	       
	       //executionStats
	       Gson gson = new Gson();
	       //System.out.println(gson.toJson(db.runCommand(Document.parse("{ \"explain\": { \"aggregate\": \""+db_coll_name+"\", pipeline:["+rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)+"], \"cursor\":{} }}"))));
	       //System.out.println(gson.toJson(db.runCommand(Document.parse("{ \"explain\": { \"aggregate\": \""+db_coll_name+"\", pipeline:["+rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)+","+rq.getDoucumentsGroupidfirst()+","+rq.getDoucumentsCountWithGroupid()+"], \"cursor\":{} }}"))));
	   	   
	       String fileContent = gson.toJson(db.runCommand(Document.parse("{ \"explain\": { \"aggregate\": \""+db_coll_name+"\", pipeline:["+rq.getJsonQuery(lon1,lon2, minlon, maxlon,  lat1,lat2, minlat, maxlat, keywords , max, h)+"], \"cursor\":{} }}")));
	       FileOutputStream outputStream = new FileOutputStream("results_real_q7.txt");
	       byte[] strToBytes = fileContent.getBytes();
	       outputStream.write(strToBytes);
	     
	       outputStream.close();
	       
	        mongo_client.close();
	 }
	 
	    public static void main(String[] args) throws Exception {

	    	org.junit.runner.JUnitCore.main("gr.unipi.geotextualindexing.sfc.RealData.RangeQueriesRealDataBox3List1");            
	 }

}
