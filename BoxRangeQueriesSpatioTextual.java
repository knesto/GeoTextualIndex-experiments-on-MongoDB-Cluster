package gr.unipi.geotextualindexing.sfc;

import java.text.ParseException;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;


public class BoxRangeQueriesSpatioTextual  {
	
	protected MongoCollection <Document> coll;
	
	public BoxRangeQueriesSpatioTextual (MongoCollection <Document> coll) {
        this.coll = coll;      
    }
	
    public  void executeQuery(MongoCollection<Document> mongoCollection, List<Bson> list){
        long t1 = System.currentTimeMillis();
        int count=0;
        MongoCursor<Document> cursor = mongoCollection.aggregate(list).iterator();
        //System.out.println(list);
        try {
            while (cursor.hasNext()) {
            	cursor.next().toJson();
                //System.out.println(cursor.next().toJson());
                count++;
                //if (count>101) {cursor.close();break;}
            }
        } finally {
            cursor.close();
        }
        System.out.println(count);
        System.out.println("Calculation Time: " + (System.currentTimeMillis() - t1));
    }
    
    public  String getJsonQuery(double lon1,double lon2, double minLon, double maxLon, double lat1,double lat2, double minLat, double maxLat, String keywords[],long max,SmallHilbertCurve hc)throws ParseException {

        StringBuilder sbkewords = new StringBuilder();
        StringBuilder sbreg= new StringBuilder();
        StringBuilder sbreg1= new StringBuilder();
        StringBuilder sbreg2= new StringBuilder();
        //System.out.print(" ranges found: "+rangesList.size()  + " ");
        //retrieve hilbert values and concatenation of keywords 
        sbkewords.append("\""+keywords[0]+" "+keywords[1]+" "+keywords[2]+"\"");
        //sbkewords.append("\""+keywords[1]+"\"");
        sbreg.append("\""+"^"+keywords[0]+"$"+"\"");
        sbreg1.append("\""+"^"+keywords[1]+"$"+"\"");
        sbreg2.append("\""+"^"+keywords[2]+"$"+"\"");
  
        //System.out.println(sbreg2.toString());
        //sbkewords.deleteCharAt(sbkewords.length()-1);
        //sbkewords.deleteCharAt(sbkewords.length()-1);
        
        
        //return "{$match:{\"location.coordinates\": {$geoWithin:  { $box:  [ [ "+lon1+","+lat1+" ], [ "+lon2+", "+lat2+"  ] ]}} }}";
        //return "{$match:{$text:{$search:"+sbkewords.toString()+"}}},{$project: {_id: 1}}";
        return "{$match:{ $and: [{$text:{$search:"+sbkewords.toString()+"}},{ $or:[ {Text:{$elemMatch: {$regex:"+sbreg.toString()+" }}},{Text:{$elemMatch: {$regex:"+sbreg1.toString()+" }}},{Text:{$elemMatch: {$regex:"+sbreg2.toString()+" }}}] }]}}";

        //return "{$match:{ $and: [{\"location.coordinates\": {$geoWithin:  { $box:  [ [ "+lon1+","+lat1+" ], [ "+lon2+", "+lat2+"  ] ]}}},{$text:{$search:\"Cambodian Lebanese Persian\" }}]}}";
        
        //return "{$match:{ $and: [{\"location.coordinates\": {$geoWithin:  { $box:  [ [ "+lon1+","+lat1+" ], [ "+lon2+", "+lat2+"  ] ]}}},{$text:{$search:"+sbkewords.toString()+"}}   ,{ $or:[ {Text:{$elemMatch: {$regex:"+sbreg.toString()+" }}},{Text:{$elemMatch: {$regex:"+sbreg1.toString()+" }}},{Text:{$elemMatch: {$regex:"+sbreg2.toString()+" }}}] }  ]}}";
     
        
        
  		
    }
    //get number of documents
    protected  String getDoucumentsCount(){
        return "{ $group: { _id: null , count: {$sum: 1} } }";
    }
    protected  String getDoucumentsGroupidfirst(){
        return "{ $group: { _id: \"$groupid\", temp: {$first: 1} } }";
    }
    // get number of groupids
    protected  String getDoucumentsCountWithGroupid(){
        return "{ $group: { _id: \"$_id.groupid\", count: {$sum: \"$temp\"} } }";  
    }

}
