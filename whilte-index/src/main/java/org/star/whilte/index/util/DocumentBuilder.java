package org.star.whilte.index.util;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.BinaryField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.star.whilte.schema.WhiteSchema;

public class DocumentBuilder {
	
	public static Document toDocument(Map<String,Object> fields, WhiteSchema schema) {	   
	    Document out = new Document();	    
	    for( Entry<String, Object> ea : fields.entrySet() ) {
	      String name = ea.getKey();
    	  Object v=ea.getValue();
	      SchemaField sfield = schema.getFieldOrNull(name);
	      boolean used = false;	 
	      boolean hasField = false;
	      try {
	          if( v == null ) {
	            continue;
	          }
	          String val = null;
	          hasField = true;
	          if (sfield != null && sfield.getType() instanceof BinaryField) {
		            BinaryField binaryField = (BinaryField) sfield.getType();
		            IndexableField f = binaryField.createField(sfield,v);
		            if(f != null){
		              out.add(f);
		            }
		            used = true;
				} else {
		            val = v.toString();	            
		            if (sfield != null) {
		              used = true;
		              addField(out, sfield, val);
		            }
				}      
	      }catch( Exception ex ) {
	    	  ex.printStackTrace();
	        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
	            "ERROR: Error adding field '" + 
	            		name+ "'='" +v+"'", ex );
	      }	      
	      // make sure the field was used somehow...
	      if( !used && hasField ) {
	        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
	            "ERROR: unknown field '" +name + "'");
	      }
	    }
	    return out;
	}
	
	  private static void addField(Document doc, SchemaField field, String val) {
		    org.apache.solr.schema.FieldType ft =  field.getType();
		    if (field.isPolyField()) {
		    	List<IndexableField> farr = field.getType().createFields(field, val);
		      for (IndexableField f : farr) {
		        if (f != null) doc.add(f); // null fields are not added
		      }
		    } else {
		    	List<IndexableField> farr = field.createFields(val);
		    	for (IndexableField f : farr) {
		    			System.out.println("f1:" + f.fieldType().storeTermVectorPositions());
		    			System.out.println("f2:" +f.fieldType().storeTermVectorOffsets());
		    			System.out.println("f3:" +f.fieldType().storeTermVectorPayloads());
		    			System.out.println("f4:" +f.fieldType().storeTermVectors());
	    		
		            if (f != null) doc.add(f); // null fields are not added
		          }
		    }
		  }

}
