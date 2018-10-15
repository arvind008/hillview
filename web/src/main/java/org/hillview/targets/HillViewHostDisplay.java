package org.hillview.targets;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.google.gson.Gson;

public class HillViewHostDisplay {
	public String tagsList[];
	ArrayList hosts = new ArrayList() ;
	Map hostsTagsValues = new HashMap() ;
	Map hostsTagsValues1 = new HashMap() ;

	public void getTagsHosts(String tags) throws IOException, ParseException {
		if(tags.contains(","))
    		this.tagsList = tags.split(",");
    	else {
    		this.tagsList = new String[1];
    		this.tagsList[0]=tags;
    	}
		// parsing file  
        Object obj = new JSONParser().parse(new FileReader("/root/arvind/hillview/hostNames.json")); 
          
        // typecasting obj to JSONObject 
        JSONObject jo = (JSONObject) obj; 
        Set keys = jo.keySet();
        Iterator a = keys.iterator();
        
        while(a.hasNext()) {
        	
        	String key = (String)a.next();
            // loop to get the dynamic key
        	JSONArray ja = (JSONArray) jo.get(key);
        	outerloop:
        	for(int i=0;i<ja.size();i++){
        		for(int k=0;k<tagsList.length;k++) {
        			
	                if(ja.get(i).toString().contains(tagsList[k].trim())) {	
	                	hosts.add(key);
	                	break outerloop;
	                }
        		}
             }
        }
	    /*try {
     		 FileWriter writer = new FileWriter("/root/arvind/newHosts",false);
      		 for(int i=0;i<hosts.size();i++) {
        			writer.write(""+hosts.get(i)+":3569");
        	 }	
      		 writer.close();
    		} catch (FileNotFoundException e) {
      			// File not found
      			e.printStackTrace();
    		} catch (IOException e) {
     			 // Error when writing to the file
      			e.printStackTrace();
    		}*/
        try (BufferedWriter writer= new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("/root/arvind/newHosts"), "utf-8"))) {
        		for(int i=0;i<hosts.size();i++) {
        			writer.write(""+hosts.get(i)+":3569");
        			writer.newLine();
        		}
        	    
        }
			
	}
	public Map getTagsValues() throws FileNotFoundException, IOException, ParseException {
		Gson gson = new Gson();

		BufferedReader br = new BufferedReader(
			new FileReader("/root/arvind/hillview/hostNames.json"));

		hostsTagsValues = (Map) gson.fromJson(br, hostsTagsValues.getClass());
		for(int i=0;i<hosts.size();i++){
			if(hostsTagsValues.containsKey(hosts.get(i))) {
				hostsTagsValues1.put(hosts.get(i), hostsTagsValues.get(hosts.get(i)));
			}
		}
		return hostsTagsValues1;
	}
}
