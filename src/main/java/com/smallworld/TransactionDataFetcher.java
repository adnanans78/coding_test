package com.smallworld;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionDataFetcher {
	
	File fl;
	FileInputStream fs;
	ObjectMapper objectMapper;
	JsonNode  jn;
	Stream<JsonNode> sjn;
	ObjectMapper  jnobjectMapper;  
	
	 
	public TransactionDataFetcher()
	{
		try {
						
			jnobjectMapper=new ObjectMapper();  
				jn=jnobjectMapper.readTree(new File("transactions.json"));
				sjn=StreamSupport.stream(jn.spliterator(),false);
		} 
		catch (Exception e)
		{
			System.out.println(e); 
		}
	}

    /**
     * Returns the sum of the amounts of all transactions
     */
    public double getTotalTransactionAmount()  throws UnsupportedOperationException{
    		sjn=StreamSupport.stream(jn.spliterator(),false);
    
    	return 	sjn.filter(s->s.isObject())
	 		    .filter(s-> s.get("beneficiaryFullName").isTextual()  && s.get("mtn").isInt()  && s.get("issueSolved").asBoolean()).distinct()		 
	 		    .map(node->node.get("amount").doubleValue()).mapToDouble(db->db).sum();
    }

    /**
     * Returns the sum of the amounts of all transactions sent by the specified client
     */
    public double getTotalTransactionAmountSentBy(String senderFullName) {
    	sjn=StreamSupport.stream(jn.spliterator(),false);
    	return 
    			 sjn.filter(node->node.get("senderFullName").asText().equals("Tom Shelby")  && node.get("mtn").isInt()  && node.get("issueSolved").asBoolean())
					.map(amt-> amt.get("amount").doubleValue())
					.mapToDouble(in-> Math.round(in) ).sum();                   
    	  }

    /**
     * Returns the highest transaction amount
     */
    public double getMaxTransactionAmount()  throws UnsupportedOperationException{
    	sjn=StreamSupport.stream(jn.spliterator(),false);
     return	sjn.filter(s-> s.get("mtn").isInt()  && s.get("issueSolved").asBoolean()).distinct()	 
				.map(node->node.get("amount").doubleValue())
				.max(Comparator.comparingDouble(Double::valueOf)).get();
    		
    }

    /**
     * Counts the number of unique clients that sent or received a transaction
     */
    public long countUniqueClients() throws UnsupportedOperationException {
    	sjn=StreamSupport.stream(jn.spliterator(),false);
    	return (Long)sjn.map(node->node.get("senderFullName")).distinct().count();
    }

    /**
     * Returns whether a client (sender or beneficiary) has at least one transaction with a compliance
     * issue that has not been solved
     */
    public boolean hasOpenComplianceIssues(String clientFullName) {
    	sjn=StreamSupport.stream(jn.spliterator(),false);
    	return	
    		  sjn.filter(send->send.get("senderFullName").asText().equals("Tom Shelby") && !send.get("issueSolved").asBoolean()  )
    		  .map(openissue-> openissue.get("issueSolved") ).count()>0;
    }

    /**
     * Returns all transactions indexed by beneficiary name
     */
    public synchronized Map<String, Object> getTransactionsByBeneficiaryName() throws UnsupportedOperationException {
    	sjn=StreamSupport.stream(jn.spliterator(),false);
    		
    return		sjn.filter(s->s.isObject())
	 		   .filter(s-> s.get("beneficiaryFullName").isTextual()  && s.get("mtn").isInt()  && s.get("issueSolved").asBoolean()).distinct()		 
	 		   .collect(Collectors.groupingBy(s-> s.get("beneficiaryFullName").asText(),Collectors.toList() )  )    
	 		   .entrySet().stream()
	 	       .collect(Collectors.collectingAndThen(Collectors.toMap(Entry::getKey,Entry::getValue),HashMap::new) );
    } 

    /**
     * Returns the identifiers of all open compliance issues
     */
    public Set<Integer> getUnsolvedIssueIds() throws UnsupportedOperationException{
    	sjn=StreamSupport.stream(jn.spliterator(),false);
    		
    	return  sjn.filter(s->s.isObject())
    		    .filter(s-> !s.get("issueSolved").booleanValue())
    		   	.map(s->s.get("mtn").asInt()).collect(Collectors.toSet());
    }

    /**
     * Returns a list of all solved issue messages
     */
    public List<String> getAllSolvedIssueMessages() throws UnsupportedOperationException {
    		sjn=StreamSupport.stream(jn.spliterator(),false);
    	 
    		return sjn.filter( solissue->solissue.get("issueSolved").booleanValue() && !solissue.get("issueMessage").asText().equalsIgnoreCase("null") )
    				.map(msg->msg.get("issueMessage").asText()).collect(Collectors.toList());
    	
    }

    /**
     * Returns the 3 transactions with highest amount sorted by amount descending
     */
    public List<Object> getTop3TransactionsByAmount() throws UnsupportedOperationException{
    			sjn=StreamSupport.stream(jn.spliterator(),false);
    				
    	return sjn.filter(amt->amt.get("amount").isDouble() && amt.get("mtn").isInt()  && amt.get("issueSolved").asBoolean()).distinct()
    			.sorted( (n1,n2)->  Double.compare(n2.findValue("amount").doubleValue(),n1.findValue("amount").doubleValue()))
    			.limit(3).collect(Collectors.toList());
    }
    /**
     * Returns the sender with the most total sent amount 
     */
    public Optional<Object> getTopSender() throws UnsupportedOperationException {
    			sjn=StreamSupport.stream(jn.spliterator(),false);
   
    	return	sjn.filter(obj->obj.isObject() && obj.get("mtn").isInt()  && obj.get("issueSolved").asBoolean()).distinct()
	  		 	 .collect( Collectors.groupingBy( r1-> r1.get("senderFullName"), Collectors.mapping(a-> a.get("amount").doubleValue(), Collectors.summingDouble(s->s)) )) 
	  		 	 .entrySet().stream().sorted(Map.Entry.comparingByValue( Comparator.reverseOrder()))   
	  		     .collect(Collectors.collectingAndThen(Collectors.toList(), Optional::of));
    }
    
 

}
