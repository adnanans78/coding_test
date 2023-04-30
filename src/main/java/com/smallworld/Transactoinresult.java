package com.smallworld;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Transactoinresult {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

		TransactionDataFetcher tc=new TransactionDataFetcher();  
				
		System.out.println( tc.getTotalTransactionAmount()) ;
	
		System.out.println( tc.getTotalTransactionAmountSentBy("Tom Shelby")) ;
		
		System.out.println(tc.getMaxTransactionAmount() );
		
		System.out.println(tc.countUniqueClients() );
		
		System.out.println(tc.hasOpenComplianceIssues("Tom Shelby"));
		
		System.out.println(tc.getTransactionsByBeneficiaryName() );
		
		System.out.println(tc.getUnsolvedIssueIds() );
		
		System.out.println(tc.getAllSolvedIssueMessages() );
		
		System.out.println(tc.getTop3TransactionsByAmount() );
		
		System.out.println(tc.getTopSender());
		
		
	}

	
}
