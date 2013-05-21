/*
 * Copyright 2013 David Gildeh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.davidgildeh.hadoop.input.simpledb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;

/**
 * SimpleDB Client DAO with simple helper methods for accessing SimpleDB
 * 
 * @author David Gildeh
 */
public class SimpleDBDAO {
    
    // Configuration Name Constants
    public static final String SIMPLEDB_AWS_ACCESSKEY = "simpledb.aws.accessKey";
    public static final String SIMPLEDB_AWS_SECRETKEY = "simpledb.aws.secretKey";
    public static final String SIMPLEDB_AWS_REGION = "simpledb.aws.region";
    public static final String SIMPLEDB_DOMAIN = "simpledb.domain";
    public static final String SIMPLEDB_WHERE_QUERY = "simpledb.wherequery";
    
    // Maximum Select Limit for SimpleDB
    private static final int MAX_SELECT_LIMIT = 2500;
    
    // Log4J Logger
    private static final Log LOG = LogFactory.getLog(SimpleDBDAO.class);
    
    private final AmazonSimpleDB sdb;
    private final String sdb_domain;
    private String whereQuery;
    
    /**
     * Default Constructor, initialises SimpleDB Client
     * 
     * @param jobConf   Hadoop Job Configuration
     */
    public SimpleDBDAO(JobConf jobConf) {
        
        // Load Configuration
        String awsAccessKey = jobConf.get(SIMPLEDB_AWS_ACCESSKEY);
        String awsSecretKey = jobConf.get(SIMPLEDB_AWS_SECRETKEY);
        // Default to US-EAST Region
        String simpleDBRegion = jobConf.get(SIMPLEDB_AWS_REGION, "sdb.amazonaws.com");
        sdb_domain = jobConf.get(SIMPLEDB_DOMAIN);
        whereQuery = jobConf.get(SIMPLEDB_WHERE_QUERY, null);
  
        // Initialise SimpleDB Client  
        sdb = new AmazonSimpleDBClient(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
        sdb.setEndpoint(simpleDBRegion);
    }
    
    /**
     * Does a paged query in SimpleDB
     *
     * @param query         The Select Query
     * @param nextToken     If there is a paging token to start from, or null if none
     * @return              The SelectResult from the query
     */
    private SelectResult doQuery(String query, String nextToken) {

        SelectResult results = null;
        
        try {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Running Query: " + query);
            }
            
            SelectRequest selectRequest = new SelectRequest(query);

            if (nextToken != null) {
                selectRequest.setNextToken(nextToken);
            }

            results = sdb.select(selectRequest);

        } catch (AmazonServiceException ase) {
            LOG.error("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon SimpleDB, but was rejected with an error response for some reason.");
            LOG.error("Select Query:     " + query);
            LOG.error("Error Message:    " + ase.getMessage());
            LOG.error("HTTP Status Code: " + ase.getStatusCode());
            LOG.error("AWS Error Code:   " + ase.getErrorCode());
            LOG.error("Error Type:       " + ase.getErrorType());
            LOG.error("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            LOG.error("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with SimpleDB, "
                    + "such as not being able to access the network.");
            LOG.error("Error Message: " + ace.getMessage());
        } finally {
            return results;
        }
    }
    
    /**
     * Creates a SimpleDB Query for a Count or Select
     * 
     * @param isCount   True - Count Query, False - Select Query
     * @param limit     If > 0, will apply limit to query, else ignore
     * @return          A Select statement to run on SimpleDB
     */
    private String createQuery(boolean isCount, long limit) {
        
        String query;
        
        if (isCount) {
            query = "SELECT COUNT(*) FROM ";
        } else {
            query = "SELECT * FROM ";
        }
        
        query += sdb_domain;
        
        if (whereQuery != null) {
            query += " WHERE " + whereQuery;
        }
        
        if (limit > 0) {
            query += " LIMIT " + String.valueOf(limit);
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Query: " + query);
        }
        
        return query;
    }
    
    /**
     * Get the current Where Query or null if none set
     * 
     * @return      The Where Query
     */
    public String getWhereQuery() {
        return this.whereQuery;
    }
    
    /**
     * Set the current Where Query or null to clear it
     * 
     * @param whereQuery    Where Query to set
     */
    public void setWhereQuery(String whereQuery) {
        this.whereQuery = whereQuery;
    }
    
    /**
     * Get the count for a Select query
     *
     * @param query	WHERE Clause to select on, null if none
     * @return          Count of results
     */
    public long getCount() {

        String countQuery = createQuery(true, -1);
        long count = 0;

        // Run Count Query, iterate over tokens if multiple tokens returned
        SelectResult results = doQuery(countQuery, null);
        count += getCountValue(results);
            
        while (results.getNextToken() != null) {
            count += getCountValue(results);    
            // Get next results
            results = doQuery(countQuery, results.getNextToken());
        }
        
        // Return 0 by default
        return count;
    }
    
    /**
     * Helper method to get Count Value from Results
     * 
     * @param result    The results of a count() query
     * @return          The count value
     */
    private long getCountValue(SelectResult result) {
        
        for (Item item : result.getItems()) {    
            for (Attribute attribute : item.getAttributes()) {
                if (attribute.getName().equals("Count")) {
                    return Long.parseLong(attribute.getValue());
                }
            }
        }
        
        // Return -1 if no count found
        return -1;
    }
    
    /**
     * Runs a count LIMIT query for LIMIT = offset to get the nextToken from
     * that row onwards
     * 
     * @param offset    Row to get nextToken for
     * @return          The nextToken to get data after the offset row
     */
    public String getSplitToken(long page, long limit) {
        
        String nextToken = null;
                
        // Run Count Query, iterate over tokens if multiple tokens returned
        SelectResult result = null;
        long totalCount = 0;
        
        while (totalCount < (page * limit)) {
            
            String countQuery = createQuery(true, limit);
            
            if (result != null) {
                result = doQuery(countQuery, result.getNextToken());
            } else {
                result = doQuery(countQuery, null);
            }
                        
            nextToken = result.getNextToken();
            totalCount += getCountValue(result);
        }
        
        return nextToken;       
    }
    
    /**
     * Gets the next item from the Domain. Needs to iterate through the results 
     * until we reach the limit for number of items
     * 
     * @param nextToken     The token to get the next Item
     * @param limit         The size of items to fetch
     * @return              Returns the SelectResult with the Item
     */
    public List<Item> getItems(String nextToken, int limit) {
        
        List<Item> items = new ArrayList<Item>();
        
        // Count of total items
        int totalItems = 0;
        String currentToken = nextToken;
        
        while(totalItems < limit) {
            
            SelectResult results = doQuery(createQuery(false, MAX_SELECT_LIMIT), currentToken);
            for (Item item : results.getItems()) {
                
                if (totalItems < limit) {
                    items.add(item);
                    totalItems++;
                }
            }
            currentToken = results.getNextToken();
            // If currentToken is null, means there's no more data
            if (currentToken == null) {
                break;
            }
        }

        return items;
    }
    
    /**
     * Select a list of unique results as Hashmap. The field will specify which 
     * field to return all unique results for, and the HashMap value for that field
     * will contain a count of how many times that field value occurred in the results
     * 
     * @param field     The field name to get unique results for
     * @return          A HashMap of unique results with a count of how many times they
     *                  occur in the result set
     */
    public HashMap<String, Integer> doSelectUniqueQuery(final String field) {
        
        HashMap<String, Integer> uniqueResults = new HashMap<String, Integer>();
        SelectResult results = doQuery(createQuery(false, -1), null);
        int totalCount = 0;
        
        while ((results.getNextToken() != null)) {

            for (Item item : results.getItems()) {

                for (Attribute attribute : item.getAttributes()) {

                    // Check if the result is unique
                    if (attribute.getName().equals(field)) {
                        final String value = attribute.getValue();
                        
                        if (! uniqueResults.containsKey(value)) {
                            
                            uniqueResults.put(value, 1);
                            
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(String.valueOf(++totalCount) + " - " + value);
                            }
                            
                        } else {
                            Integer count = uniqueResults.get(value);
                            uniqueResults.put(value, ++count);
                        }
                    }  
                }
            }
            
            // Get next results
            results = doQuery(createQuery(false, -1), results.getNextToken());
        }
        
        // Return full list of unique results
        return uniqueResults;
    }
    
    /**
     * Get total item count in domain
     * 
     * @return  Total Item Count in domain
     */
    public long getTotalItemCount() {       
        return sdb.domainMetadata(new DomainMetadataRequest(sdb_domain)).getItemCount().longValue();
    }
}
