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

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * The InputFormat determines how to split the input data into InputSplits that
 * are then sent to the RecordReader to load the key/value Tuples that are processed
 * by the Mappers. It also provides a factory method to create new SimpleDBRecordReaders
 * to process the splits into Tuples.
 * 
 * More info can be found here: http://developer.yahoo.com/hadoop/tutorial/module4.html#closer
 * 
 * The SimpleDBInputFormat splits the rows by the LIMIT Size (Max 2500 by default) and pages through
 * the entire domain to load all the data.
 * 
 * The SimpleDBInputFormat splits the SimpleDB domain by simpledb.split.size (default 100,000) rows,
 * creating InputSplits for each range of rows with a startRow, endRow and nextToken to load the range
 * from SimpleDB. It iterates over the total number of rows in the SimpleDB Domain, or a filtered list of
 * rows if a simpledb.wherequery is provided, getting the nextToken for the end of that split so the next
 * split can load its rows from the end of the last split. 
 * 
 * Once the InputSplits are created (note they're created in memory as an array so they only hold range
 * data for the RecordReaders to load across the cluster, and the number of splits needs to be small or
 * you get OutOfMemory errors), the RecordReader factory method is called for each Split, and the RecordReader
 * then loads all the rows in the Split range, and iterates through them to create key/value records (Tuples)
 * that are sent to the Mappers.
 * 
 * To setup this InputFormat, the Job Configuration must have all necessary fields set:
 * 
 * - simpledb.aws.accessKey: Your AWS Account Access Key to access SimpleDB
 * - simpledb.aws.secretKey: Your AWS Account Secret Key to access SimpleDB
 * - simpledb.domain: The SimpleDB Domain to load the data from
 * - simpledb.aws.region: (OPTIONAL) The AWS Region the SimpleDB is in, defaults to US-EAST
 * - simpledb.split.size: (OPTIONAL) LIMIT size to page through rows in SimpleDB. Cannot be
 *                        greater than 100,000. Defaults to 100,000
 * - simpledb.wherequery: (OPTIONAL) Any where/order by query. Do not include LIMIT
 * 
 * @author David Gildeh
 */
public class SimpleDBInputFormat  implements InputFormat<Text, MapWritable> {
    
    // Log4J Logger
    private static final Log LOG = LogFactory.getLog(SimpleDBInputFormat.class);
    
    // Set the split size (number of rows), Max & default = 100,000
    public static final String SIMPLEDB_SPLIT_SIZE = "simpledb.split.size";
    
    // Name of 'time' field in SimpleDB domain to split data with
    private static final int MAX_SPLIT_SIZE = 100000;
    
    /**
     * Main method to generate splits from SimpleDB. Takes a start and end date range
     * and generates split periods to filter SimpleDB based on split period given in 
     * configuration
     * 
     * @param jobConf       The Map Task Job Configuration
     * @param numSplits     Hint to calculate the number of splits
     * @return              The Input Splits
     * @throws IOException 
     */
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        
        // Get the splitsize (number of rows), defaults to 100,000 as much larger seems to 
        // screw up getting accurate rows
        int splitSize = Integer.parseInt(jobConf.get(SimpleDBInputFormat.SIMPLEDB_SPLIT_SIZE, String.valueOf(MAX_SPLIT_SIZE)));
        if (splitSize > MAX_SPLIT_SIZE) {
            splitSize = MAX_SPLIT_SIZE;
        }
        
        // Get total number of rows to calculate number of splits required
        SimpleDBDAO sdb = new SimpleDBDAO(jobConf);
        long totalItems;
        if (sdb.getWhereQuery() != null) {
            totalItems = sdb.getCount();
        } else {
            totalItems = sdb.getTotalItemCount();
        }

        long totalSplits = 1;
        if (splitSize < totalItems) {
            totalSplits = totalItems / splitSize;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Total Rows:" + String.valueOf(totalItems));
            LOG.debug("Total Splits:" + String.valueOf(totalSplits));
        }
        
        // Array to hold splits
        ArrayList<SimpleDBInputSplit> splits = new ArrayList<SimpleDBInputSplit>();
       
        // Create Splits
        for (int i = 0; i  < totalSplits; i++) {
            
            String splitToken = sdb.getSplitToken(i, splitSize);
            long startRow = i * splitSize;
            long endRow = startRow + splitSize;
            if (endRow > totalItems) {
                endRow = totalItems;
            }
            
            SimpleDBInputSplit split = new SimpleDBInputSplit(startRow, endRow, splitToken);
            splits.add(split);
            
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created Split: " + split.toString());
            }
        }
        
        // Return array of splits
        return splits.toArray(new SimpleDBInputSplit[splits.size()]);
    }

    /**
     * Creates a SimpleDBRecordReader to convert the InputSplit into a Key/Value Record that can be sent
     * to the Mappers for processing
     * 
     * @param split         The Input Split to process
     * @param jobConf       The Job Configuration
     * @param reporter      
     * @return              A new Record Reader that processes the Input Split
     * @throws IOException 
     */
    public RecordReader<Text, MapWritable> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {
        
        return new SimpleDBRecordReader((SimpleDBInputSplit)split, jobConf);
    }    
}
