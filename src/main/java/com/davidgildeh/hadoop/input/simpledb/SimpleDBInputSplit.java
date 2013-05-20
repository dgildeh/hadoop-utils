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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;

/**
 * InputSplit represents the slice of data to be processed by a Record Reader which in
 * turn sends each row in the slice to the Mappers.
 * 
 * Typically, it presents a byte-oriented view on the input and is the responsibility of 
 * RecordReader of the job to process this and present a record-oriented view.
 * 
 * The SimpleDBInputSplit holds the start/end/splitToken for the range of rows so the
 * RecordReader can load the range and create key/value records (Tuples) for each row
 * 
 * @author David Gildeh
 */
public class SimpleDBInputSplit implements InputSplit {

    // Log4J Logger
    private static final Log LOG = LogFactory.getLog(SimpleDBInputSplit.class);
    
    // Start Row
    private long startRow;
    
    // End Row
    private long endRow;
    
    // Next Token
    private String splitToken = null;

    /**
     * Default Constructor for when loading from file using ReadField Method
     */
    public SimpleDBInputSplit() {}
    
    /**
     * Default Constructor for SimpleDBInputSplit. Contains date
     * range to filter SimpleDB query on
     * 
     * @param startRow      Row number of first item in split
     * @param endRow        Row number of last item in Split
     * @param splitToken    Next Token to start Select from
     *                      If NULL, will start from beginning of table
     */
    public SimpleDBInputSplit(long startRow, long endRow, String splitToken) {
        this.startRow = startRow;
        this.endRow = endRow;
        this.splitToken = splitToken;
    }
    
    /**
     * This is supposed to return the total number of Bytes in the data of the InputSplit. 
     * However as this our data split is an ArrayList, we will just return the number of 
     * items in the data as Bytes is complicated to calculate
     * 
     * @return                  The number of rows in the input split.
     */
    public long getLength() {
        return (endRow - startRow);
    }

    /**
     * Get the list of dates where the input split is located.
     * 
     * @return                  list of dates in dd/MM/yy format where data of 
     *                            the InputSplit is located as an array of Strings.
     * @throws IOException 
     */
    public String[] getLocations() throws IOException {
  
        return new String[] {};
    }
    
    /**
     * Return the split token to get from SimpleDB
     * 
     * @return  Next Token from SimpleDB
     */
    public String getSplitToken() {
        return this.splitToken;
    }
    
    /**
     * Get the start row of split
     * 
     * @return  The explicit split size
     */
    public long getStartRow() {
        return this.startRow;
    }
    
    /**
     * Get the end row of split
     * 
     * @return  The explicit split size
     */
    public long getEndRow() {
        return this.endRow;
    }
    
    /**
     * Serialises the Split Object so it can be persisted to disk
     * 
     * @param output            The output stream to write to
     * @throws IOException 
     */
    public void write(DataOutput output) throws IOException {
         
        output.writeLong(startRow);
        output.writeLong(endRow);
        if (splitToken == null) {
            output.writeUTF("NULL");
        } else {
            output.writeUTF(splitToken);
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Writing SimpleDBInputSplit: " + this.toString());
        }
    }

    /**
     * Read the Split from the serialised Split file to initialise InputSplit for
     * processing
     * 
     * @param input         The input stream of file to read from
     * @throws IOException 
     */
    public void readFields(DataInput input) throws IOException {

        startRow = input.readLong();
        endRow = input.readLong();
        splitToken = input.readUTF();
        if (splitToken.equals("NULL")) {
            splitToken = null;
        }
    }
    
    /**
     * Override toString() method
     * 
     * @return      String value of InputSplit 
     */
    @Override
    public String toString() {
        return "startRow=" + String.valueOf(startRow) + ", endRow=" + String.valueOf(endRow)
                + ", splitToken=" + splitToken;
    }
}
