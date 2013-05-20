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

import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * RecordReader takes a split and produces individual key/value pair records (Tuples)
 * that are sent to the Mappers for processing
 * 
 * The SimpleDBRecordReader loads the InputSplit range from SimpleDB and then iterates
 * through each item (row) from to creates a key/value pair record (Tuple) for the Mappers
 * 
 * @author David Gildeh
 */
public class SimpleDBRecordReader implements RecordReader<Text, MapWritable> {
    
    // Log4J Logger
    private static final Log LOG = LogFactory.getLog(SimpleDBRecordReader.class);
    
    // InputSplit
    private final SimpleDBInputSplit split;
    
    // SimpleDBDAO Client
    private final SimpleDBDAO sdb;
    
    // Position Cursor to data
    private int cursor = 0;
    
    // Current token of item to get
    private List<Item> items;

    /**
     * Default Constructor creates new SimpleDBRecordReader for a SimpleDBInputSplit
     * 
     * @param split     The Input Split
     * @param jobConf   Hadoop Job Configuration
     */
    public SimpleDBRecordReader(SimpleDBInputSplit split, JobConf jobConf) {
        this.split = split;
        this.sdb = new SimpleDBDAO(jobConf);  
        this.items = sdb.getItems(split.getSplitToken(), (int) split.getLength());
    }
    
    /**
     * Get next Key/Value Record (Tuple) from the Split
     * 
     * @param key           The key to set
     * @param value         The HashMap value to set
     * @return              True - next Item available, False - No more items available
     * @throws IOException 
     */
    public boolean next(Text key, MapWritable value) throws IOException {
        
        // Get next item off the ArrayList unless we're at the end
        if (cursor < split.getLength()) {
            
            Item item = items.get(cursor++);
            
            key.set(item.getName());
            for (Attribute attribute : item.getAttributes()) {
                value.put(new Text(attribute.getName()), new Text(attribute.getValue()));
            }
               
            System.out.println("Sending next record to Mappers: " + key.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending next record to Mappers: " + key.toString());                
            }
            
            return true;
        } else {
            return false;
        }
    }

    /**
     * Creates a new blank key for the next() method to set
     * 
     * @return  A new key
     */
    public Text createKey() {
        return new Text();
    }

    /**
     * Creates a new blank value for the next() method to set
     * 
     * @return A new value 
     */
    public MapWritable createValue() {
        return new MapWritable();
    }

    /**
     * Get current position in Split data array
     * 
     * @return                  Current cursor position
     * @throws IOException 
     */
    public long getPos() throws IOException {
        return cursor;
    }

    /**
     * Called when RecordReader is closed. Used for cleanup code. In our cases
     * there is nothing to clean up.
     * 
     * @throws IOException 
     */
    public void close() throws IOException {
        // Do Nothing
    }

    /**
     * Get percentage (float) progress of how far RecordReader has iterated
     * over total rows in Split
     * 
     * @return                  Percentage progress as float
     * @throws IOException 
     */
    public float getProgress() throws IOException {
        
        if(this.cursor  == this.split.getLength())
        {
            return 0.0f;
        }
        else
        {
            return Math.min(1.0f, this.cursor / (float)(this.split.getLength()));
        }
    }
}
