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

package com.davidgildeh.hadoop.output.csv;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

/**
 * Writes Records from Reducer to CSV File. Each Record is written as a new line
 * with comma delimited values. If an array of header fields is passed in constructor
 * then an initial header row will be printed and output will be generated as follows:
 * 
 * header1,header2,header3,header4...
 * key,value1,value2,value3,value4....
 * 
 * @author David Gildeh
 */
public class CSVRecordWriter <K, V> implements RecordWriter<K, V> {
    
    // Output Data Stream
    private DataOutputStream out;
    
    /**
     * Default Constructor. Initialises output stream of output file to write to
     * 
     * @param out           Output Stream to output file
     * @throws IOException 
     */
    public CSVRecordWriter(DataOutputStream out, String[] headers) throws IOException {
      this.out = out;
      
      // If headers isn't null, print initial header row
      if (headers != null) {        
          for(int i = 0; i < headers.length; i++) {
              writeObject(headers[i]);
              
              if (i != (headers.length - 1)) {
                  out.writeChars(",");
              } 
          }
          out.writeBytes("\n");
      }
    }

    /**
     * Write a key/value record (Tuple) as single row in CSV
     * 
     * @param key       The Record Key
     * @param value     The Record Value
     * @throws IOException 
     */
    public synchronized void write(K key, V value) throws IOException {
        
        writeObject(key);
        out.writeChars(",");
        writeObject(value);
        out.writeBytes("\n");
    }
    
    /**
     * Write the object to the byte stream, handling Text as a special case.
     *
     * @param o
     *          the object to print
     * @throws IOException
     *           if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      
        if (o instanceof Text) {
        
            Text to = (Text) o;
            out.write(to.getBytes(), 0, to.getLength());
        
        } else if (o instanceof MapWritable) {
            
            // Loop through values in Map and write to CSV
            MapWritable mo = (MapWritable) o;
            for(Writable key : mo.keySet()) {
                Writable value = mo.get(key);
                value.write(out);
                out.writeChars(",");
            }
            
        }else {  
            out.write(o.toString().getBytes("UTF-8"));
        }
    }

    /**
     * Perform any cleanup of resources when closing the file
     * 
     * @param reporter
     * @throws IOException 
     */
    public synchronized void close(Reporter reporter) throws IOException {
        
        // Close the output stream to free up resources
        out.close();
    }    
}
