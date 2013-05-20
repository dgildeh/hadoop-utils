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

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

/**
 * Output format to generate a CSV file output of rows from Reducer. Use the Job
 * Configuration object to set "csv.header.fields" property with array of String 
 * header fields to be printed at the top of the file. Output will be generated 
 * as follows:
 * 
 * header1,header2,header3,header4...
 * key,value1,value2,value3,value4....
 * 
 * Use the conf.setStrings(string1,...,stringn) to set header field property
 * 
 * @author David Gildeh
 */
public class CSVOutputFormat <K, V> extends FileOutputFormat {
    
    // Public Configuration Keys
    public static final String CSV_HEADER_FIELDS = "csv.header.fields";
    
    // File extension for created output files
    private static final String FILE_EXTENSION = ".csv";

    /**
     * CSVRecordWriter factory. The RecordWriter takes the output records (Tuples) of
     * the Reducers and writes them to file in CSV format
     * 
     * @param fileSystem
     * @param jobConf
     * @param fileName
     * @param progress
     * @return
     * @throws IOException 
     */
    @Override
    public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String fileName, Progressable progress) throws IOException {
        
        Path file = FileOutputFormat.getTaskOutputPath(jobConf, fileName + FILE_EXTENSION);
        FileSystem fs = file.getFileSystem(jobConf);
        FSDataOutputStream fileOut = fs.create(file, progress);        
        return new CSVRecordWriter<K, V>(fileOut, jobConf.getStrings(CSV_HEADER_FIELDS, (String) null));
    }   
}
