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

package com.davidgildeh.hadoop.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.PropertyConfigurator;

/**
 * Utility class that provides simple methods for accessing files on the Hadoop
 * File System (HDFS). If the Hadoop environment is setup and running, it will be
 * able to access files on HDFS, if not it will only access files on the local file
 * system. In order to access files on S3 (while using Elastic MapReduce) you should
 * pass in a s3n://... path to load the files on S3.
 * 
 * The main methods provided by this class:
 * 
 * - loadJobConf: This enables you to load a key/value properties file from HDFS that
 *                contains all your job configuration. It will automatically load the
 *                values into the JobConf object. This enables you to provide different
 *                settings for each of your Jobs across different environments. An example
 *                properties file can be found under src/main/resources
 * - mergeFiles:  This method will take all the output files under a specified directory
 *                on HDFS, and merge them into a single file in another output directory.
 *                This makes it easy for the output to be exported from S3 once the job
 *                is complete as opposed to having to manually merge the output files
 *                after the job is complete.
 * 
 * @author David Gildeh
 */
public class FileUtils {
    
    // Log4J Logger
    private static final Log LOG = LogFactory.getLog(FileUtils.class);
    
    /**
     * Loads a Properties object with key/value properties from file on HDFS
     * 
     * @param path          The path to the properties file
     * @return              The initialised properties loaded from file
     * @throws IOException 
     */
    public static Properties loadPropertiesFile(String path) throws IOException {
        
        // Properties Object to load properties file
        Properties propFile = new Properties();
        
        Path fsPath = new Path(path);
        FileSystem fileSystem = getFileSystem(fsPath);
        checkFileExists(fileSystem, fsPath);
        FSDataInputStream file = fileSystem.open(fsPath);
        propFile.load(file);
        file.close();
        fileSystem.close();
        return propFile;
    }
    
    /**
     * Given a path to a valid key/value properties file on HDFS, all values will be
     * loaded into the provided JobConf to provide configuration properties for the 
     * Hadoop Job
     * 
     * @param jobConf           The JobConf to load property values into
     * @param path              The path to the properties file
     * @return                  The JobConf with loaded properties values added     
     */
    public static JobConf loadJobConf(JobConf jobConf, String path) throws IOException {
        
        Properties propFile = loadPropertiesFile(path);
 
        // Loop through all properties in properties file
        for (Object keyObject : propFile.keySet()) {
            String key = (String) keyObject;
            String value = propFile.getProperty(key);
            jobConf.set(key, value);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Loaded Configuration Property " + key + ": " + value);
            }
        }
        
        return jobConf;
    }
    
    /**
     * Utility method to load a custom log4J properties file on HDFS. This allows
     * you to override logging levels in classes of your app. Note that loading a
     * custom properties file does not reset the existing log4J settings, only overrides
     * or appends to them.
     * 
     * Will not do anything if Log4J is not provided in the classpath
     * 
     * @param path          The path to the custom log4J properties file on HDFS
     * @throws IOException
     */
    public static void loadCustomLog4JSettings(String path) throws IOException {
        
        try {
            // Check Log4J exists on the classpath
            Class.forName("org.apache.log4j.PropertyConfigurator");
            
            // If it is, load custom Log4J Settings
            Properties propFile = loadPropertiesFile(path);
            PropertyConfigurator.configure(propFile);
            LOG.info("Loaded custom Log4J settings from " + path);
      
        } catch(ClassNotFoundException e) {
            // it does not exist on the classpath
            LOG.info("Log4J is not in the classpath for this system.");
        }    
    }
        
    /**
     * Delete a file on HDFS
     * 
     * @param path          The path to the file on HDFS
     * @throws IOException 
     */
    public static void deleteFile(String path) throws IOException {
                
        Path fsPath = new Path(path);
        FileSystem fileSystem = getFileSystem(fsPath);
        checkFileExists(fileSystem, fsPath);
        
        // Delete file
        fileSystem.delete(fsPath, true);
        fileSystem.close();
    }
    
    /**
     * Merges a list of input files in a directory to a single file under the 
     * outputpath with a specified filename
     * 
     * @param inputPath         The input directory containing all the input files. E.g. /input/dir/on/hdfs/
     * @param outputPath        The output path to output the file. E.g. /output/dir/on/hdfs/filename
     * @throws IOException
     */
    public static void mergeFiles(String inputPath, String outputPath) throws IOException {
        
        Path inputDir = new Path(inputPath);
        Path outputFile = new Path(outputPath);
        FileSystem fileSystem = getFileSystem(outputFile);
        checkFileExists(fileSystem, inputDir);
        
        // Check the input path is a directory
        if (! fileSystem.getFileStatus(inputDir).isDir()) {
            LOG.error("Path '" + inputDir.toString() + "' is not a directory.");
            throw new IOException("Path '" + inputDir.toString() + "' is not a directory.");
        }
        
        // Create Output File
        OutputStream out = fileSystem.create(outputFile);
        
        try {

            FileStatus contents[] = fileSystem.listStatus(inputDir);

            // Loop through all files in directory and merge them into one file
            for (int i = 0; i < contents.length; i++) {
                
                if (!contents[i].isDir()) {
                
                    InputStream in = fileSystem.open(contents[i].getPath());
                    try {
                        IOUtils.copyBytes(in, out, fileSystem.getConf(), false);
                    } finally {
                        in.close();
                    }         
                }
            }
        
        } finally {
            out.close();
            fileSystem.close();
            LOG.info("Merged input files from '" + inputPath + "' to '" + outputPath + "'");
        }
    }
    
    /**
     * Opens the HDFS FileSystem so file operations can be run. Configuration is
     * loaded and will automatically load Hadoop environment settings
     * 
     * @return  The HDFS FileSystem, null if failure 
     * @throws IOException
     */
    private static FileSystem getFileSystem(Path filePath) throws IOException {
            
        // Check if we have local Configuration for HDFS Set, if not it will default to local file system
        Configuration conf = new Configuration();
        if (System.getenv("HADOOP_HOME") != null) {
            LOG.info("Loading Hadoop Configuration Files under " + System.getenv("HADOOP_HOME"));
            Path coreSitePath = new Path(System.getenv("HADOOP_HOME"), "conf/core-site.xml");
            conf.addResource(coreSitePath);
            Path hdfsSitePath = new Path(System.getenv("HADOOP_HOME"), "conf/hdfs-site.xml");
            conf.addResource(hdfsSitePath);
        } else {
            LOG.info("HADOOP_HOME Not Set. Using Local File System.");
        }

        return filePath.getFileSystem(conf);
    } 
    
    /**
     * Check if a file exists, if not will throw a FileNotFoundException
     * 
     * @param path              The path of the file to check
     * @throws IOException 
     */
    private static void checkFileExists(FileSystem fileSystem, Path path) throws IOException {
        
        // Check file exists
        if (! fileSystem.exists(path)) {
            LOG.error("Path '" + path.toString() + "' does not exist.");
            fileSystem.close();
            throw new FileNotFoundException("Path '" + path.toString() + "' does not exist.");
        }
    }
}