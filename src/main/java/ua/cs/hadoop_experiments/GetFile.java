/*
   Copyright 2018 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package ua.cs.hadoop_experiments;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class GetFile {
    
    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
        FileSystem fs;
        Path rootPath;
        
        System.out.println("Initializing HDFS client");
        
        String nameNodeURI = args[0];
        
        if(nameNodeURI != null && !nameNodeURI.isEmpty()) {
            config.set("fs.defaultFS", nameNodeURI);
        } else {
            config.set("fs.defaultFS", "hdfs://localhost:9000");
        }
        
        String hdfsRoot = config.get("fs.defaultFS");
        System.out.println("hdfs root : " + hdfsRoot);
        
        // combine hdfs root + user defined root
        String userPath = args[1];
        
        if(userPath != null && !userPath.isEmpty()) {
            rootPath = new Path(hdfsRoot, userPath);
        } else {
            rootPath = new Path(hdfsRoot, "/");
        }
        
        fs = rootPath.getFileSystem(config);
        
        FileStatus status = fs.getFileStatus(rootPath);
        if(status.isFile()) {
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            System.out.println("> Path : " + status.getPath().toUri().toString());

            long blockSize = status.getBlockSize();
            System.out.println("> BlockSize : " + blockSize);
            
            int blockID = Integer.parseInt(args[2]);
            System.out.println("> Requested BlockID : " + blockID);
            
            BlockLocation location = fileBlockLocations[blockID];
            long offset = location.getOffset();
            long length = location.getLength();
            System.out.println(">> Offset: " + offset);
            System.out.println(">> Length: " + length);

            String[] names = location.getNames();
            System.out.println(">> Names");
            for(String name : names) {
                System.out.println(name);
            }

            String[] hosts = location.getHosts();
            System.out.println(">> Hosts");
            for(String host : hosts) {
                System.out.println(host);
            }

            System.out.println("");
            
            long startTime = System.currentTimeMillis();
            FSDataInputStream input = fs.open(status.getPath());
            input.seek(offset);
            
            int buffersize = 1024*64;
            byte[] buffer = new byte[buffersize];
            long toRead = length;
            
            while(toRead > 0) {
                int read = input.read(buffer, 0, (int) Math.min(buffersize, toRead));
                if(read <= 0) {
                    break;
                }
                
                toRead -= read;
                System.out.print(".");
            }
            System.out.println("");
            
            long endTime = System.currentTimeMillis();
            System.out.println("Download of the block took " + (endTime - startTime) + " milliseconds");
            input.close();
            
        } else {
            throw new IOException("Not a file");
        }
    }
}
