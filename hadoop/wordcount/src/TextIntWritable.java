import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;
import java.lang.*;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.util.*;


public class TextIntWritable implements Writable {
       // Some data     
       private String valword;
       private int valint;
       
       //constructors
       public TextIntWritable() {
       }
       
       public TextIntWritable(String a, int b) {
       	valword = a;
       	valint = b;
       }
       
       public void write(DataOutput out) throws IOException {
         out.writeBytes(valword.toString());
         out.writeInt(valint);
       }
       
       public void readFields(DataInput in) throws IOException {
         valword = in.readLine();
         valint = in.readInt();
       }
       
       public static TextIntWritable read(DataInput in) throws IOException {
       TextIntWritable w = new TextIntWritable();
        w.readFields(in);
        return w;
       }
       
       public String toString() {
	       return valword + ' ' + valint;
       }
}

