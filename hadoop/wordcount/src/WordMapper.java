import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;


public class WordMapper
extends Mapper<Object, Text, Text, Text>
{
    public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException
	{	String s = value.toString();
		String[] val = s.split("\\W+");
		
		for (int i = 0; i < val.length-1; i++) {
				context.write(new Text(val[i]), new Text(val[i+1]));
		}
       
    }
}
