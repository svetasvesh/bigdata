import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*; 

import java.io.*;
import java.util.*;

public class Summer
extends Reducer<Text, Text, Text, TextIntWritable>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException
	{
    	Map<String, Integer> hmap = new HashMap<String, Integer>();
    for (Text val: values) {
			String t = val.toString();
		if (hmap.get(t)==null) hmap.put(t, 1);
		else {
			Integer tmp = hmap.get(t);
			tmp++;
			hmap.put(t, tmp);
		}	    
    }
    
	Integer maxx = 0;
	String result = "";
	for (Map.Entry<String, Integer> entry: hmap.entrySet())
	{
    		if (entry.getValue() > maxx) { maxx = entry.getValue(); result = entry.getKey();}
	}
        context.write(key, new TextIntWritable(result,maxx));
    }
}
