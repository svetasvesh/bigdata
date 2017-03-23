import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;


public class ValueSorter
extends Mapper<Object, Text, Int, Text>
{
    public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException
	{	String s = value.toString();
		String[] val = s.split("\\W+");
		Integer k;
		k.decode(val[2]);
		
		context.write(new TextIntWritable(val[1],k), new Text(val[0]));
       
    }
}