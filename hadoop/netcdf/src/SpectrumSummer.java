import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;


public class SpectrumSummer
extends Reducer<Text, RawSpectrum, Text, RawSpectrum>
{
    public void reduce(Text key, Iterable<RawSpectrum> values, Context context)
	throws IOException, InterruptedException
	{	
		//сортируем спектры
		SortedSet <RawSpectrum> spectres = new TreeSet<RawSpectrum>(new RawSpectrumComparator());
		for (RawSpectrum val: values) {
			spectres.add(val);
		}

		//в цикле записываем в контекст
		int k = 0;
		for (RawSpectrum val: spectres) {
			val.setSerialNumber(k);
			k++;
			context.write(key, val);
		}
       
    }
}