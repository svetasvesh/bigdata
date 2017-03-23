import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;
import java.lang.*;

public class SpectrumRecordReader 
extends RecordReader<Text,RawSpectrum>
{
	
	// line example: 2012 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
	private static final Pattern
	LINE_PATTERN = Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) 
	throws IOException, InterruptedException {

		CombineFileSplit combineSplit = (CombineFileSplit) split;
		Path[] names = combineSplit.getPaths();
		

		for (int i = 0; i < names.length; i++) {
			FileSystem fs = names[i].getFileSystem(context.getConfiguration());
			FSDataInputStream unzip = fs.open(names[i]);
			BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(unzip))); 
			String data = in.readLine();
			while(data != null) {
				
				Matcher matcher = LINE_PATTERN.matcher(data);
				if (matcher.matches()) {
					String date = matcher.group(1);
					String spectr = matcher.group(2);
					
					Pattern counts = Pattern.compile("[^\\s]+");
					Matcher matchercounts = counts.matcher(spectr);
					List <String> listcounts = new ArrayList<>();
					while (matchercounts.find()) {
						listcounts.add(matchercounts.group());
					}

					float[] spectres = new float[listcounts.size()];
					for (int j = 0; j < listcounts.size(); j++) spectres[j] = Float.parseFloat(listcounts.get(j));

					if (hmap.get(date)==null) {
						RawSpectrum tmpspectr = new RawSpectrum();
						tmpspectr.setField(names[i].getName(), spectres); 
						hmap.put(date, tmpspectr);
					} else { 
						hmap.get(date).setField(names[i].getName(), spectres);
					}
					
				}
				data = in.readLine();
			}
		}
		
		Iterator<Map.Entry<String, RawSpectrum>> itr = hmap.entrySet().iterator();
			while (itr.hasNext())
    				if (itr.next().getValue().isValid() != true) itr.remove();

		itr1 = hmap.entrySet().iterator();	
}
	

	@Override
	public boolean nextKeyValue()
	throws IOException, InterruptedException 
	{
		f = itr1.hasNext();
		if (f)	tmpmap = itr1.next();	
		return f;
	}

	@Override
	public void close() {
		// can be empty
	}
	
	@Override
	public Text getCurrentKey() {
		currentDate = tmpmap.getKey();
		return new Text(currentDate);
	}
	
	@Override
	public RawSpectrum getCurrentValue() {
		currentSpec = tmpmap.getValue();
		return currentSpec;
	}

	@Override
	public float getProgress() {
		return 0.f;
	}
	
	private boolean f;	
	private String currentDate;
	private RawSpectrum currentSpec;
	private Map<String, RawSpectrum> hmap = new HashMap<>();
	private Iterator<Map.Entry<String, RawSpectrum>> itr1; 
	private Map.Entry<String, RawSpectrum> tmpmap;

}
