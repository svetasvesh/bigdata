import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class SpectrumInputFormat
extends InputFormat<Text,RawSpectrum>
{
		public static final Pattern
		FILENAME_PATTERN = Pattern.compile("([0-9]{5})([dijkw])([0-9]{4})\\.txt\\.gz");

	@Override
	public List<InputSplit>	getSplits(JobContext ctx)
	throws IOException, InterruptedException
	{
		

		Map<String, List<Path>> hmap = new HashMap<>();
		List<InputSplit> out = new ArrayList<InputSplit>();

		for (Path path : FileInputFormat.getInputPaths(ctx)) {
			FileSystem fs = path.getFileSystem(ctx.getConfiguration());
			for (FileStatus file : fs.listStatus(path)) {
				String filename = file.getPath().getName();
				Matcher matcher = FILENAME_PATTERN.matcher(filename);
				if (matcher.matches()) {
					String field = matcher.group(1)+"-"+matcher.group(3);					
					if (hmap.get(field) == null) {
						List <Path> tmp = new ArrayList<>();
						tmp.add(file.getPath());
						hmap.put(field,tmp);
					} else {
						hmap.get(field).add(file.getPath());
					}
					
				}
				
			}
		}
		for (Map.Entry<String, List<Path>> entry: hmap.entrySet()) {
			if (entry.getValue().size() == 5) {
				List <Path> tmp = entry.getValue();
				Path[] arr = new Path[5];
				out.add(new CombineFileSplit(tmp.toArray(arr), getFileLengths(tmp, ctx))); 
			}
		}
		//for (InputSplit val: out) System.out.println(val);
		return out;
	}

	@Override
	public RecordReader<Text,RawSpectrum>
	createRecordReader(InputSplit split, TaskAttemptContext context)
	throws IOException, InterruptedException
	{
		return new SpectrumRecordReader();
	}

	private static long[]
	getFileLengths(List<Path> files, JobContext context)
	throws IOException
	{
		long[] lengths = new long[files.size()];
		for (int i=0; i<files.size(); ++i) {
			FileSystem fs = files.get(i).getFileSystem(context.getConfiguration());
			lengths[i] = fs.getFileStatus(files.get(i)).getLen();
		}
		return lengths;
	}

}
