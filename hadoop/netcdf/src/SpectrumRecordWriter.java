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

import ucar.ma2.*;
import ucar.nc2.*;
import ucar.nc2.util.*;

public class SpectrumRecordWriter
extends RecordWriter<Text,RawSpectrum>
{
	public SpectrumRecordWriter(String output) {
		directory = output;
	}
	
	public String directory;

	@Override
	public void
	write(Text key, RawSpectrum value) {
		
		//set filename
		final String filename = directory + "/station" + key;

		//set number of spectres
		final int NSpectres = value.NumOfSpectres();

		NetcdfFileWriter dataFile = null;		

		try {

			dataFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, filename);
		
			//create dimensions
			Dimension dates = dataFile.addUnlimitedDimension("dates");
			Dimension spectres = dataFile.addDimension(null, "spectres", NSpectres);
			

			// define dimensions
	      		List<Dimension> dims = new ArrayList<>();
			dims.add(dates);
	      		dims.add(spectres);
	      		

			//variables
			Variable iSpectr, jSpectr, kSpectr, dSpectr, wSpectr;
			iSpectr = dataFile.addVariable(null, "i", DataType.FLOAT, dims);
			jSpectr = dataFile.addVariable(null, "j", DataType.FLOAT, dims);
			kSpectr = dataFile.addVariable(null, "k", DataType.FLOAT, dims);
			wSpectr = dataFile.addVariable(null, "w", DataType.FLOAT, dims);
			dSpectr = dataFile.addVariable(null, "d", DataType.FLOAT, dims);

			dataFile.create();
			
			ArrayFloat.D2 iOut = new ArrayFloat.D2(1, spectres.getLength());
			ArrayFloat.D2 jOut = new ArrayFloat.D2(1, spectres.getLength()); 
			ArrayFloat.D2 kOut = new ArrayFloat.D2(1, spectres.getLength());
			ArrayFloat.D2 wOut = new ArrayFloat.D2(1, spectres.getLength());
			ArrayFloat.D2 dOut = new ArrayFloat.D2(1, spectres.getLength());
			
			//записываем строку спектров
			for (int i = 0; i < spectres.getLength(); i++) {
				iOut.set(value.getSerialNumber(), i, value.getValue("i", i));
				jOut.set(value.getSerialNumber(), i, value.getValue("j", i));
				kOut.set(value.getSerialNumber(), i, value.getValue("k", i));
				wOut.set(value.getSerialNumber(), i, value.getValue("w", i));
				dOut.set(value.getSerialNumber(), i, value.getValue("d", i));
				++origin[0];
			}
		
			
			dataFile.write(iSpectr, origin, iOut);
			dataFile.write(jSpectr, origin, jOut);
			dataFile.write(kSpectr, origin, kOut);
			dataFile.write(wSpectr, origin, wOut);
			dataFile.write(dSpectr, origin, dOut);


		} catch (IOException e) {
      			e.printStackTrace();

    		} catch (InvalidRangeException e) {
      			e.printStackTrace();

    		} finally {
      			if (null != dataFile)
			try {
		  		dataFile.close();
			} catch (IOException ioe) {
		  		ioe.printStackTrace();
			}
    		}
		
		
	}

	
	@Override
	public void close(TaskAttemptContext context) {
		// TODO
	}

	int[] origin = new int[]{0,0};
}
