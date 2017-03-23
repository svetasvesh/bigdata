import java.io.*;
import java.util.*;

public class RawSpectrumComparator 
implements Comparator<RawSpectrum> {
	public int compare (RawSpectrum entry1, RawSpectrum entry2) {
		//RawSpectrum entry1 = (RawSpectrum)obj1;
		//RawSpectrum entry2 = (RawSpectrum)obj2;
		int result = entry1.getDate().compareTo(entry2.getDate());
		return result;
	}	
}