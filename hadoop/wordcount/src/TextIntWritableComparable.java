     public class TextIntWritableComparable implements WritableComparable {
       // Some data
       private String valword;
       private int valint;
       
       public void write(DataOutput out) throws IOException {
         out.writeBytes(valword.toString());
         out.writeInt(valint);
       }
       
       public void readFields(DataInput in) throws IOException {
         valword = in.readLine();
         valint = in.readInt();
       }
       
       public int compareTo(TextIntWritableComparable o) {
         int thisValue = this.value;
         int thatValue = o.value;
         return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
       }

      /* public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + counter;
         result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
         return result
     */  }

	public String toString() {
	       return valword + ' ' + valint;
       }
     }