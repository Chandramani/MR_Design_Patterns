
package util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.MapWritable

import org.apache.hadoop.io.Writable;
/*
 writable  extending MapWritable*/
public class StripMapWritable extends MapWritable<String> {

		public StripMapWritable() {
		super();
	}


	public static StripMapWritable create(DataInput in) throws IOException {
		StripMapWritable m = new StripMapWritable();
		m.readFields(in);

		return m;
	}

	public void plus(StripMapWritable m) {
		for (Map.Entry<String, String> e : m.entrySet()) {
			String key = e.getKey();
			if (this.containsKey(key)) {
				this.put(key, this.get(key) + e.getValue());
			} else {
				this.put(key, e.getValue());
			}
		}

//to increment the value 
	public void increment(String key) {
	  increment(key, 1);
	}


  public void increment(String key, int n) {
    if (this.containsKey(key)) {
      this.put(key, this.get(key) + n);
    } else {
      this.put(key, n);
    }
  }
}
