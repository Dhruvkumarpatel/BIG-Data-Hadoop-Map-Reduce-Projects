package SimpleMovingAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

// Dateclosingprice class which implements WritableComparable interface for sorting Data 
// According to Date
public class Dateclosingprice implements WritableComparable<Dateclosingprice> {

	// instance variable for date and closing price
	private Long numericdate;
	private Double closingprice;

	public static Dateclosingprice copy(Dateclosingprice c) {
		return new Dateclosingprice(c.numericdate, c.closingprice);
	}

	// Default Constructor
	public Dateclosingprice() {

	}

	// Parameterized constructor to set the values
	public Dateclosingprice(Long numericdate, Double closingprice) {

		this.numericdate = numericdate;
		this.closingprice = closingprice;
	}

	// set Date and Closing Price
	public void set(Long numericdate, Double closingprice) {
		this.numericdate = numericdate;
		this.closingprice = closingprice;
	}

	// getter method for Date
	public Long getNumericdate() {
		return numericdate;
	}

	// setter method for Date
	public void setNumericdate(Long numericdate) {
		this.numericdate = numericdate;
	}

	// getter method for closing price
	public Double getClosingprice() {
		return closingprice;
	}

	// setter method for closing price
	public void setClosingprice(Double closingprice) {
		this.closingprice = closingprice;
	}

	public static Dateclosingprice read(DataInput in) throws IOException {
		Dateclosingprice d = new Dateclosingprice();
		d.readFields(in);
		return d;
	}

	public Dateclosingprice clone() {
		return new Dateclosingprice(numericdate, closingprice);
	}

	// inbuilt method for Deserialization
	@Override
	public void readFields(DataInput in) throws IOException {

		// TODO Auto-generated method stub
		this.numericdate = in.readLong();
		this.closingprice = in.readDouble();
	}

	// inbuilt method for serialization
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.numericdate);
		out.writeDouble(this.closingprice);
	}

	@Override
	public String toString() {
		return this.numericdate + ":" + this.closingprice;
	}

	// method is used to compare objects and sort according to Date
	@Override
	public int compareTo(Dateclosingprice o) {
		// TODO Auto-generated method stub
		int compare = numericdate.compareTo(o.numericdate);

		return compare;

	}

	/*
	 * @Override public int hashCode() { // TODO Auto-generated method stub
	 * final int prime = 31; int result = 1; result = prime * result + (int)
	 * (numericdate ^ (numericdate >>> 32)); return result; // return
	 * numericdate.hashCode(); }
	 */

	/*
	 * @Override public boolean equals(Object obj) { // TODO Auto-generated
	 * method stub
	 * 
	 * if(obj instanceof Dateclosingprice) { Dateclosingprice date =
	 * (Dateclosingprice) obj; return numericdate.equals(date.getNumericdate());
	 * } return false; }
	 */

}
