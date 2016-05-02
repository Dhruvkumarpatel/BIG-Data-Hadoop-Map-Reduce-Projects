package SimpleMovingAverage;

import java.util.ArrayList;

// This class return simple moving average according to company closing price and window size
public class calculatesma {

	// store value according to window size
	static ArrayList<Double> prevvalue = new ArrayList<Double>();

	static double calculation;

	static double SMA;

	// function that take window size and closing price as input and return
	// simple moving average
	public static double calculation(int windowsize, double number) {

		// this is for first entry
		if (prevvalue.size() == 0) {
			calculation = calculation + number;

			prevvalue.add(number);

			SMA = calculation;
		}
		// if list size less than window size
		else if (prevvalue.size() < windowsize) {
			calculation = calculation + number;

			prevvalue.add(number);

			SMA = calculation / prevvalue.size();
		}
		// if list size greater than window size
		else {
			double value = prevvalue.remove(prevvalue.size() - windowsize);

			prevvalue.remove(value);

			calculation = calculation - value + number;

			prevvalue.add(number);

			SMA = calculation / windowsize;
		}

		// return simple moving average
		return SMA;
	}

}
