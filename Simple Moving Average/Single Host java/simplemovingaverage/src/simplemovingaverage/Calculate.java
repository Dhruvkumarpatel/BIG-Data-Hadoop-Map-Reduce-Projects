package simplemovingaverage;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

// class to calculate Simple Moving Average
public class Calculate {

	// store value according to window size
	ArrayList<Double> storeprev = new ArrayList<Double>();

	static double calculation;

	double SMA;

	// calculate simple moving average for number according to window size and
	// return the simple
	// moving Average
	public double returnmovingaverage(int windowsize, double number) {

		// calculate simple moving average for first entry
		if (storeprev.size() == 0) {
			calculation = calculation + number;

			storeprev.add(number);

			SMA = calculation;

		}
		// calculate if Arraylist size less than window size
		else if (storeprev.size() < windowsize) {

			calculation = calculation + number;

			storeprev.add(number);

			SMA = calculation / storeprev.size();
		}
		// calculate if ArrayList size greater than window size
		else {

			double value = storeprev.get(storeprev.size() - windowsize);

			// remove first value from ArrayList
			storeprev.remove(value);

			calculation = calculation - value + number;

			storeprev.add(number);

			SMA = calculation / windowsize;
		}
		// Return SMA to main method
		return SMA;
	}

	public static void main(String args[]) throws FileNotFoundException {

		// object of Calculate Class
		Calculate c1 = new Calculate();

		// Read from the input.txt file
		File f1 = new File("src/simplemovingaverage/input.txt");

		Scanner sc = new Scanner(f1);

		Scanner o = new Scanner(System.in);

		ArrayList<Double> list = new ArrayList<Double>();

		System.out.println("Enter the window size");

		// Enter the window size from user through command line
		int window_size = Integer.parseInt(o.nextLine());

		while (sc.hasNextLine()) {

			String line = sc.nextLine();

			list.add(Double.parseDouble(line));

		}

		for (int i = 0; i < list.size(); i++) {

			// send window size and closing price for calculate simple moving
			// Average
			double SMA = c1.returnmovingaverage(window_size, list.get(i));

			// OutPut printed on console
			System.out.println(list.get(i) + ":" + SMA);

		}

	}

}
