package matrixmultiplication;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Scanner;

public class matrixmul {

	int A[][], B[][];

	public matrixmul(int[][] a, int[][] b) throws IOException {
		super();
		A = a;
		B = b;

		// code to generate an input file which is used in hadoop map-reduce

		File file = new File("src/matrixmultiplication/sample_input.txt");

		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fwriter = new FileWriter(file.getAbsolutePath());

		BufferedWriter bw = new BufferedWriter(fwriter);

		for (int i = 0; i < A.length; i++) {
			for (int j = 0; j < A[1].length; j++) {
				if (A[i][j] != 0) {

					bw.write("A");
					bw.write(',');
					bw.write(new Integer(i).toString());
					bw.write(',');
					bw.write(new Integer(j).toString());
					bw.write(',');
					bw.write(new Double(A[i][j]).toString() + "\n");

				}

			}

		}

		for (int i = 0; i < B.length; i++) {
			System.out.println("row file length:" + B[0].length);

			for (int j = 0; j < B[1].length; j++) {
				System.out.println("column file length:" + B[1].length);

				if (B[i][j] != 0) {

					bw.write("B");
					bw.write(',');
					bw.write(new Integer(i).toString());
					bw.write(',');
					bw.write(new Integer(j).toString());
					bw.write(',');
					bw.write(new Double(B[i][j]).toString());

					bw.newLine();

				}

			}

		}

		bw.close();

	}

	public static void main(String args[]) throws IOException {

		Scanner scan = new Scanner(System.in);

		// For first matrix ----------------------------------------------

		System.out.println("Enter the first matrix for multiplication");

		System.out.println("Enter the number of rows for matrix");

		int row = Integer.parseInt(scan.nextLine());

		System.out.println("Enter the number of columns for matrix");

		int column = Integer.parseInt(scan.nextLine());

		int first_matrix[][] = new int[row][column];

		System.out.println("Fill the first matrix with " + row + "*" + column + " Matrix");

		ArrayList<Integer> random = new ArrayList<Integer>();

		for (int i = 0; i <= 200; i++) {
			random.add(i);
		}

		Collections.shuffle(random);

		for (int i = 0; i < row; i++) {
			for (int j = 0; j < column; j++) {
				first_matrix[i][j] = random.get(j);
				random.remove(j);
			}
		}

		System.out.println("Following is the First Matrix");

		for (int i = 0; i < row; i++) {
			for (int j = 0; j < column; j++) {
				System.out.println(first_matrix[i][j]);
			}
		}

		// for second matrix
		// --------------------------------------------------------

		Scanner o = new Scanner(System.in);

		System.out.println("Enter the second matrix for multiplication");

		System.out.println("Enter the number of rows for matrix");

		int row_second = Integer.parseInt(o.nextLine());

		System.out.println("Enter the number of columns for matrix");

		int column_second = Integer.parseInt(o.nextLine());

		int second_matrix[][] = new int[row_second][column_second];

		System.out.println("Fill the second matrix with " + row_second + "*" + column_second + " Matrix");

		ArrayList<Integer> randomfor2 = new ArrayList<Integer>();

		for (int i = 0; i <= 200; i++) {
			randomfor2.add(i);
		}

		Collections.shuffle(randomfor2);

		for (int i = 0; i < row_second; i++) {
			for (int j = 0; j < column_second; j++) {
				second_matrix[i][j] = randomfor2.get(j);
				randomfor2.remove(j);
			}
		}

		System.out.println("Following is the Second Matrix");

		for (int i = 0; i < row_second; i++) {
			System.out.println("Row_secod :" + row_second);

			for (int j = 0; j < column_second; j++) {

				System.out.println("column second :" + column_second);

				System.out.println(second_matrix[i][j]);
			}
		}

		System.out.println("first matrix length :" + first_matrix.length);
		System.out.println("first matrix length :" + first_matrix[1].length);
		System.out.println("second matrix length :" + second_matrix.length);
		System.out.println("second matrix length :" + second_matrix[1].length);

		new matrixmul(first_matrix, second_matrix);

	}

}
