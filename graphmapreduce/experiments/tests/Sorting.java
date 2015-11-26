package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;

public class Sorting {

	public static void main(String[] args) {

		String [] tuples = generate_tuples(1000000);
		String [] tuples2 = generate_tuples(1000000);

		System.out.println("Data generated.");

		long start = System.nanoTime();
		//		System.out.println(Arrays.toString(tuples));
		Arrays.sort(tuples);
		//		System.out.println(Arrays.toString(tuples));
		long stop = System.nanoTime();

		double time = (stop - start) / 1000000000.0;
		double size = getSize(tuples);

		System.out.println("Sorting:");
		System.out.println("size (B):" + size);
		System.out.println("time (s):" + time);
		System.out.println("Bps:" + size / time);
		System.out.println("spB:" + time / size);
		System.out.println();

		System.out.println("Sorting (MB):");
		size = getSize(tuples) / (1024.0 * 1024);
		System.out.println("size (MB):" + size);
		System.out.println("time (s):" + time);
		System.out.println("MBps:" + size / time);
		System.out.println("spMB:" + time / size);
		System.out.println();

		Arrays.sort(tuples2);
		// merge
		start = System.nanoTime();
		merge(tuples,tuples2);
		stop = System.nanoTime();
		time = (stop - start) / 1000000000.0;
		System.out.println("Merging:");
		System.out.println("size (MB):" + size);
		System.out.println("time (s):" + time);
		System.out.println("MBps:" + size / time);
		System.out.println("spMB:" + time / size);
		System.out.println();


		// write
		start = System.nanoTime();
		write(tuples, "output/testfile");
		stop = System.nanoTime();
		time = (stop - start) / 1000000000.0;
		System.out.println("Writing:");
		System.out.println("size (MB):" + size);
		System.out.println("time (s):" + time);
		System.out.println("MBps:" + size / time);
		System.out.println("spMB:" + time / size);
		System.out.println();


		// read
		start = System.nanoTime();
		read("output/testfile");
		stop = System.nanoTime();
		time = (stop - start) / 1000000000.0;
		System.out.println("Reading:");
		System.out.println("size (MB):" + size);
		System.out.println("time (s):" + time);
		System.out.println("MBps:" + size / time);
		System.out.println("spMB:" + time / size);
		System.out.println();

	}

	private static void merge(String[] tuples, String[] tuples2) {
		
		int i = 0, j = 0, k = 0;
		String [] result = new String [tuples.length + tuples2.length];
		while (j < tuples.length && k < tuples2.length) {
			
			if (tuples[j].compareTo(tuples2[k]) < 0) {
				result[i] = tuples[j];
				j++;
			} else {
				result[i] = tuples2[k];
				k++;
			}
			
			i++;
		}
		
		while (j < tuples.length) {
			result[i] = tuples[j];
			i++;
			j++;
		}
		while (k < tuples.length) {
			result[i] = tuples2[k];
			i++;
			k++;
		}

	}

	private static void read(String string) {
		try {
			File fin = new File(string);
			BufferedReader fr = new BufferedReader(new FileReader(fin));

			while (fr.readLine() != null) {
				
			}
			
			fr.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void write(String[] tuples, String string) {

		try {
			File fout = new File(string);
			FileOutputStream fos = new FileOutputStream(fout);

			OutputStreamWriter osw = new OutputStreamWriter(fos);

			for (int i = 0; i < tuples.length; i++) {

				osw.write(tuples[i]);

			}

			osw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static long getSize(String[] tuples) {

		long size = 0;
		for (int i = 0; i < tuples.length; i++) {
			size += tuples[i].length()*2;
		}

		return size;
	}

	private static String [] generate_tuples(int size) {
		String [] array = new String[size];

		for (int i = 0; i < size; i++) {
			array[i] = generate_string();

		}

		return array;
	}

	private static String generate_string() {
		long val = (long) (Math.random()*Long.MAX_VALUE);
		return ""+val;
	}

}
