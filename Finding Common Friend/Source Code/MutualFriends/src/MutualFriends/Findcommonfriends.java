package MutualFriends;

// This class is used for the intersection or find common friends
public class Findcommonfriends {

	// Take two string and return common friend id between that two Strings
	public static String common(String s1, String s2) {

		String first = s1;

		String second = s2;
		
		// split by  space and store into the two different arrays
		String array1[] = first.split(" ");
		String array2[] = second.split(" ");

		String output = "";

		// here i used brute force approach to find intersection 
		for (int i = 0; i < array1.length; i++) {
			for (int j = 0; j < array2.length; j++) {
				if (array2[j].equals(array1[i])) {
					output = output + array2[j] + ",";
				}
			}
		}

		
		// just remove "," from last in given string 
		if(output.length()>0)
		{
			output = output.substring(0, output.length() - 1);
		}
	
		// return the common friends as an output
		return "["+output+"]";
	}

}