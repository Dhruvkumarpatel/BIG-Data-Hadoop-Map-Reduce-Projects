
All the Documentation of my code I explained in my Report.
Here is the steps to run the source code.
1) unzip my project.
2) My project contains Dataset files for map reduce.
3) For Hadoop MapReduce :
	a) Goto Map-Reduce Source Code and import MatrixMultiplication in your eclipse.
	b) create input directory in hdfs and open input file folder in my project and put _user_vcslstudent_matrixinput_sample_input.csv inside input directory.
	c) you can use files which is in Datasets folder in my projects.
	d) Now set the Run configuration in eclipse According to what professor mentioned in vcsl manual.
	e) Run MatrixMultiplication.java which contains Mapper , Reducer and Driver class.
	f) you will get output in your output directory.
	g) here i put my output file _user_vcslstudent_matrixoutput_part-r-00000.txt in output folder.
	
	
4) Additionally i add simple java program to generate input file according to matrix size what user required.
5) in Datasets matrixmul.txt file i generated using my program for testing.
6) you have to change configuration parameter size externally according to your matrices size in input file. Here i gave 10*10 and 10*5 matrices similarly to your input file.

	
Thank you.
		