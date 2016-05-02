All the Documentation of my code I explained in my Report.
Here is the steps to run the source code.
1) unzip my project.
2) For Hadoop MapReduce :
	a) Goto Source Code and import Kmercounting.zip in your eclipse.
	b) create input directory in hdfs and open input file folder in my project and put _user_vcslstudent_kmersinput_ecoli.fa inside input directory.
	c) Now set the Run configuration in eclipse According to what professor mentioned in vcsl manual.
	d) Run Kmercounting.java which contains Mapper , Reducer and Driver class.
	e) you will get output in your output directory.
	f) here i put my output file _user_vcslstudent_kmersoutput_part-r-00000 for 10-mers in 10-mers output folder and output file 
	_user_vcslstudent_kmersoutput_part-r-00000 for 20-mers in 20-mers output folder
	g) So in my program if you need Top 10 20-mers then just change configuration parameter with 20 and for 10 just change parameter with 10.
Thank you.