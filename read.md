This is Scala Spark application for Clearscore case study.
Instruction to setup:
  1.Import the project using build.sbt file.
  2.Buld the projet to ensure all the dependencies are fetched.
  3.Place the accounts and reports input files in local directory.
  4.Change the input file directory path in run_exe.scala at line number 30 and 41 for reports and accounts respectively.
  5.Also, provide the output files diretory at line number 67,88,128 and 164 . Make sure the last sub-directory doesn't exsits already. This is where all
  the ouptut CSVs will be generated.
  
  Running the Spark Application:
    1.In Run Configuration, select Application and provide the class name and jdk used for the project.
    2.Provide the run configuration name.
    3.Run the application using the setup configured.
    
    
  Testing of Spark Application:
  1.Scala test has been used to test this application :src/test/scala/com.clearscore.casestudy.tests/def_tests.scala
  2.Please place the provided test input files in correct path as memtioned in test scala file.
  
  Test and corresponding Files. test_input.zip file is provided for the input files :
  1.Test of question1 - To check the total numbers of average credit score. Please use the files from def1_input folder.
  2.Test of question2 - To check number of users by employment status. Please use the files from def2_input folder.
  3.Test of question3 - To check the number of score ranges in the input data. Please use the files from def3_input folder.
  4.Test of question4 - To check the total number of records generated from the join of reports and account files. Please use the files
   from the def4_input/accounts and def4_input/reports folder.
  
    
