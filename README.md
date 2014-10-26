# Distributed Systems Fall 2013
- This repo contains code the University of Colorado Boulder class

## Homework Projects

### HadoopCount
- This assignment involved performing a distributed instillation of Hadoop on no less than 10 ec2 servers.
- Then a simple map reduce job was written in Java to analyze a public data set of weather data, from this dataset temperature values were extracted and a histogram complied of the recorded temperatures.
- Important things to know about Hadoop Instillation:
    + If you choose to develop locally you should use the **exact same** version of Hadoop **and** Java on your distributed implementation as you use to write your Map Reduce job locally.
    + Decide if you will write the job in API 1 or API 2 before you begin writing the job or installing Hadoop
    + There are some nice automation scripts for setting up Hadoop in ec2. If you choose to use these you should test the set up, then find the Hadoop and Java versions and install these version locally if you want to compile anything locally for testing.