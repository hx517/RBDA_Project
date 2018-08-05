# RBDA_Project
repo for RBDA final project


##  Analytics Project:  
#  ***Find Safest Place to Live in NYC from Criminal Data***

## Team Members:
#### Hao Xi haoxi@nyu.edu

## preliminary
* Hadoop, Spark setup in Dumbo Cluster
* Download Crime Dataset from [here](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i "NYPD crime dataset")
* upload	the	data file on HDFS directly by [HUE](http://babar.es.its.nyu.edu:8888/filebrowser/ "HUE webpage here")
* Log into the main HPC node:
  - (Windows) open Ubuntu which is installed from Microsoft Store. type `ssh -Y hpctunnel`, and then enter password when prompted.
  - type `ssh dumbo`. Enter password again.
  - load jupyter notebook for python coding, by typing `module load anaconda3/4.3.1`, `jupyter notebook`, copy and paste the url for jupyter notebook.
* Set environment variables
  -  `alias hfs='/usr/bin/hadoop fs '`
  -  `export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib`
  -  `export HSJ=hadoop-mapreduce/hadoop-streaming.jar`
  -  `alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'`

# Data Cleaning
* Run	the	Data Cleaner Python	program	using	Spark: `spark-submit datacleaned.py NYPD_Complaint_Data_Historic.csv`
* Output can be found in cleandata.csv, get in dumbo using: `hadoop fs -getmerge cleaned_data.csv cleaned_data.csv`

# Data Analysis
* Run	the	Data Analysis Python programs using	Spark: `spark-submit 'namehere.py' NYPD_Complaint_Data_Historic.csv`
* Output can be found in `output_file_name.out`, get in dumbo using: `hadoop fs -getmerge 'output_namehere.out' 'output_namehere.out'`
* Then download output files to local.

# Data Visualization
* Do the visualization part in local, using jupyter notebook and Arcgis for visualization.

