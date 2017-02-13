# IWZ_1
# (up-to-date version of repo IWZ)

wavetronix data aggregation , grouping and filtering

### java source code for parallel processing of wavetronix data in HDFS for Iowa DOT's TCP IWZ project

Wavetronix (a kind of radar sensor) traffic data are stored in hadoop distributed file system (HDFS)
The data size is 500MB per day which yields to 15GB per month. Total ~400GB of two year data are stored in HDFS. Data is still comming in one file per day.

Three major tasks are performed by this project:

1. reaggregation: The raw 20s interval data can be reaggregated into any higher level grid. Desire summary stats can be calculated during the reggregation
2. filtering: desired sensors are selected based on a pre-defined list
3. grouping: selected sensors are set into groups and the output are organized as one file per group
