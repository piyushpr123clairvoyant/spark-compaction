# Spark Compaction
When streaming data into HDFS, small messages are written to a large number of files that if left unchecked will cause unnecessary strain on the HDFS NameNode.  To handle this situation, it is good practice to run a compaction job on directories that contain many small files to help reduce the resource strain of the NameNode by ensuring HDFS blocks are filled efficiently.  It is common practice to do this type of compaction with MapReduce or on Hive tables / partitions and this tool is designed to accomplish the same type of task utilizing Spark.

**Compaction Strategies:**
```
1. Default(default): In this Strategy number of output files to be generated after compaction is calculated depending on the calculation give below in compression Math.
2. Size Range(size_range): In this Strategy user will provide the range in the config file. So the final disck space occupied is calculated and checked under which range it falls and its corresponding value is taken as output file size.
   Example: "compaction": {
                "size_ranges_for_compaction": [
                  {
                    "min_size_in_gb": 0,
                    "max_size_in_gb": 50,
                    "size_after_compaction_in_mb": 256
                  },
                  {
                    "min_size_in_gb": 50,
                    "max_size_in_gb": 100,
                    "size_after_compaction_in_mb": 512
                  },
                  {
                    "min_size_in_gb": 100,
                    "max_size_in_gb": 0,
                    "size_after_compaction_in_mb": 1000
                  }
                ]
              }
    The Range is taken between "min_size_in_gb" and "max_size_in_gb". If the size of the files falls under the range then "size_after_compaction_in_mb" is taken as output file size. And total space occupied by the files is then divided by the output file size and the result is the number of output files.
        Nubmer of Output files = (Total disk space occupied by files in mb)/(size_after_compaction_in_mb)
```

## Compression Math

At a high level this class will calculate the number of output files to efficiently fill the default HDFS block size on the cluster taking into consideration the size of the data, compression type, and serialization type.

**Compression Ratio Assumptions**
```vim
SNAPPY_RATIO = 1.7;     // (100 / 1.7) = 58.8 ~ 40% compression rate on text
LZO_RATIO = 2.0;        // (100 / 2.0) = 50.0 ~ 50% compression rate on text
GZIP_RATIO = 2.5;       // (100 / 2.5) = 40.0 ~ 60% compression rate on text
BZ2_RATIO = 3.33;       // (100 / 3.3) = 30.3 ~ 70% compression rate on text

AVRO_RATIO = 1.6;       // (100 / 1.6) = 62.5 ~ 40% compression rate on text
PARQUET_RATIO = 2.0;    // (100 / 2.0) = 50.0 ~ 50% compression rate on text
```

**Compression Ratio Formula**
```vim
Input Compression Ratio * Input Serialization Ratio * Input File Size = Input File Size Inflated
Input File Size Inflated / ( Output Compression Ratio * Output Serialization Ratio ) = Output File Size
Output File Size / Block Size of Output Directory = Number of Blocks Filled
FLOOR( Number of Blocks Filled ) + 1 = Efficient Number of Files to Store
```

### Text Compaction

**Text to Text Calculation**
```vim
Read Input Directory Total Size = x
Detect Output Directory Block Size = 134217728 => y

Output Files: FLOOR( x / y ) + 1 = # of Mappers
```

**Text to Text Snappy Calculation**
```vim
Default Block Size = 134217728 => y
Read Input Directory Total Size = x
Compression Ratio = 1.7 => r

Output Files: FLOOR( x / (r * y) ) + 1 = # of Mappers
```

**Execution Using Shell Script**
```
A shell script is written to help users execute the compaction job. It runs the spark job and stores the logs of each run.

Instructions to Deploy the app:
1. Navigate to the directory where you want to deploy this application. Run the following commands to create the required directories to set the environment for the project.
      mkdir bin
      mkdir conf
      mkdir lib 
2. Now do the following steps to copy the files to the project directory.
      a. Copy the application_configs.json file from the resources dir in the project to conf directory. Update the application_configs.json file with the required configurations.
      b. Copy the jar file thats created by running `mvn clean install` to lib directory.
      c. Copy the shell script under bin dir in the project to bin directory that you have created. Change the permissions for the file if necessary.
```


**Execution Options**

```vim
spark-submit \
  --class com.apache.bigdata.spark_compaction.Compact \
  --master local[2] \
  --driver-class-path {CONF_PATH}:${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  ${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  --input-path ${INPUT_PATH} \
  --output-path ${OUTPUT_PATH} \
  --input-compression [none snappy gzip bz2 lzo] \
  --input-serialization [text parquet avro] \
  --output-compression [none snappy gzip bz2 lzo] \
  --output-serialization [text parquet avro]
  --compaction-strategy [default size_range]
```

It is not required to pass the last four variables as it will be inferred from the input path and output paths by using the same input options for the output options.  It should also be noted that if Avro is used as the output serialization only uncompressed and snappy compression are supported in the upstream package (spark-avro by Databricks) and the compression type will not be passed as part of the output file name.  The other option that is not supported is Parquet + BZ2 and that will result in an execution error. If Compaction Strategy is not passed then the default strategy is taken in to account in which number of output files are decided based on the calculation given above.
 
**Execution Example (Text to Text):**

```vim
spark-submit \
  --class com.apache.bigdata.spark_compaction.Compact \
  --master local[2] \
  --driver-class-path {CONF_PATH}:${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  ${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  -i ${INPUT_PATH} \
  -o ${OUTPUT_PATH} \
  -ic [input_compression] \
  -is [input_serialization] \
  -oc [output_compression] \
  -os [output_serialization] \
  -cs [default size_range]

spark-submit \
  --class com.apache.bigdata.spark_compaction.Compact \
  --master local[2] \
  --driver-class-path {CONF_PATH}:${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  ${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  -i hdfs:///landing/compaction/input \
  -o hdfs:///landing/compaction/output \
  -ic none \
  -is text \
  -oc none \
  -os text \
  -cs default

spark-submit \
  --class com.apache.bigdata.spark_compaction.Compact \
  --driver-class-path {CONF_PATH}:${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  --master local[2] \
  ${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  -i hdfs:///landing/compaction/input \
  -o hdfs:///landing/compaction/output
```

To elaborate further, the following example has an input directory consisting of 9,999 files consuming 440 MB of space.  Using the default block size, the resulting output files are 146 MB in size, easily fitting into a data block.

**Input (Text to Text)**
```vim
$ hdfs dfs -du -h /landing/compaction/input | wc -l
9999
$ hdfs dfs -du -h /landing/compaction
440.1 M  440.1 M  /landing/compaction/input

440.1 * 1024 * 1024 = 461478298 - Input file size.
461478298 / 134217728 = 3.438
FLOOR( 3.438 ) + 1 = 4 files

440.1 MB / 4 files ~ 110 MB
```

**Output (Text to Text)**
```vim
$ hdfs dfs -du -h /landing/compaction/output
110.0 M  110.0 M  /landing/compaction/output/part-00000
110.0 M  110.0 M  /landing/compaction/output/part-00001
110.0 M  110.0 M  /landing/compaction/output/part-00002
110.0 M  110.0 M  /landing/compaction/output/part-00003
```


### Parquet Compaction

**Input (Parquet Snappy to Parquet Snappy)**
```vim
$ hdfs dfs -du -h /landing/compaction/input_snappy_parquet | wc -l
9999
$ hdfs dfs -du -h /landing/compaction
440.1 M  440.1 M  /landing/compaction/input_snappy_parquet

440.1 * 1024 * 1024 = 461478298 - Total input file size.
1.7 * 2 = 3.4 - Total compression ratio.
(3.4 * 461478298) / (3.4 * 134217728) = 3.438
FLOOR( 3.438 ) + 1 = 4 files

440.1 MB / 2 files ~ 110 MB
```

**Output (Parquet Snappy to Parquet Snappy)**
```vim
$ hdfs dfs -du -h /landing/compaction/output_snappy_parquet
110 M  110 M  /landing/compaction/output_snappy_parquet/part-00000.snappy.parquet
110 M  110 M  /landing/compaction/output_snappy_parquet/part-00001.snappy.parquet
110 M  110 M  /landing/compaction/output_snappy_parquet/part-00002.snappy.parquet
110 M  110 M  /landing/compaction/output_snappy_parquet/part-00003.snappy.parquet
```



## Multiple Directory Compaction

**Sub Directory Processing**
```vim
$ hdfs dfs -du -h /landing/compaction/partition/
293.4 M  293.4 M  /landing/compaction/partition/date=2016-01-01

$ hdfs dfs -du -h /landing/compaction/partition/date=2016-01-01
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=00
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=01
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=02
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=03
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=04
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=05

$ hdfs dfs -du -h /landing/compaction/partition/date=2016-01-01/* | wc -l
6666
```

**Output Directory**
```vim
$ hdfs dfs -du -h /landing/compaction/partition/output_2016-01-01
293.4 M  293.4 M  /landing/compaction/partition/output_2016-01-01

$ hdfs dfs -du -h /landing/compaction/partition/output_2016-01-01/* | wc -l
3
```

**Wildcard for Multiple Sub Directory Compaction**
```vim
spark-submit \
  --class --class com.apache.bigdata.spark_compaction.Compact \
  --master local[2] \
  --driver-class-path {CONF_PATH}:${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  ${JAR_PATH}/spark-compaction-1.0.0-jar-with-dependencies.jar \
  --input-path hdfs:///landing/compaction/partition/date=2016-01-01/hour=* \
  --output-path hdfs:///landing/compaction/partition/output_2016-01-01 \
  --input-compression none \
  --input-serialization text \
  --output-compression none \
  --output-serialization text \
  --compaction_strategy default
```
