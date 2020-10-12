set -x
#! /bin/sh

. /tmp/Ecomm_BDA_VAP_Validation/Ecomm_BDA_VAP_Validation.conf


schema="dev_audit"

export Rcvr_Mail_Addr="Hoa.Luu@fisglobal.com"

export Prcs_Strtd_TS=$(date '+%Y%m%d%H%M%S')
export Prcs_Strtd_TS_Fmt=`date +%Y-%m-%d_%H-%M-%S_%N`

export Btch_Dt=$(date '+%Y%m%d')        # Date on which the master script batch started
export Btch_dt_hive_fmt=$(date +%Y-%m-%d)  # Date on which the master script batch started hive format


export Log_File=${Script_Nm}_${Prcs_Strtd_TS_Fmt}.log
export Err_Log_File=${Script_Nm}_Error_${Prcs_Strtd_TS_Fmt}.log

exec 1> ${Local_Path}/${Log_File} 2> ${Local_Path}/${Err_Log_File}

echo -e BDA-VAP Validation process Script $0 started at `date +%Y-%m-%d_%H-%M-%S_%N`"\n"

echo -e "\n"Log file name for script $0 execution is ${Local_Path}${Log_File}
echo Error Log file name for script $0 execution is ${Local_Path}${Err_Log_File}

echo -e "\n"Run window is ${Job_Run_Intrvl}
echo Batch Size to run parallely the spark submit process/s is ${Batch_Size}
echo Date on which the master script batch started is ${batch_dt_hive_fmt}
echo Spark Ingestion Pool used for spark submit process/s is ${Spark_Sbmt_Ing_Pool}
echo Spark Number of Executors used for  spark submit process/s is ${Spark_Sbmt_Ing_Executors}
echo Spark Number of Cores used for spark submit process/s is ${Spark_Sbmt_Ing_Cores}
echo Spark Executor Memory used for spark submit process/s is ${Spark_Sbmt_Ing_Exec_Mem}
echo Spark Driver Memory used for spark submit process/s is ${Spark_Sbmt_Ing_Drvr_Mem}
echo Spark Count Star jar name is ${Jar_Nm_Sprk_Count}
echo Spark submit deploy mode is ${Spark_Deploy_Md}
echo -e "\n"BDA node name is ${BDA_Nd_Nm}
echo -e "\n"script name is ${Script_Nm}


hdfs dfs -put -f  ${Local_Path}/${Log_File} ${Hdfs_Log_Path}
hdfs dfs -put -f ${Local_Path}/${Err_Log_File} ${Hdfs_Log_Path}

 
Submit_Each() {
    echo $1 start
    
    spark2-submit --queue ${Spark_Sbmt_Ing_Pool} --class ${Class_Nm_Sprk_Ing} --deploy-mode ${Spark_Deploy_Md} --master yarn --num-executors ${Spark_Sbmt_Ing_Executors} --executor-cores ${Spark_Sbmt_Ing_Cores} --executor-memory ${Spark_Sbmt_Ing_Exec_Mem} --driver-memory ${Spark_Sbmt_Ing_Drvr_Mem} ${HDFS_Jar_Loc}${Jar_Nm_Sprk_Ing} $1 
    
    if [ $? -eq 0 ];
        echo $1 complete
    then
        spark2-submit --queue ${Spark_Sbmt_Ing_Pool} --class ${Class_Nm_Sprk_Ing} --deploy-mode ${Spark_Deploy_Md} --master yarn --num-executors ${Spark_Sbmt_Ing_Executors} --executor-cores ${Spark_Sbmt_Ing_Cores} --executor-memory ${Spark_Sbmt_Ing_Exec_Mem} --driver-memory ${Spark_Sbmt_Ing_Drvr_Mem} ${HDFS_Jar_Loc}${Jar_Nm_Sprk_Ing} $1 
        if [ $? -eq 0 ];
             echo "$1 Completed"
        then 
             echo "$1 Failed"
        
        fi;
    fi
    
    hdfs dfs -put -f  ${Local_Path}/${Log_File} ${Hdfs_Log_Path}
    hdfs dfs -put -f ${Local_Path}/${Err_Log_File} ${Hdfs_Log_Path}

}


echo 'Starting for the part I of Today Validation'
j=1
for tableId in $(hive -S -e "set mapreduce.job.queuename = ingestion; select table_id from ${schema}.bda_data_validation_conf WHERE count_ind='Y';") 
  do 
    echo "Table ID is ${tableId}"
    if echo "${tableId}" | grep -qE '^[0-9]+$'; then
        echo "Valid number."
        
          if ($j%${Batch_Size} != 0)
                Submit_Each ${tableId} ${Btch_dt_hive_fmt}&
          else 
                Submit_Each ${tableId} ${Btch_dt_hive_fmt}& wait
                j=1
          fi
          ((j+=1))
    fi
 done
 
& wait

echo 'Done for the part I of today Validation'


echo 'Starting for the part II of Previous Days Failed Validation'

j=1
for tableId in $(hive -S -e "set mapreduce.job.queuename = ingestion; select t2.table_id, to_Date(t1.inserted_date) from ${schema}.bda_data_counts_validation t1 inner join ${schema}.bda_data_validation_conf t2 on upper(t1.table_name) =upper(t2.table_name) and t2.failed_rerun = 'Y' and t1.matched='N';") 
 do 
  echo "Table ID is ${tableId}"
  if ($j%${Batch_Size} != 0)
            Submit_Each ${tableId} &
  else 
            Submit_Each ${tableId} & wait
            j=1
  fi
  ((j+=1))
  
 done
 
& wait

echo 'Done for the part II of Previous Days Failed Validation'




 

hdfs dfs -put -f  ${Local_Path}/${Log_File} ${Hdfs_Log_Path}
hdfs dfs -put -f ${Local_Path}/${Err_Log_File} ${Hdfs_Log_Path}