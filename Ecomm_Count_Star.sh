set -x
#! /bin/sh
unset HADOOP_CONF_DIR
. /tmp/Ecomm_Count_Star/Ecomm_Count_Star_Tables.conf

export Rcvr_Mail_Addr="Hoa.Luu@fisglobal.com"

export Prcs_Strtd_TS=$(date '+%Y%m%d%H%M%S')
export Prcs_Strtd_TS_Fmt=`date +%Y-%m-%d_%H-%M-%S_%N`

export Btch_Dt=$(date '+%Y%m%d')        # Date on which the master script batch started
export batch_dt_hive_fmt=$(date +%Y-%m-%d)  # Date on which the master script batch started hive format


export Log_File=${Script_Nm}_${Prcs_Strtd_TS_Fmt}.log
export Err_Log_File=${Script_Nm}_Error_${Prcs_Strtd_TS_Fmt}.log

exec 1> ${Local_Path}/${Log_File} 2> ${Local_Path}/${Err_Log_File}

echo -e BDA Tables Count Star process Script $0 started at `date +%Y-%m-%d_%H-%M-%S_%N`"\n"

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


#Get list of tables 
echo 'print in order tableid tablename saved_path to spark argument'
var_tableId_Ls=`hdfs dfs -cat ${Conf_Hdfs_Location}* | awk -F'\a' '{print $2" "$1"."$3" "$7" ~IND_YESNO_"$5}' | grep 'IND_YESNO_Y'  | sed 's/ ~IND_YESNO_Y//g'`

Proc_Submit_Each() {
    echo "Submitting table_id table_name date saved_path, Spark job started with these paras" 
    paras="$1 $2 $4 ${Data_Hdfs_Location}" 
    spark2-submit --queue ${Spark_Sbmt_Ing_Pool} --class ${Main_Class} --deploy-mode ${Spark_Deploy_Md} --master yarn --num-executors ${Spark_Sbmt_Ing_Executors} --executor-cores ${Spark_Sbmt_Ing_Cores} --executor-memory ${Spark_Sbmt_Ing_Exec_Mem} --driver-memory ${Spark_Sbmt_Ing_Drvr_Mem} ${HDFS_Jar_Loc}${Jar_Nm_Sprk_Count} $paras  
    
    if [ $? -eq 0 ];
    then
        echo $paras completed
    else
        spark2-submit --queue ${Spark_Sbmt_Ing_Pool} --class ${Main_Class} --deploy-mode ${Spark_Deploy_Md} --master yarn --num-executors ${Spark_Sbmt_Ing_Executors} --executor-cores ${Spark_Sbmt_Ing_Cores} --executor-memory ${Spark_Sbmt_Ing_Exec_Mem} --driver-memory ${Spark_Sbmt_Ing_Drvr_Mem} ${HDFS_Jar_Loc}${Jar_Nm_Sprk_Count} $paras
        if [ $? -eq 0 ]; 
        then
             echo $paras completed after second try
        else 
             echo $paras 'Failed'
             echo -e "\n"Job CountStar for  $paras Failed | mail -s "Alert:Job CountStar - $paras failed in `hostname` on `date +%Y-%m-%d`" "${Rcvr_Mail_Addr}"
        
        fi;
    fi

}

SAVEIFS=$IFS   # Save current IFS
IFS=$'\n'      # Change IFS to new line
tablelist=($var_tableId_Ls) # split to array
IFS=$SAVEIFS   # Restore IFS
echo $tablelist
for (( i=0; i<${#tablelist[@]}; i++ ))
do
    echo "$i: ${tablelist[$i]}"
    date_ago=`echo ${tablelist[$i]} | awk -F' ' '{ if ($3 ~ /^[0-9]+$/ ) print $3}'`
    [  -z "$date_ago" ] && date_ago=0
    
    summarize_date=`date -d "$batch_dt_hive_fmt - $date_ago days" '+%Y-%m-%d'` 
        
     j=$i+1
      
      if [ $j%${Batch_Size} != 0 ]; then
            Proc_Submit_Each ${tablelist[$i]} "$summarize_date" &
      else 
            Proc_Submit_Each ${tablelist[$i]} "$summarize_date" & wait
      fi
    

done
wait

echo 'Done'

 hdfs dfs -put -f  ${Local_Path}/${Log_File} ${Hdfs_Log_Path}
 hdfs dfs -put -f ${Local_Path}/${Err_Log_File} ${Hdfs_Log_Path}

