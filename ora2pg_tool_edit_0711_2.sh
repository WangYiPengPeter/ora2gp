#!/bin/sh
source ~/.bash_profile
export PGPASSWORD=xdhSIS123

if [ $# -lt 6 ];then
    echo "选项：
-C  CONFIG，对应conf文件中的oraconf文件 必备参数
-S  SRC TABLE  需要从oracle中读出的表  必备参数
-D  DES TABLE  需要载入到gp中的表  必备参数
-R  CHARSET(落地文件的字符集：AL32UTF8 或 ZHS16GBK)
-L  使用到的作为查询的COLUMN列
-G  使用到的作为查询的COLUMN的类型  STRING  TIMESTAMP
-B  BEGINTIME 用作查询列对应的起始时间
-E  ENDTIME 用作查询列对应的截止时间
-T  TRANSFORM  是否需要使用sed去掉[\x00]类的乱码
-M  MODE 需要使用的load文件的预设模式
-K  KEEP 是否需要保存oracle导出的数据文件。 默认为NO 删除导出数据文件。 
-U  TRUNCATE 是否需要把greenplum中的原表清空.  默认为NO ,不清空。"
    exit 1
fi

#trap the exception quit
trap 'log_info "TERM/INTERRUPT(subprocess) close";close_subproc' INT TERM

declare -a sublist

function log_info()
{
    DATETIME=`date +"%Y%m%d %H:%M:%S"`
    echo -e "S $DATETIME P[$$]: $*"| tee -a "$LOGPATH"/"$CONFNAME"/"$GPTABLE"_$$.log
}

function check_is_keyword()
{ 
  THE_WORD="$(echo $1 | tr '[:lower:]' '[:upper:]')"
  IS_KEYWORD=$(eval `grep ^[^#] /home/hadoop/ora2gp/conf/gpconf |awk -F':'  -v T_WORD=$THE_WORD '{printf("psql -h %s -p %s -U %s %s -tAc \042 select 1 from ods.oracle_key_word where word_key=\047%s\047 \042",$1,$2,$4,$3,T_WORD)}'`)
  if [  -z $IS_KEYWORD  ];then
    echo 0
  else
    echo 1
  fi
}


function collect_subproc()
{
        local index
        if [ ${#sublist} -eq 0 ];then
                index=0
        else
                index=$[${#sublist}]+1
        fi
        sublist[$index]=$1

}

function close_subproc()
{
    for subid in ${sublist[@]}
    do
        log_info "kill processid: $subid"
        kill $subid
    done
}

function parse_yaml()
{
    local file=$1
    local tablename=$2
    local pipename=$3
    sed -i -e "s/mypipe/"$pipename"/" -e "s/tablename_err/"$tablename"_err/" -e "s/\<tablename\>/"$tablename"/"    $file
}

export -f check_is_keyword
if [ $(dirname $0) == '.' ];then
    PRIPATH=${PWD}
else 
        PRIPATH=$(dirname $0)
fi
TPLPATH="$PRIPATH"/template
LOGPATH="$PRIPATH"/log

echo $PRIPATH


while getopts "c:C:s:S:d:D:r:R:l:L:g:G:b:B:e:E:tTm:M:kKuU" arg
do
   case $arg in 
        [Cc]) echo  "C 配置项名称：$OPTARG"
	   CONFNAME=$OPTARG
		;;
        [Ss]) echo  "S 源表表名：$OPTARG"
	   ORATABLE="$(echo $OPTARG | tr '[:lower:]' '[:upper:]')"
		;;
        [Dd]) echo  "D 目标表表名：$OPTARG"
	   GPTABLE="$(echo $OPTARG | tr '[:lower:]' '[:upper:]')"
		;;
        [Rr]) echo  "R 字符集：$OPTARG"
           CHARSET=$OPTARG
		   if [ "$CHARSET" != "AL32UTF8" ] && [ "$CHARSET" != "ZHS16GBK" ] ;then
		      echo "error CHARSET.  charset只接受AL32UTF8或ZHS16GBK"
			  exit 1
		   fi
		;;
        [Ll]) echo  "L 作为查询条件的列名：$OPTARG"
           COLUMN=$OPTARG
		;;
        [Gg]) echo "G 作为查询条件的列的类型：$OPTARG"
           COLUMN_TYPE=$OPTARG
		   if [ "$COLUMN_TYPE" != "STRING" ] && [ "$COLUMN_TYPE" != "TIMESTAMP" ] && [ "$COLUMN_TYPE" != "DATE" ] ;then
		      echo "error COLUMN_TYPE.  条件列的时间戳类型只能为STRING 或 TIMESTAMP 或者 DATE"
			  exit 1
		   fi
		;;
        [Bb]) echo  "B 作为查询条件使用到的起始时间(>=)：$OPTARG"
	       START_TIME=$OPTARG
		;;
        [Ee]) echo  "E 作为查询条件使用到的截止时间(<)：$OPTARG"
	       END_TIME=$OPTARG
		;;
        [Tt]) echo  "T 是否需要使用sed去掉[\x00]类的乱码：$OPTARG"
	       #IS_SED="$(echo $OPTARG | tr '[:lower:]' '[:upper:]')"
	       IS_SED="YES"
              echo $IS_SED
		;;
        [Mm]) echo "M 指定需要使用的load方式的预设模式： $OPTARG"
               MODE=$OPTARG
                ;;
        [Kk]) echo "K keep  是否保持oracle导出的数据文件:  $OPTARG"
              # KEEPFILE="$(echo $OPTARG | tr '[:lower:]' '[:upper:]')"
               KEEPFILE="YES"
	       echo $KEEPFILE	;;
        [Uu])  echo "U truncate 是否将greenplum原表中的数据清空:  $OPTARG"
               #IS_TRUNCATE="$(echo $OPTARG | tr '[:lower:]' '[:upper:]')"
               IS_TRUNCATE="YES"
              echo $IS_TRUNCATE ;;
        ?) echo "unknow argument"
   		   exit 1
		;;
   esac
done

if [ -z $CONFNAME ] || [  -z $ORATABLE ] || [  -z $GPTABLE ] ;then
  echo "error arguments"
  exit 1;
fi

if [ -z $COLUMN_TYPE ];then
    COLUMN_TYPE="DATE"
fi


if [ -z $MODE ];then
    MODE=$CONFNAME
fi
#"$CHARSET" =  "AL32UTF8"

if [ -z $CHARSET ];then
    CHARSET="AL32UTF8"
fi

if [ -z $IS_TRUNCATE ];then
    IS_TRUNCATE="NO"
fi

if [ -z $KEEPFILE ];then
    KEEPFILE="NO"
fi

[ ! -d "$LOGPATH"/"$CONFNAME" ] && mkdir -p "$LOGPATH"/"$CONFNAME"
PIPENAME="P"$$"$GPTABLE"
eval `grep "^$CONFNAME" "$PRIPATH"/conf/oraconf |awk -F':' '{print $2}'|awk -F'^' '{print "ORACLE_USER="$1";ORACLE_PASS="$2";ORACLE_SID="$3}'`
echo $GPTABLE

eval $(eval `grep ^[^#] /home/hadoop/ora2gp/conf/gpconf |awk -F':' -v table=$GPTABLE '{printf("psql -h %s -p %d -U %s %s -tAc \047\\\d %s \047",$1,$2,$4,$3,table)}'` |awk -F "|" '
{
"check_is_keyword " $1|getline result;
if(result=="0"){ 
cmd=cmd $1","
}else{
cmd=cmd "\\\""$1 "\\\"" ","
}
}
END{print "collist="cmd ; }  ')
echo abcdefg
collist=`echo $collist|sed "s/,$//g"`

#echo >> "$LOGPATH"/"$CONFNAME"/"$GPTABLE".log
#create and modify template for gpload use
#log_info "create template "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl."
#cp "$TPLPATH"/gp_template_load_"$MODE".ctl "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl
#if [ $? -ne 0 ]; then
#    log_info "create template "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl failed."
#        exit 2
#fi

#parse_yaml "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl $GPTABLE $PIPENAME $CONFNAME
#if [ $? -ne 0 ]; then
#    log_info "modify template "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl failed."
#        exit 2
#fi

#log_info "gbk导出的文件，需要落地，转换，不采用管道方式.将生产文件 /home/hadoop/ora_to_gp/ora2gp/"$PIPENAME"."
#gbk导出的文件，需要落地，转换，不采用管道方式
#mknod ~/ora_to_gp/ora2gp/"$PIPENAME" p
echo "本次sqluldr2导出的中间文件为""$PRIPATH/$PIPENAME"
if [ -z $COLUMN ];then
	echo "sqluldr2 的查询条件没有对时间戳的约束条件。"
    condition=""
else
        if [ -z $START_TIME ] || [  -z $END_TIME ];then
 	   	echo "未传入查询起始或截止时间，默认选择起始为昨天凌晨，截止今天凌晨"
    		DATEEND=0
    		START_TIME=`date -d "yesterday" +%Y%m%d`
    		END_TIME=`date -d "today" +%Y%m%d`
  	fi

	if [ $COLUMN_TYPE == "DATE" ];then
		condition=" and $COLUMN <to_date($END_TIME,'yyyymmddhh24miss') and $COLUMN>=to_date($START_TIME,'yyyymmddhh24miss') "		
	elif [ $COLUMN_TYPE == "STRING" ];then
		condition=" and $COLUMN <$END_TIME||'000000' and $COLUMN>=$START_TIME||'000000' "		
	elif  [ $COLUMN_TYPE == "TIMESTAMP" ];then
                condition=" and $COLUMN <to_timestamp('$END_TIME','yyyymmddhh24miss') and $COLUMN>=to_timestamp('$START_TIME','yyyymmddhh24miss') "
	fi
	echo "sqluldr2 的时间戳查询条件为  $condition"
fi

echo $PRIPATH/"$PIPENAME"  
log_info "unload sql:select $collist from $ORATABLE   where 1=1  $condition"

 ~/sqluldr2 user="$ORACLE_USER"/"$ORACLE_PASS"@"$ORACLE_SID" query="select $collist from $ORATABLE  where 1=1 $condition "   field=0x7c file=$PRIPATH/"$PIPENAME" charset=$CHARSET  text=CSV safe=yes   log=+"$LOGPATH"/"$CONFNAME"/SQLULDR2_$$"$GPTABLE".log
collect_subproc $!


has_ora_error=`sed -n '$p' "$LOGPATH"/"$CONFNAME"/SQLULDR2_$$"$GPTABLE".log`
#判断sqluldr 过程是否包含错误
if [[ ! $has_ora_error =~ "ORA-" ]];then
      log_info "时间范围在$LASTTIME 至 $CURRENTTIME 中的文件，落地过程正常"
 else
      log_info "时间范围在$LASTTIME 至 $CURRENTTIME 中的文件，落地过程有错误,请查看导出日志：" "$LOGPATH"/"$CONFNAME"/SQLULDR2_$$"$GPTABLE".log
      exit 1
fi

#if [ $? -ne 0 ];then
#    log_info "sqluldr2 failed!"
#    exit 1
#else
#  if [ $COLUMN  ]; then
#      getMaxInfo_sql=`cat $PRIPATH/conf/$ORATABLE/sql`
#      echo "$getMaxInfo_sql"
#      MaxInfo=`executeSql "$ORACLE_USER/$ORACLE_PASS@$ORACLE_SID" "$getMaxInfo_sql"`
#      echo "$MaxInfo"> $PRIPATH/conf/$ORATABLE/temp_file$ORATABLE
#      grep "^max" $PRIPATH/conf/$ORATABLE/temp_file$ORATABLE | awk -F ':' '{print $2}' >$PRIPATH/conf/$ORATABLE/max_info
#      rm -rf $PRIPATH/conf/$ORATABLE/temp_file$ORATABLE
#  fi
#fi
#!

cat "$LOGPATH"/"$CONFNAME"/SQLULDR2_$$"$GPTABLE".log
echo "$LOGPATH"/"$CONFNAME"/SQLULDR2_$$"$GPTABLE".log
unload_file_size_row=`sed -n '$p' "$LOGPATH"/"$CONFNAME"/SQLULDR2_$$"$GPTABLE".log`
echo $unload_file_size_row
unload_file_size=`sed -n '$p' "$LOGPATH"/"$CONFNAME"/SQLULDR2_$$"$GPTABLE".log | awk '{print $9}'`
echo $unload_file_size
if  [ ! -z $IS_SED   ]  &&  [ "$IS_SED"  ==  "YES" ] ;then
        echo "sed $PRIPATH/"$PIPENAME""
        sed -i 's/[\x00]//g' $PRIPATH/"$PIPENAME"
fi

if  [ "$CHARSET" == "ZHS16GBK" ] ;then
	echo "落地文件字符集为ZHS16GBK，需要处理"
       ~/codeconv  $PRIPATH/"$PIPENAME"  $PRIPATH/"$PIPENAME".out  gbk  utf8  2
        #iconv -c -f GB18030//IGNORE -t UTF-8 $PRIPATH/"$PIPENAME" >  $PRIPATH/"$PIPENAME".out
fi

echo $unload_file_size
if  [ "$IS_TRUNCATE"  ==  "YES" ] ;then
     echo "清空表。。。"
	truncate_statement=`grep ^[^#] /home/hadoop/ora2gp/conf/gpconf |awk -F':' -v GPTABLE=$GPTABLE  -v  GPTABLE_err_table=$GPTABLE_err_table  -v copy_stat=$copy_stat -v  PRIPATH=$PRIPATH -v PIPENAME=$PIPENAME  '{	printf("psql -h %s -p %s -U %s %s ",$1,$2,$4,$3)}'`
	truncate_statement_com="truncate table   $GPTABLE  ;"
        truncate_statement=`sed s/-a// <<<$truncate_statement`
        $copy_statement 2>&1 <<- EOF | tee -a "$LOGPATH"/"$CONFNAME"/"$GPTABLE"_$$.log
        \timing
        $truncate_statement_com
EOF
else 
  echo ""
fi

echo $unload_file_size
if ((  $unload_file_size < 1024 ));then
	# chmod 777 $PRIPATH/$PIPENAME
	echo 'small file!! use copy...'
	GPTABLE_err_table="$GPTABLE"_err
	copy_statement=`grep ^[^#] /home/hadoop/ora2gp/conf/gpconf |awk -F':' -v GPTABLE=$GPTABLE  -v  GPTABLE_err_table=$GPTABLE_err_table  -v copy_stat=$copy_stat -v  PRIPATH=$PRIPATH -v PIPENAME=$PIPENAME  '{	printf("psql -h %s -p %s -U %s %s ",$1,$2,$4,$3)}'`
	echo $copy_statement
	if [ "$CHARSET" =  "AL32UTF8"  ];then
		copy_statement_com="\copy   $GPTABLE   from  '$PRIPATH/$PIPENAME' delimiter as ',' csv header quote as '\"'  LOG ERRORS INTO $GPTABLE_err_table KEEP SEGMENT REJECT LIMIT  100   ;"
	elif [  "$CHARSET" =  "ZHS16GBK"  ];then
		copy_statement_com="\copy   $GPTABLE   from  '$PRIPATH/$PIPENAME.out' delimiter as ',' csv header quote as '\"'  LOG ERRORS INTO $GPTABLE_err_table KEEP SEGMENT REJECT LIMIT  100   ;"
	fi
	echo $copy_statement_com
        copy_statement=`sed s/-a// <<<$copy_statement` 
       $copy_statement 2>&1 <<- EOF | tee -a "$LOGPATH"/"$CONFNAME"/"$GPTABLE"_$$.log
       \timing
       $copy_statement_com
EOF
        #判断sqluldr 过程是否包含错误

	has_copy_error=`sed -n '$p'  "$LOGPATH"/"$CONFNAME"/"$GPTABLE"_$$.log`
	if [[ ! $has_copy_error =~ "error" ]];then
	      log_info "采用copy方式，向 ""$GPTABLE""插入数据正常结束..."
	else
	      log_info "采用copy方式，向 ""$GPTABLE""插入数据有误，请查看日志 ""$LOGPATH"/"$CONFNAME"/"$GPTABLE"_$$.log 
	      exit 1
	fi
else 
	echo 'big file!!use gpload... '

	cp "$TPLPATH"/gp_template_load_"$MODE".ctl "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl
	if [ $? -ne 0 ]; then
		log_info "create template "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl failed."
		exit 2
	fi
	if [ "$CHARSET" =  "AL32UTF8"  ];then
		parse_yaml "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl $GPTABLE "$PIPENAME"
	elif [  "$CHARSET" =  "ZHS16GBK"  ];then
		parse_yaml "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl $GPTABLE "$PIPENAME".out 
	fi
	gpload -f "$LOGPATH"/"$CONFNAME"/"$GPTABLE".ctl -l "$LOGPATH"/"$CONFNAME"/"$GPTABLE"_$$.log
	collect_subproc $!
	wait
	if [ $? -ne 0 ];then
	    log_info "GPLOAD failed!"
	    exit 1
	else
	    log_info "GPLOAD succ!"
	fi


fi
	if  [ "$KEEPFILE" =  "NO"  ];then 
		log_info "rm -rf $PRIPATH"$PIPENAME""
	     if  [ "$CHARSET" =  "AL32UTF8"  ];then
		rm -rf  $PRIPATH/"$PIPENAME"
	     elif  [ "$CHARSET" = "ZHS16GBK" ] ;then
		rm -rf  $PRIPATH/"$PIPENAME"
		rm -rf  $PRIPATH/"$PIPENAME".out
	     fi
	else
	    echo "保持oracle导出的数据文件不删除。"
	    log_info "keep  $PRIPATH"$PIPENAME" "
	fi

	if [ $? -ne 0 ];then
	    log_info "rm -rf "$PIPENAME" failed."
	    exit 4
	else 
	    exit 0
	fi

