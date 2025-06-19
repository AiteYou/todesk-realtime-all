#!/bin/bash

beg_s=`date -d "$1" +%s`
end_s=`date -d "$2" +%s`
ss=$beg_s

get_table_path(){
for i in dim dwd dws dwt ods;  
do  
	 hadoop fs -ls /warehouse/todesk/$i | awk '{print $8}' >> ~/bin/filename.txt
done  
}
get_table_size(){
cat  ~/bin/filename.txt | while read line
do
        while [ "$beg_s" -le "$end_s" ];do
                day=$(date -d "@$beg_s" +"%Y-%m-%d")
                fileName="dt=$day"
                if hadoop fs -test -e "$line/$fileName"; then
                    fileSize=$(hadoop fs -ls "$line/$fileName" | awk -F ' ' '{print $5}' | awk '{a+=$1}END{print a/(1024*1024)}')
                    echo "$line,$day,$fileSize" >> ~/bin/filetotal.txt
                else
                    echo "$line,$day,not exist" >> ~/bin/filetotal.txt
                fi

                beg_s=$((beg_s+86400))

        done
        beg_s=$ss
done
}

case $3 in
"path" )
     get_table_path
;;
"size" )
     get_table_size
;;
"all" )
     get_table_path
     get_table_size
;;
esac
