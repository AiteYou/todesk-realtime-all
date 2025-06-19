#!/bin/bash
# 设置要操作的HDFS路径和要删除的日期
hdfs_path="/spark-history"
delete_before_date="2025-01-31"

# 列出目录下的文件，并逐行处理
hdfs dfs -ls "$hdfs_path" | while read -r line; do
  file_path=$(echo "$line" | awk '{print $NF}')  # 提取文件路径
  file_date=$(echo "$line" | awk '{print $6}')  # 提取创建日期
  if [[ "$file_date" < "$delete_before_date" ]]; then
    # 删除早于指定日期的文件
    hdfs dfs -rm -skipTrash "$file_path"
    echo "Deleted: $file_path"
  fi
done
