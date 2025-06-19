#!/bin/bash
if [ $# -lt 1 ]
then
  echo "no args error....."
  exit
fi

for i in zuler101 zuler102 zuler103
do
  echo "===================> $i <==================="
  ssh $i "$*"
done
