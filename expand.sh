#!/bin/bash
if [ "$1" != "-t" ]; then
  FLAG=false
  discs=($1, $3)
else
  FLAG=true
  discs=($1, $3)
fi

path='data/'
file_discs='freedb_discs.csv'
file_tracks='freedb_tracks.csv'
arg_discs=$path$file_discs
arg_tracks=$path$file_tracks

for disc_id in $discs
do
    pcregrep -M -o --colour=auto "\n$disc_id;(.*)\n" $arg_discs
	if [ $FLAG == true ]; then
  		pcregrep -M -o "\n$1;(.*)\n" $arg_tracks;
	fi
done

function usage {
   cat << EOF
Usage: script.sh -t <application>

Performs some activity
EOF
   exit 1
}
