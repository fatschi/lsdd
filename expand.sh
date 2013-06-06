#!/bin/bash

path='data/'
file_discs='freedb_discs.csv'
arg_discs=$path$file_discs

pcregrep -M -o --colour=auto "\n$1;(.*)\n" $arg_discs
pcregrep -M -o "\n$2;(.*)\n" $arg_discs