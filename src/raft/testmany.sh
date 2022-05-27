#!/bin/bash
int=1
while(( $int<=50 )) 
do
	echo "*********$int**********\n"
	go test -run 2A 
	let "int++"
done
