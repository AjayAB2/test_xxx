#!/bin/bash
#中文测试
input_dir=$1
if [[ "X$input_dir" = "X" ]]; then
	echo "Usage: $0 input_dir"
	exit;
fi
echo $input_dir
input_dir=${input_dir%/}
echo $input_dir

echo "Begin to make dir ..."
find ${input_dir} -type d -exec mkdir -p ${input_dir}_utf8/{} \;
echo "Make dir end"

find ${input_dir} -type f |\
	while read fileName ; do
		fileInfo=`file ${fileName}`
		isUtf8=`awk -v fileInfo="$fileInfo" 'BEGIN{print index(fileInfo,"UTF-8")?"ok":"no found";}'`
		isISO=`awk -v fileInfo="$fileInfo" 'BEGIN{print index(fileInfo,"ISO")?"ok":"no found";}'`
		isASCII=`awk -v fileInfo="$fileInfo" 'BEGIN{print index(fileInfo,"ASCII")?"ok":"no found";}'`

		if [[ "X$isUtf8" = "Xok" ]]; then
			echo "----------------utf8 copy----------------"
			echo $fileInfo
			cp -p $fileName ${input_dir}_utf8/$fileName
		elif [[ "X$isISO" = "Xok" ]]; then
			echo "----------------ISO iconv----------------"
			echo $fileInfo 
			iconv -f GB18030 -t UTF-8 $fileName > ${input_dir}_utf8/$fileName
		elif [[ "X$isASCII" = "Xok" ]]; then
			echo "----------------ASCII copy----------------"
			echo $fileInfo 
			cp -p $fileName ${input_dir}_utf8/$fileName
		else
			echo -e "\e[1;31m ---------------Cann't identity file encoding, copy------ \e[0m"
			echo $fileInfo
			cp -p $fileName ${input_dir}_utf8/$fileName
		fi
	done
