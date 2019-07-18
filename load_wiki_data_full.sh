#!/bin/bash

export DUMP_BASE_URL="https://dumps.wikimedia.org/enwiki/20190701"
export XML_OUTPUT_FOLDER="data/xml/"

while read bz_dump_name; do
    wget ${DUMP_BASE_URL}/${bz_dump_name}
    bzip2 -d ${bz_dump_name}
    xml_file_name=$(echo ${bz_dump_name} | sed 's/.\{4\}$//')
    mv ${xml_file_name} ${XML_OUTPUT_FOLDER}${xml_file_name}
done < data/parts.txt

