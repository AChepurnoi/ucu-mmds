#!/bin/bash
wget https://dumps.wikimedia.org/enwiki/20190620/enwiki-20190620-pages-articles-multistream1.xml-p10p30302.bz2
bzip2 -d enwiki-20190620-pages-articles-multistream1.xml-p10p30302.bz2 
mv enwiki-20190620-pages-articles-multistream1.xml-p10p30302 xml_data/sample.xml
