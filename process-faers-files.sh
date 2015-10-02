#!/bin/bash
# Dr. Nicholas A. Davis, 9-18-15
# Download, extract, and process zip files from the FDA's Adverse Event 
# Reporting System (FAERS).

# Download files using curl
for url in `cat faers-downloads.txt`; do curl -O $url; done

# Extract files into single directory
# don't overwrite existing filenames (unzip -B preserves duplicate named files by making backups)
ls *.zip | xargs -I{} unzip -jB {} -d extract/

cd extract

# Rename lowercase files for consistency
for file in *.txt; do mv "$file" "$(tr [:lower:] [:upper:] <<< "$file")"; done

# Convert DRUG12Q4 from UTF8 to ASCII to remove BOM <U+FEFF> Unicode character from beginning of header
piconv -f utf8 -t ascii DRUG12Q4.TXT > DRUG12Q4-mod.TXT
mv DRUG12Q4-mod.TXT DRUG12Q4.TXT

# Convert all .TXT files to Unix format
dos2unix -f *.TXT

# Convert Word and PDF documentation to plaintext using Apache Tika (optional)
for file in *.doc* *.pdf*; do java -jar ~/software/tika-app-1.10.jar --text $file > $file-converted.txt; done

# Use SHA1 sums to find duplicate documentation files to remove
find . -iname "*converted*" -exec sha1sum {} \; | sort | uniq -D -w40 | awk '{print $2}' | xargs -I{} rm {}

# Remove original Doc and PDF files
ls *doc* *pdf* | grep -v "converted" | xargs -I{} rm {}

# Move documentation to docs directory
mkdir docs && mv *converted* docs

