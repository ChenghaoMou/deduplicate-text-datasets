#!/bin/bash

lan="en"
sa_output_file="/home/chenghao/data_tooling/ac_dc/deduplicate/outputs/${lan}/sa.txt"
final_output_file="/home/chenghao/data_tooling/ac_dc/deduplicate/outputs/${lan}/substring_bytes.tsv"
text_file="/home/chenghao/data_tooling/ac_dc/deduplicate/outputs/${lan}/text.csv"
id_file="/home/chenghao/data_tooling/ac_dc/deduplicate/outputs/${lan}/ids.csv"

rm -rf ${sa_output_file} ${final_output_file}
rm -rf /tmp/dups_text.csv_*
python scripts/make_suffix_array.py ${text_file}
cargo run selfsimilar_parallel ${text_file}
cargo run collect_similar text.csv >> ${sa_output_file} # use filename only
python scripts/restore.py ${text_file} ${sa_output_file} ${id_file} >> ${final_output_file}
gsutil -o GSUtil:parallel_composite_upload_threshold=150M -m cp /home/chenghao/data_tooling/ac_dc/deduplicate/outputs/${lan}/{substring_bytes.tsv,sa.txt,ids.csv,text.csv} gs://commoncrawl2022/outputs/${lan}/