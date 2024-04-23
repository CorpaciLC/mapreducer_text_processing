#!/bin/bash

partial_data='../data/reviews_devset.json'
all_data=''

temp_file='../data/temp.csv'

python ./job1.py "$partial_data" > "$temp_file"
python ./job2.py "$all_data" 