#!/usr/bin/bash 

# Make sure we have access to the correct lbibraries
module load pandas/0.18.1

# Run
spark-submit $1.py
hadoop fs -get $1_valid.out $1_valid.out	