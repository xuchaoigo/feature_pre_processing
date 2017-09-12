#!/bin/env python
# coding=utf-8
import os
import sys
import csv
import json
import shutil
import traceback
import argparse
import pandas as pd
from pandas import Timestamp

def parse_raw(fin, fout):
    feature_list = ['passid', 'apply_dt', 'cuid']
    df = pd.read_csv(fin,sep = '\t', converters = {'cuid':str, 'passid':str, "apply_dt": Timestamp})[feature_list]

    df['cuid'] = df['cuid'].map(lambda cuid: cuid.replace('-', '').upper())
    df_out = df.drop_duplicates()
    print 'fin.count = ', df.count()
    print 'fin.uniq.count = ', df_out.count()
    df_out.to_csv(fout, index=False)
    print 'df save to ' + fout

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in', '-i', help='file input')
    parser.add_argument('--file_out', '-o', help='file output')
    args = parser.parse_args()
    
    parse_raw(args.file_in, args.file_out)
