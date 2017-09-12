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
import numpy as np


def multi_cols(df_meta, key, col_list):
    dic_list = []
    dic_dic = {}
    for col in col_list:
        print 'start groupby', col
        df = pd.DataFrame(df_meta, columns=[key, col])
        grouped1 = df.groupby(key)
        for passid, group in grouped1:
            sum = group[col].count()
            vc = group[col].value_counts()
            vlst = zip(vc.index.tolist(), vc.values.tolist())
            if passid not in dic_dic:
                dic_dic[passid] = {}
            if 'passid' not in dic_dic[passid]:
                dic_dic[passid]['passid'] = passid
            dic_dic[passid][col + '_list'] = vlst

    for passid in dic_dic:
        dic_list.append(dic_dic[passid])
    return pd.DataFrame(dic_list)


def one_col(df_meta, key, col):
    df = pd.DataFrame(df_meta, columns=[key, col])
    grouped1 = df.groupby(key)
    listdic = []
    for passid, group in grouped1:
        sum = group[col].count()
        vc = group[col].value_counts()
        vlst = zip(vc.index.tolist(), vc.values.tolist())
        listdic.append({'passid': passid, col + '_list': vlst})
    return pd.DataFrame(listdic)


def parse_raw(fin, fout):
    print 'start load ', fin
    feature_list = ["passid", "fea1", "fea2"]
    dicList=[json.loads(line) for line in open(fin)]
    df = pd.DataFrame(dicList)
    
    print 'start calc factor'
    #df1 = one_col(df, "passid", "fea1")
    df1 = multi_cols(df, "passid", ["fea1", "fea2"])
    df1.to_csv(fout, index = False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in_left', '-il', help='file1 input')
    parser.add_argument('--file_out', '-o', help='file output')
    args = parser.parse_args()
    
    parse_raw(args.file_in_left, args.file_out)
