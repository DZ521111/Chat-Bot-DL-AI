# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 18:21:14 2020

@author: DHRUV
"""
import sqlite3
import pandas as pd

timeframes = ["2015-01"]

for tf in timeframes:
    connection = sqlite3.connect(f"{tf}.db")
    c = connection.cursor()
    limit = 5000
    last_time = 0
    cur_len = limit
    counter = 0
    test_done = False
    while (cur_len == limit):

        df = pd.read_sql(f"select * from parent_answer where unix > {last_time} and parent not null and score > 0 order by unix asc limit {limit}", connection)
        last_time = df.tail(1)["unix"].values[0]
        cur_len = len(df)
        print(cur_len)

        if not test_done:
            with open("test.from", "a", encoding = "utf8") as f:
                for content in df["parent"].values:
                    f.write(content + "\n")
            with open("test.to", "a", encoding = "utf8") as f:
                for content in df["comment"].values:
                    f.write(content + "\n")
            test_done = True
        else:
            with open("train.from", "a", encoding = "utf8") as f:
                for content in df["parent"].values:
                    f.write(content + "\n")
            with open("train.to", "a", encoding = "utf8") as f:
                for content in df["comment"].values:
                    f.write(content + "\n")
        counter += 1

        