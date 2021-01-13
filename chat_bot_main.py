# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 13:43:43 2020

@author: DHRUV
"""

import sqlite3
import json
from datetime import datetime

# fetch the database
timeframe = "2015-01"
sql_change = []
connection = sqlite3.connect(f"{timeframe}.db")
c = connection.cursor()

# create a table for chat bot
def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS
              parent_answer(parent_id text primary key,
              comment_id text unique, parent text,
              comment text, subreddit text, unix int,
              score int)""")

# formation of data
def formation_data(data):
    data = data.replace("\n", "  new_line_char ").replace("\r", "  new_line_char ").replace('"', "'")
    return data

# find parents from database
def find_parent(pid):
    try:
        sql = f"select comment from parent_answer where comment_id = '{pid}' limit 1"
        c.execute(sql)
        result = c.fetchone()
        if (result != None):
            return result[0]
        else:
            return False
    except Exception as e:
        print("find_parent", e)
        return False

# find existing score from database
def find_existing_score(pid):
    try:
        sql = f"select score from parent_answer where parent_id = '{pid}' limit 1"
        c.execute(sql)
        result = c.fetchone()
        if (result != None):
            return result[0]
        else:
            return False
    except Exception as e:
        print("find_score", e)
        return False

# accept the current data or not
def accept(data):
    if (len(data.split(" ")) > 50 or len(data) < 1):
        return False
    elif (len(data) > 1000):
        return False
    elif (data == "[deleted]" or data == "[removed]"):
        return False
    else:
        return True

def exchange_bldr(sql):
    global sql_change
    sql_change.append(sql)
    if (len(sql_change) > 1000):
        c.execute("BEGIN TRANSACTION")
        for s in sql_change:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_change = []

def sql_insert_rep_cmt(cid, pid, par, cmt, sr, time, sc):
    try:
        sql = """update parent_answer set parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? where parent_id = ?;""".format(pid, cid, par, cmt, sr, int(time), sc, pid)
        exchange_bldr(sql)
    except Exception as e:
        print("Error_cmt", str(e))

def sql_insert_has_par(cid, pid, par, cmt, sr, time, sc):
    try:
        sql = """insert into parent_answer (parent_id, comment_id, parent, comment, subreddit, unix, score) values ("{}", "{}", "{}", "{}", "{}", "{}", "{}");""".format(pid, cid, par, cmt, sr, int(time), sc)
        exchange_bldr(sql)
    except Exception as e:
        print("Error_has_par", str(e))

def sql_insert_no_par(cid, pid, cmt, sr, time, sc):
    try:
        sql = """insert into parent_answer (parent_id, comment_id, comment, subreddit, unix, score) values ("{}", "{}", "{}", "{}", "{}", "{}");""".format(pid, cid, cmt, sr, int(time), sc)
        exchange_bldr(sql)
    




if __name__ == '__main__':
    create_table()

    row_counter = 0
    paired_rows = 0

    with open(f"RC_{timeframe}", buffering=1000) as file:
        for row in file:
            #print(row)
            #print("\n")
            row_counter += 1
            row = json.loads(row)
            parent_id = row["parent_id"]
            comment_id = row["name"]
            body = formation_data(row["body"])
            created_utc = row["created_utc"]
            score = row["score"]
            subreddit = row["subreddit"]
            parent_data = find_parent(parent_id)

            if (score >= 2):
                if (accept(body)):
                    existing_comment_score = find_existing_score(parent_id)
                    if (existing_comment_score):
                        if (score > existing_comment_score):
                            sql_insert_rep_cmt(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        if (parent_data):
                            sql_insert_has_par(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_no_par(comment_id, parent_id, body, subreddit, created_utc, score)

            if (row_counter % 100000 == 0):
                print(f"Total row read ::-  {row_counter}, paired row ::- {paired_rows}, times ::- {str(datetime.now())}")