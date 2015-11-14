#!/bin/env python3

import sys
import os
import math
from datetime import datetime
from collections import namedtuple
from enum import Enum
from lxml import etree

# Data Structures

# Utility Functions

# csv

def create_records(filename, headers):
    if not headers or len(headers) < 1:
        raise Exception("Error in headers")
    for h in headers:
        if "," in h:
            raise Exception("Malformed Header: %s" % h)
    if os.path.exists(os.path.realpath(filename)):
        raise Exception("File Already Exists")
    try:
        os.makedirs(os.path.dirname(os.path.realpath(filename)))
    except FileExistsError:
        pass
    with open(os.path.realpath(filename), 'w') as f:
        print(",".join(headers), file=f)

def insert_records(filename, records=[]):
    if not records or len(records) < 1:
        return
    with open(os.path.realpath(filename), 'r') as f:
        all_lines = f.read()
    lines = all_lines.split("\n")
    headers = None
    existing_records = {}
    for line in lines:
        try:
            if not headers:
                headers = line.split(",")
                if len(headers) != len(records[0]):
                    raise Exception("Record format does not match file format")
                continue
            tokens = line.split(",")
            rec_key = tokens[0]
            timestamp = float(tokens[1])
            if rec_key not in existing_records:
                existing_records[rec_key] = []
            existing_records[rec_key] += [tuple([timestamp] + tokens[2:])]
        except (IndexError, ValueError):
            continue
    for record in sorted(records, key=lambda x: float(x[1])):
        rec_key = str(record[0])
        timestamp = float(record[1])
        insert_record = tuple(map(str, record[2:]))
        if rec_key not in existing_records:
            existing_records[rec_key] = []
        if len(existing_records[rec_key]) > 0 and (existing_records[rec_key][-1][0] > timestamp or existing_records[rec_key][-1][1:] == insert_record):
            continue
        existing_records[rec_key] += [tuple([timestamp]) + insert_record]
    with open(os.path.realpath(filename), 'w') as f:
        print(",".join(headers), file=f)
        for key in sorted(existing_records.keys()):
            for tup in existing_records[key]:
                print("%s,%s" % (key, ",".join(map(str, tup))), file=f)

def get_records(filename, key):
    key = str(key)
    with open(os.path.realpath(filename), 'r') as f:
        all_lines = f.read()
    lines = all_lines.split("\n")
    headers = None
    existing_records = []
    for line in lines:
        try:
            if not headers:
                headers = line
                continue
            tokens = line.split(",")
            rec_key = tokens[0]
            if "|" in rec_key:
                rec_key_main = rec_key.split("|")[0]
            else:
                rec_key_main = rec_key
            if rec_key_main != key:
                continue
            timestamp = float(tokens[1])
            existing_records += [tuple([rec_key, timestamp] + tokens[2:])]
        except ValueError:
            continue
    return existing_records

# XML

def create_records_xml(filename):
    if os.path.exists(os.path.realpath(filename)):
        raise Exception("File Already Exists")
    try:
        os.makedirs(os.path.dirname(os.path.realpath(filename)))
    except FileExistsError:
        pass
    with open(os.path.realpath(filename), 'w') as f:
        print("<root/>", file=f)

def insert_records_xml(filename, records):
    if not records or len(records) < 1:
        return
    with open(os.path.realpath(filename), 'r') as f:
        all_lines = f.read()
    root = etree.fromstring(all_lines)
    existing_records = root.xpath("*")
    pass

def get_records_xml(filename, key):
    existing_records = []
    with open(os.path.realpath(filename), 'r') as f:
        all_lines = f.read()
    root = etree.fromstring(all_lines)
    records = root.xpath("*")
    for record in records:
        try:
            values = record.xpath("*")
            tokens = [v.text for v in values]
            print(tokens, file=sys.stderr)
            rec_key = tokens[0]
            if rec_key != key:
                continue
            timestamp = float(tokens[1])
            existing_records += [tuple([rec_key, timestamp] + tokens[2:])]
        except (ValueError, IndexError):
            continue
    return existing_records

