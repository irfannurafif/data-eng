#!/usr/bin/env python3

import csv
import re
import itertools

from neo4j.v1 import GraphDatabase, basic_auth
driver = GraphDatabase.driver('bolt://localhost:7687', auth=basic_auth('neo4j', 'asdf'))
session = driver.session()

def serialize(dict):
    result = []
    for property in dict:
        result += [property, dict[property]]
    return result

if 1:
    writer = csv.writer(open('temporal-network-nodes.csv', 'w'))
    result = session.run("MATCH (n) RETURN n")
    for record in result:
        node = record['n']
        writer.writerow([node.id] + serialize(node.properties))

if 1:
    writer = csv.writer(open('temporal-network-edges.csv', 'w'))
    result = session.run("MATCH ()-[e]->() RETURN e")
    for record in result:
        node = record['e']
        writer.writerow([node.id, node.start, node.end] + serialize(node.properties))

session.close()
