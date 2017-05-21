#!/usr/bin/env python3


# pip install neo4j-driver
from neo4j.v1 import GraphDatabase, basic_auth

# Application Reference,Application Type,Premises Name,Premises
# Address,Application Legislation,Application Status,Application
# Objections
# Approved Date
# Date Of Events
# Expiry Date
# Hearing Date
# Last Date
# For Representations
# Received Date
# Refused Date
# Cumulative Impact Policy Area
# Name,Ward Code,Ward Name,Easting,Northing,Longitude,Latitude,Spatial
# Accuracy,Last Uploaded,Location,Socrata ID,Organisation URI

import csv
import re

def show(key, value):
    return "'%s'" % value

def toident(key):
    key = '_' + key.lower()
    return re.sub(r'[-_\\\s&%$\/()\[\]{}"\':;]', '_', key)

def kvs(dict, keys):
    return {key: dict[key] for key in keys}

driver = GraphDatabase.driver('bolt://localhost:7687', auth=basic_auth('neo4j', 'asdf'))
session = driver.session()

def magic(dict):
    dict = {toident(key): dict[key] for key in dict}
    dicts = '{%s}' % ', '.join('%s: {%s}' % (key, key) for key in dict)
    return (dict, dicts)

def node(nodelabel, dict={}):
    dict, dicts = magic(dict)
    session.run('MERGE (:%s %s)' % (nodelabel, dicts), dict)

def edge(edgelabel, edgedict, srclabel, srcdict, dstlabel, dstdict):
    srcdict, srcdicts = magic(srcdict)
    dstdict, dstdicts = magic(dstdict)
    edgedict, edgedicts = magic(edgedict)
    crucible = {}
    crucible.update(srcdict)
    crucible.update(dstdict)
    crucible.update(edgedict)
    session.run('MATCH (a:%s %s), (b:%s %s) MERGE (a)-[:%s %s]->(b)' %
                (srclabel, srcdicts, dstlabel, dstdicts, edgelabel, edgedicts),
                crucible)

csvfile = open('Camden_Licensing_Applications_Beta.csv')
reader = csv.DictReader(csvfile)
data = [row for row in reader][:25]

nodeattrs = {
    'ward': ['Ward Code', 'Ward Name'],
    'premises': ['Premises Name', 'Premises Address'],
    'application': ['Application Reference', 'Application Legislation',
                    'Approved Date', 'Expiry Date', 'Hearing Date',
                    'Date Of Events', 'Last Date For Representations',
                    'Received Date', 'Refused Date'],
    'application_type': ['Application Type'],
    'objection': ['Application Objections'],
}

nodeedges = [
    ('application', 'belongs_to', [], 'ward'),
    ('application', 'created_by', [], 'premises'),
    ('application', 'is_a', [], 'application_type'),
    ('application', 'is_being_objected', [], 'objection'),
]

if 1:
    session.run('MATCH (n) DETACH DELETE n')

    node('application_status', {'status': 'open'})
    node('application_status', {'status': 'approved'})
    node('application_status', {'status': 'refused'})

    for row in data:
        for nodelabel in nodeattrs:
            attrs = nodeattrs[nodelabel]
            node(nodelabel, kvs(row, attrs))
        for (srclabel, edgelabel, edgeattrs, dstlabel) in nodeedges:
            srcattrs = nodeattrs[srclabel]
            dstattrs = nodeattrs[dstlabel]
            edge(edgelabel, kvs(row, edgeattrs),
                 srclabel, kvs(row, srcattrs),
                 dstlabel, kvs(row, dstattrs))
        d_approved = row['Approved Date']
        d_expiry = row['Expiry Date']
        d_hearing = row['Hearing Date']
        d_events = row['Date Of Events']
        d_last = row['Last Date For Representations'] # TODO what is this
        d_received = row['Received Date']
        d_refused = row['Refused Date']
        # print('received %10s, refused %10s, approved %10s, either %10s' %
        #       (d_received, d_refused, d_approved, d_refused or d_approved))
        if d_approved:
            edge('has_status', {'from': d_approved, 'until': d_expiry},
                 'application', kvs(row, nodeattrs['application']),
                 'application_status', {'status': 'approved'})
        if d_received:
            edge('has_status', {'from': d_received, 'until': d_refused or d_approved},
                 'application', kvs(row, nodeattrs['application']),
                 'application_status', {'status': 'open'})
        if d_refused:
            edge('has_status', {'from': d_refused, 'until': d_approved},
                 'application', kvs(row, nodeattrs['application']),
                 'application_status', {'status': 'refused'})

session.close()
