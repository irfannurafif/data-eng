#!/usr/bin/env python3

import csv
import re
import itertools

# pip install neo4j-driver
from neo4j.v1 import GraphDatabase, basic_auth

def toident(key):
    key = '_' + key.lower()
    return re.sub(r'[-_\\\s&%$\/()\[\]{}"\':;]', '_', key)

def kvs(dict, keys):
    return {key: dict[key] for key in keys}

# password = asdf
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
    dicts = {}
    dicts.update(srcdict)
    dicts.update(dstdict)
    dicts.update(edgedict)
    session.run('MATCH (a:%s %s), (b:%s %s) MERGE (a)-[:%s %s]->(b)' %
                (srclabel, srcdicts, dstlabel, dstdicts, edgelabel, edgedicts),
                dicts)

csvfile = open('Camden_Licensing_Applications_Beta_filtered.csv')
reader = csv.DictReader(csvfile)
# license_application_data = itertools.islice(reader, 500)
license_application_data = [row for row in reader]

csvfile = open('Companies_Registered_In_Camden_And_Surrounding_Boroughs_filtered.csv')
reader = csv.DictReader(csvfile)
# company_data = itertools.islice(reader, 500)
company_data = [row for row in reader]

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
    ('premises', 'has_applied_at', [], 'ward'),
]

session.run('MATCH (n) DETACH DELETE n')

if 1:
    # create the 3 status nodes
    node('application_status', {'status': 'open'})
    node('application_status', {'status': 'approved'})
    node('application_status', {'status': 'refused'})

    for row in license_application_data:
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

if 1:
    for row in company_data:
        node('ward', kvs(row, ['Ward Code']))
        node('company', kvs(row, ['Company Name', 'Company Category']))
        edge('is_registered_at', {'from': row['Incorporation Date'],
                                  'until': row['Dissolution Date']},
             'company', kvs(row, ['Company Name', 'Company Category']),
             'ward', kvs(row, ['Ward Code']))

# MATCH (c:company)-[:is_registered_at]->(w:ward), (p:premises)-[:has_applied_at]->(w)
# RETURN c, w, p
# LIMIT 1000;

session.close()
