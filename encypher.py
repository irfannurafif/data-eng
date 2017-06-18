#!/usr/bin/env python3

import csv
import re
import itertools
from datetime import datetime

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
licence_application_data = [row for row in reader]

csvfile = open('Companies_Registered_In_Camden_And_Surrounding_Boroughs_filtered.csv')
reader = csv.DictReader(csvfile)
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

RECREATE = 1
# canonicalize date (so it compared lexicographically)
def cd(d):
    if not d:
        return ''
    dt = datetime.strptime(d, '%d/%m/%Y')
    return datetime.strftime(dt, '%Y/%m/%d')

if 0:
    #
    for row in licence_application_data:
        dd = (row['Approved Date'], row['Expiry Date'], row['Hearing Date'],
              row['Date Of Events'], row['Received Date'], row['Refused Date'])
        assert(len(d) == 10 for d in dd)
        print("%11s _%11s _%11s _%11s _%11s _%11s" % dd)

if RECREATE:
    session.run('MATCH (n) DETACH DELETE n')

if RECREATE:
    # create the 3 status nodes
    node('application_status', {'status': 'open'})
    node('application_status', {'status': 'approved'})
    node('application_status', {'status': 'refused'})

    for row in licence_application_data:
        for nodelabel in nodeattrs:
            attrs = nodeattrs[nodelabel]
            node(nodelabel, kvs(row, attrs))
        for (srclabel, edgelabel, edgeattrs, dstlabel) in nodeedges:
            srcattrs = nodeattrs[srclabel]
            dstattrs = nodeattrs[dstlabel]
            edge(edgelabel, kvs(row, edgeattrs),
                 srclabel, kvs(row, srcattrs),
                 dstlabel, kvs(row, dstattrs))
        d_approved = cd(row['Approved Date'])
        d_expiry = cd(row['Expiry Date'])
        d_hearing = cd(row['Hearing Date'])
        d_events = cd(row['Date Of Events'])
        d_last = cd(row['Last Date For Representations']) # TODO what is this
        d_received = cd(row['Received Date'])
        d_refused = cd(row['Refused Date'])
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

if RECREATE:
    for row in company_data:
        node('ward', kvs(row, ['Ward Code']))
        node('company', kvs(row, ['Company Name', 'Company Category']))
        edge('is_registered_at', {'from': row['Incorporation Date'],
                                  'until': row['Dissolution Date']},
             'company', kvs(row, ['Company Name', 'Company Category']),
             'ward', kvs(row, ['Ward Code']))

session.close()

"""
MATCH (c:company)-[:is_registered_at]->(w:ward), (p:premises)-[:has_applied_at]->(w)
RETURN c, w, p
LIMIT 1000;
"""

'E05000131'
'E05000134'
'E05000136'
'E05000139'
"Cantelowes"
"Gospel Oak"
"Haverstock"
"Kentish Town"

"""
MATCH (a:application)-[:belongs_to]->(w:ward), (a)-[:is_a]->(t:application_type)
WHERE w._ward_name = 'Gospel Oak'
RETURN w, a, t
"""

"""
MATCH (c:company)-[:is_registered_at]->(w:ward)
WHERE w._ward_name = 'Haverstock'
RETURN c, w
"""

"""
MATCH (a:application)-[:belongs_to]->(w:ward), (a)-[:is_a]->(t:application_type)
WHERE left(a._approved_date, 4) == 2008
RETURN w, a, t
"""

"""
MATCH (a:application)-[:belongs_to]->(w:ward), (a)-[:is_a]->(t:application_type), (c:company)-[reg:is_registered_at]->(w)
WHERE right(a._approved_date, 4) = '2008'
  AND reg._from <= '2008' AND (reg._until >= '2008' or reg._until = '')
RETURN w, a, t, c
"""

"""
MATCH (s)-[reg:is_registered_at|belongs_to]->(d)
WHERE (s:company AND
       reg._from <= '2008' AND (reg._until >= '2008' OR reg._until = ''))
   OR (s:application)
RETURN s, d
"""

"""
MATCH (s)-[reg:is_registered_at|belongs_to]->(d)
WHERE (s:company AND
       right(reg._from, 4) <= '2008' AND (right(reg._until, 4) >= '2008' OR right(reg._until, 4) = ''))
   OR (s:application)
RETURN s, d
"""

"""
MATCH (s)-[reg:is_registered_at|belongs_to]->(d), (s)-[:is_a|is_registered_at]->(t)
WHERE (s:company AND right(reg._from, 4) = '2008' AND t:ward AND d:ward)
   OR (s:application AND right(s._approved_date, 4) = '2008'
   AND t:application_type)
RETURN s, d, t
"""
