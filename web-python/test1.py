#!/usr/bin/env python

# init_cassandra and session
import helpers.cassandra_helper 

from routes.rest import fix_json_format
import pprint
from json import dumps

DSE_CLUSTER = '127.0.0.1'
KEYSPACE='retail'

helpers.cassandra_helper.init_cassandra(DSE_CLUSTER.split(','), KEYSPACE)

# do cassandra query
statement = "SELECT * FROM receipts_by_credit_card" \
            " WHERE credit_card_number = ?" \
            " LIMIT 1"


credit_card_number = 5383439216820189;

query = helpers.cassandra_helper.session.prepare(statement)

results = helpers.cassandra_helper.session.execute(query, [credit_card_number])

print results


print "---------"

print results[0][u'customer']
print type(results[0][u'customer'])

print "---------"

print results[0][u'customer'][0]
print results[0][u'customer']._fields


print "---------"

"""
NEED SOMETHING LIKE THIS

if hasattr(obj, '_fields'):                                                                          
	results = helpers.cassandra_helper.session.execute(query, [credit_card_number])
        return {k : getattr(obj, k) for k in obj._fields}     
"""


for field in results[0][u'customer']._fields:
    print getattr(results[0][u'customer'], field)


#thejson = dumps([description] + data, default=fix_json_format)
thejson = dumps(results[0], default=fix_json_format)

print pprint.pformat(thejson)
