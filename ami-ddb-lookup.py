#!/usr/bin/env python
#==============================================================================
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#==============================================================================

import os
import sys
import re
import logging

try:
  import simplejson as json
except ImportError:
  import json

from boto import dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(handler)

log = logging.getLogger('ami-ddb-lookup')
log.setLevel(logging.INFO)

class FatalError(SystemExit):
    def __init__(self, reason):
        super(FatalError, self).__init__(-1)
        log.error('Failing resource: %s', reason)
        print u'{ "Reason": "%s" }' % reason

try:
  event_obj = json.loads(os.environ.get('EventProperties'))
  log.info(u"Received event: %s", json.dumps(event_obj, indent=4))
except ValueError:
  raise FatalError(u"Could not parse properties as JSON")

resource_properties = event_obj.get('ResourceProperties')
if not resource_properties:
  raise FatalError(u"ResourceProperties not found.")

# Set passed key/value variables
request_type = event_obj['RequestType']
if not request_type:
  raise FatalError(u"Event_RequestType was not valid.")

region = resource_properties.get('region')
table = resource_properties.get('table')
hash = resource_properties.get('hash')
range = resource_properties.get('range')

if request_type != 'Delete':
  # For Create and Update do lookup and return AMI ID
  try:
    amis = Table(table,connection=dynamodb2.connect_to_region(region))
    # Handle our range in a special manner, since it can be specified as 'latest'.
    if not range or range.lower() == 'latest':
      exists = amis.query_count(hash__eq=hash,consistent=True)
      if exists is not 0:
        item_range = amis.query_2(hash__eq=hash,consistent=True,reverse=True,limit=1)
        item = list(item_range)
        ami = item[0]['ami']
      else:
        raise FatalError(u"'%s' is not a valid hash from '%s' DynamoDB table." % (hash, table))
    else:
      exists = amis.has_item(hash=hash,range=int(range),consistent=True)
      if exists:
        item = amis.get_item(hash=hash,range=int(range),consistent=True)
        ami = item['ami']
      else:
        raise FatalError(u"'%s' is not a valid string or '%s' is not a valid range from '%s' DynamoDB table." % (hash, range, table))

    # Write out our successful response!
    print u'{ "PhysicalResourceId" : "%s", "Data": { "hash": "%s", "range": "%s", "region": "%s", ' \
          u'"ami": "%s" } }' % (ami, hash, range, region.lower(), ami)
  except Exception, e:
    raise FatalError(u"Sservice not configured: %s" % (str(e)))
else:
  # For Delete return nothing
    print u"{}"
