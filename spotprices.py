#!/usr/bin/env python

import sys
import boto
import datetime as dt
from collections import defaultdict

instance_type = sys.argv[1]
start_time = dt.datetime.utcnow() - dt.timedelta(hours=1)
end_time = start_time + dt.timedelta(hours=2)

ec2 = boto.connect_ec2()
hist = ec2.get_spot_price_history(instance_type = instance_type,
                                  product_description = 'Linux/UNIX',
                                  start_time = start_time.isoformat() + 'Z',
                                  end_time = end_time.isoformat() + 'Z')
prices = defaultdict(list)
for h in hist:
    prices[h.availability_zone].append(h.price)

for zone in sorted(prices):
    ps = prices[zone]
    print('%s %.4f (%.4f..%.4f)' % (zone, sum(ps)/len(ps), min(ps), max(ps)))
