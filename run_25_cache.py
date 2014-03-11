import nose
import os
import sys

params = eval(os.environ['PG8000_TEST'])
params['use_cache'] = True
os.environ['PG8000_TEST'] = str(params)

nose.run()
