print("analyzer: started analyzer-test-ripe")

from ptocore.analyzercontext import AnalyzerContext
from functools import partial
import genobs

import datetime

ac = AnalyzerContext()

max_action_id, timespans = ac.sensitivity.basic()
ac.set_result_info(max_action_id, timespans)


sc = ac.get_spark()
sc.addPyFile('genobs.py')

mms = ac.spark_get_uploads({'complete': True,
                            'action_id': {'$lte': max_action_id},
                            'meta.format': {'$in': ac.input_formats},
                            'meta.origmeta.packets': {'$gte': 3} })

print("analyzer: generating observations")
mms.flatMap(partial(genobs.generate_rtt, ac.action_id, ac.analyzer_id)).saveToMongoDB(ac.output_url)

print("analyzer: datetime fix")
todo = list(ac.output.find({}, {'_id': 1, 'time': 1}))
for obs in todo:
    ac.output.update_one({'_id': obs['_id']}, {'$set': {'time': datetime.datetime.fromtimestamp(obs['time'])}})

print("analyzer: finished analyzer-test-ripe")
