import storm
import json
import redis

class ResultBolt(storm.BasicBolt):
    def process(self, tup):
        message = json.loads(tup.values[0])
        r = redis.StrictRedis(host="bd-1-3", port="6379", db=0)
        channel_name = "Exp " + str(message['exp_id'])
        r.publish(channel_name, json.dumps(message))


ResultBolt().run()
