import storm
import json
import redis

class ResultBolt(storm.BasicBolt):
    def initialize(self, stormconf, context):
        self.conf = stormconf
        self.context = context

    def process(self, tup):
        message = json.loads(tup.values[0])
        host = self.conf.get("REDIS_HOST")
        port =int(self.conf.get("REDIS_PORT"))
        r = redis.StrictRedis(host=host, port=port, db=0)
        channel_name = "Exp " + str(message['exp_id'])
        r.publish(channel_name, json.dumps(message))


ResultBolt().run()
