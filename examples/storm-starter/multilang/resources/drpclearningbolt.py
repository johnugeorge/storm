# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import storm

class CognitiveBolt(storm.BasicBolt):
    def process(self, tup):
        word = tup.values[0] + "!!"
        storm.emit([word,tup.values[1]])
        #word = tup.values[1] + "!!"
        #storm.emit([tup.values[0],word])

'''
class CognitiveBolt(SimpleBolt):

    OUTPUT_FIELDS = ['sentence']

    def process_tuple(self, tup):
        sentence, name = tup.values
        new_sentence = "{0} says, \"{1}\"".format(name, sentence)
        self.emit((tup.values[0] + "!!",tup.values[1],), anchors=[tup])
'''

CognitiveBolt().run()
