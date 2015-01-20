/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.spout.RedisSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class CognitiveTopology {
  public static class CognitiveBolt extends ShellBolt implements IRichBolt {

    public CognitiveBolt() {
      super("python", "cognitive.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("cogresult"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class ResultBolt extends ShellBolt implements IRichBolt {

    public ResultBolt() {
      super("python", "cogresult.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//      declarer.declare(new Fields("result"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }


  
  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();
    Map fileConf = Utils.readStormConfig();
    String host = fileConf.get("redis.server.host").toString();
    int port =  new Integer(fileConf.get("redis.server.port").toString());
    builder.setSpout("redis", new RedisSpout(host,port,"workflow"));
    builder.setBolt("cognitive", new CognitiveBolt(), 8).shuffleGrouping("redis");
    builder.setBolt("result", new ResultBolt(), 8).shuffleGrouping("cognitive");


    Config conf = new Config();
    conf.setDebug(true);
    conf.put("REDIS_HOST",host);
    conf.put("REDIS_PORT",port);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("cognitive", conf, builder.createTopology());

      Thread.sleep(300000);
      cluster.killTopology("cognitive");
      cluster.shutdown();
    }
  }
}
