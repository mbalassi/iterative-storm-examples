/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.iterative.storm.examples.scc;

import java.util.LinkedList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SccUpdate extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

	@Override
	public void execute(Tuple input) {
		int node =  input.getIntegerByField("node");
		LinkedList<Integer> neighbors=(LinkedList<Integer>) input.getValueByField("neighbors");
		///////compute here!////////
		int min=Integer.MAX_VALUE;
		for(int i=0; i<neighbors.size(); ++i){
			if(neighbors.get(i)<min){
				min=neighbors.get(i);
			}
		}
		////////////////////////////
		_collector.emit(new Values(node, min));
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("node", "min"));
	}

}
