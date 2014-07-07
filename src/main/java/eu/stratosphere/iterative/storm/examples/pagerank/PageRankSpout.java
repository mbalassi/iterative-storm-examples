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

package eu.stratosphere.iterative.storm.examples.pagerank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class PageRankSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	BufferedReader _br = null;
	int count=0;

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
			String line = _br.readLine();
			if (line != null) {
				while (line.startsWith("#")) {
					line = _br.readLine();
				}
				_collector.emit(new Values(line));
				++count;
			} else {
				_collector.emit(new Values("-1"));
				System.out.println("finished!!!!!!");
				while (true) {
					Utils.sleep(10000);
				}
			}
			if(count==50000){
				_collector.emit(new Values("-1"));
				System.out.println("finished!!!!!!");
				while (true) {
					Utils.sleep(10000);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		try {
			_br = new BufferedReader(new FileReader("dataset/SampleScc.txt"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("data"));
	}

}
