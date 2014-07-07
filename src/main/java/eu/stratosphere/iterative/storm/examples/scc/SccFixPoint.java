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

import eu.stratosphere.iterative.storm.examples.graph.StaticUndirectedGraph;
import eu.stratosphere.iterative.storm.examples.graph.VertexCompGraph;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SccFixPoint extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

	// this is the static graph -- at least in the batch processing case.
	// may be graph, matrix, array, simple dataset.
	StaticUndirectedGraph _staticGraph = null;
	// this is the solution set.
	VertexCompGraph<Integer> _dataGraph = null;
	int _totalNodeCount = 0;
	int _receiveFeedbackCount = 0;
	int _iteration = 0;
	BlockingQueue<VertexCompGraph<Integer>> _queue = new ArrayBlockingQueue<VertexCompGraph<Integer>>(
			100);

	@Override
	public void execute(Tuple input) {
		String source = input.getSourceComponent();
		// if the tuple is from spout
		if (source.equals("spout")) {
			String tuple = input.getStringByField("data");
			// if still has data.
			if (!tuple.equals("-1")) {
				// construct the graph.
				String[] fields = tuple.split("\t");
				int sourceNode = Integer.parseInt(fields[0]);
				int targetNode = Integer.parseInt(fields[1]);
				// set the input graph.
				_staticGraph.insertEdge(sourceNode, targetNode);
				// set vertex with the component id.
				_dataGraph.setValue(sourceNode, sourceNode);
				_dataGraph.setValue(targetNode, targetNode);
			}
			// if received all input data.
			else {
				System.out.println("begin graph emitter");
				_totalNodeCount = _dataGraph._vertices.size();
				System.out.println("total node count=" + _totalNodeCount);
				try {
					_queue.put(_dataGraph);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				Thread t = new Thread(new UpdateEmitter());
				t.start();
			}
		}
		// if the tuple is from updater
		else {
			if(_iteration>1000){return;}
			int node = input.getIntegerByField("node");
			int min = input.getIntegerByField("min");
			_dataGraph.setValue(node, min);

			// sync... then another round...
			if (_iteration == 1000) {
				_collector.emit("tosink", new Values(node, min));
			}

			_receiveFeedbackCount += 1;
			if (_receiveFeedbackCount % _totalNodeCount == 0) {
				try {
					_queue.put(_dataGraph);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				_dataGraph = new VertexCompGraph<Integer>();
				_iteration += 1;
				System.out.println("current iteration: "+_iteration);
			}
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_staticGraph = new StaticUndirectedGraph();
		_dataGraph = new VertexCompGraph<Integer>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("toupdater", new Fields("node", "neighbors"));
		declarer.declareStream("tosink", new Fields("node", "result"));
	}

	private class UpdateEmitter implements Runnable {

		@Override
		public void run() {
			VertexCompGraph<Integer> _dataGraph = null;
			while (true) {
				try {
					_dataGraph = _queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Iterator<Entry<Integer, Integer>> iter = _dataGraph._vertices
						.entrySet().iterator();
				// iterate all vertices
				while (iter.hasNext()) {
					Map.Entry<Integer, Integer> pairs = (Entry<Integer, Integer>) iter
							.next();
					int node = pairs.getKey();
					// get all neighbors
					Set<Integer> neighbors = _staticGraph._vertices.get(node);
					LinkedList<Integer> neighborValues = new LinkedList<Integer>();
					// get value of itself
					neighborValues.add(_dataGraph._vertices.get(node));
					// get values of all neighbors
					for (Integer neighbor : neighbors) {
						neighborValues.add(_dataGraph._vertices.get(neighbor));
					}
					_collector.emit("toupdater", new Values(node,
							neighborValues));
				}
			}
		}
	}

}
