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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
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
import eu.stratosphere.iterative.storm.examples.graph.StaticDirectedGraph;
import eu.stratosphere.iterative.storm.examples.graph.VertexCompGraph;

public class PageRankFixPoint extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

	// this is the static graph -- at least in the batch processing case.
	// may be graph, matrix, array, simple dataset.
	StaticDirectedGraph _staticGraph = null;
	// this is the solution set.
	VertexCompGraph<Double> _dataGraph = null;
	int _totalNodeCount = 0;
	int _receiveFeedbackCount = 0;
	int _iteration = 0;
	BlockingQueue<VertexCompGraph<Double>> _queue = new ArrayBlockingQueue<VertexCompGraph<Double>>(
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
				_dataGraph.setValue(sourceNode, 1.0);
				_dataGraph.setValue(targetNode, 1.0);
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
			if(_iteration>10){return;}
			int node = input.getIntegerByField("node");
			double sum = input.getDoubleByField("sum");
			System.out.println("iteration="+_iteration+", node="+node+", sum="+sum);
			_dataGraph.setValue(node, sum);

			// sync... then another round...
			if (_iteration == 10) {
				_collector.emit("tosink", new Values(node, sum));
			}

			_receiveFeedbackCount += 1;
			if (_receiveFeedbackCount % _totalNodeCount == 0) {
				try {
					_queue.put(_dataGraph);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				_dataGraph = new VertexCompGraph<Double>();
				_iteration += 1;
				System.out.println("current iteration: "+_iteration);
			}
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_staticGraph = new StaticDirectedGraph();
		_dataGraph = new VertexCompGraph<Double>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("toupdater", new Fields("node", "neighbors"));
		declarer.declareStream("tosink", new Fields("node", "result"));
	}

	private class UpdateEmitter implements Runnable {

		@Override
		public void run() {
			VertexCompGraph<Double> _dataGraph = null;
			while (true) {
				try {
					_dataGraph = _queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Iterator<Entry<Integer, Double>> iter = _dataGraph._vertices
						.entrySet().iterator();
				// iterate all vertices
				while (iter.hasNext()) {
					Map.Entry<Integer, Double> pairs = (Entry<Integer, Double>) iter
							.next();
					int node = pairs.getKey();
					// get all fromNeighbors
					if(_staticGraph._fromVertices.containsKey(node)){
						LinkedList<Double> neighborValues = new LinkedList<Double>();
						if (!_staticGraph._toVertices.containsKey(node)){
							neighborValues.add(_dataGraph._vertices.get(node));
						}
						// get values of fromNeighbors
						for (Integer neighbor : _staticGraph._fromVertices.get(node)) {
							neighborValues.add(_dataGraph._vertices.get(neighbor)/_staticGraph._toVertices.get(neighbor).size());
						}
						_collector.emit("toupdater", new Values(node, neighborValues));
					}else{
						_collector.emit("toupdater", new Values(node, new LinkedList<Double>()));
					}
				}
			}
		}
	}

}
