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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class SccMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new SccSpout());
		BoltDeclarer fixpoint = builder.setBolt("fixpoint", new SccFixPoint());
		BoltDeclarer updater = builder.setBolt("updater", new SccUpdate(), 1);
		BoltDeclarer sink = builder.setBolt("sink", new SccSink());
		/////////////////////////////////
		fixpoint.globalGrouping("spout");
		/////////////////////////////////
		updater.globalGrouping("fixpoint", "toupdater");
		fixpoint.globalGrouping("updater");
		/////////////////////////////////
		sink.globalGrouping("fixpoint", "tosink");
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(20);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("scc", conf, builder.createTopology());
	}
}
