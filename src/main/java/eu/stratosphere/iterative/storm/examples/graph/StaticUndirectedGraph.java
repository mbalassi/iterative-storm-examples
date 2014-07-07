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

package eu.stratosphere.iterative.storm.examples.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StaticUndirectedGraph {
	public Map<Integer, Set<Integer>> _vertices = null;

	public StaticUndirectedGraph() {
		_vertices = new HashMap<Integer, Set<Integer>>();
	}

	public void insertEdge(int sourceNode, int targetNode){
		if(!_vertices.containsKey(sourceNode)){
			_vertices.put(sourceNode, new HashSet<Integer>());
		}
		if(!_vertices.containsKey(targetNode)){
			_vertices.put(targetNode, new HashSet<Integer>());
		}
		_vertices.get(sourceNode).add(targetNode);
		_vertices.get(targetNode).add(sourceNode);
	}
}
