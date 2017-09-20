// Copyright (c) 2014 Philip M. Hubbard
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
// 
// http://opensource.org/licenses/MIT

package com.philiphubbard.digraph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Stack;

// Find the Euler tours in a directed graph, that is, the paths that visit
// each edge in a connected component once.  Uses the classic algorithm
// whose running time is linear in the number of edges.  Unlike some 
// implementations, this one does not destroy the graph in the process of 
// finding the paths.

// TODO: Does not currently check the condition that guarantees that
// Euler tours actually exist: that every vertex in the graph has an
// in degree that equals its out degree.  Processing a graph that does
// not meet this condition will have unpredictable results.  Future work
// is to add heuristics for dealing with this situation, as it could
// occur in the use of this class by Sabe, the library for genome sequence
// assembly based on Euler tours.  The result would be paths that are not
// necessarily real tours, hence the name of this class.

public class EulerPaths <E extends Digraph.Edge> {
	
	// Constructor, which computes the Euler tours.
	
	public EulerPaths(Digraph<E> graph) {
		this.graph = graph;
		
		its = new ArrayList<Digraph<E>.AdjacencyIterator>(graph.getVertexCapacity());
		traceStack = new Stack<Integer>();
		output = new ArrayList<ArrayDeque<Integer>>();

		for (int i = 0; i < graph.getVertexCapacity(); i++)
			its.add(null);
		
		for (int i = 0; i < graph.getVertexCapacity(); i++) {
			if (graph.getOutDegree(i) > 0) {
				Digraph<E>.AdjacencyIterator it = its.get(i);
				if ((it == null) || (!it.done())) {
					int v = i;
					ArrayDeque<Integer> path = new ArrayDeque<Integer>();
					output.add(path);
					path.add(v);
					
					while ((tracePath(v) == v) && (!traceStack.empty())) {
						v = traceStack.pop();
						path.addFirst(v);
					}
				}
			}
		}
	}
	
	// Return the tours computed in the constructor.
	
	public ArrayList<ArrayDeque<Integer>> getPaths() {
		return output;
	}
	
	//
	
	private int tracePath(int v) {
		while (true) {
			Digraph<E>.AdjacencyIterator it = its.get(v);
			E edge;
			if (it == null) {
				it = graph.createAdjacencyIterator(v);
				its.set(v, it);
				edge = it.begin();
			}
			else
				edge = it.next();
			if (it.done())
				break;
			
			traceStack.push(v);
			v = edge.getTo();
		}
		
		return v;
	}
	
	private Digraph<E> graph;
	private ArrayList<Digraph<E>.AdjacencyIterator> its;
	private Stack<Integer> traceStack;
	private ArrayList<ArrayDeque<Integer>> output;
}
