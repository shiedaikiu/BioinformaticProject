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

import java.util.ArrayList;

// A directed graph with edges that have weights.

public class WeightedDigraph extends Digraph<WeightedDigraph.Edge> {
	
	// A weighted edge.
	
	public static class Edge extends Digraph.Edge {
		public Edge(int to) {
			super(to);
			weight = 0;
		}
		
		public Edge(int to, float weight) {
			super(to);
			this.weight = weight;
		}
		
		public float getWeight() {
			return weight;
		}
		
		public void setWeight(float weight) {
			this.weight = weight;
		}
		
		private float weight;
	}
	
	// Constructor.  Vertices can be added with indices in the range
	// from 0 to vertexCapacity - 1.  The EdgeMultiples enum
	// specifies whether the graph can have more than one edge between
	// a pair of vertices or not.
	
	public WeightedDigraph(int vertexCapacity, EdgeMultiples multiples) {
		super(vertexCapacity, multiples);
	}
	
	// Add an edge from the specified vertex pointing to another vertex.
	// Silently does nothing if the specified vertex is out of range.
	
	public void addEdge(int from, Edge edge) {
		super.addEdge(from, edge);
	}
	
	// An iterator over the edges the pointing out from the specified vertex.
	// It can be used in a loop like the following:
	// "for (Edge e = iterator.begin(); !iterator.done(); e = iterator.next())"
	
	public class AdjacencyIterator 
		extends Digraph<WeightedDigraph.Edge>.AdjacencyIterator {
		
		@Override
		public Edge begin() {
			return super.begin();
		}
		
		@Override
		public Edge next() {
			return super.next();
		}

		protected AdjacencyIterator(WeightedDigraph graph, int from) {
			super(graph, from);
		}
	}
	
	// Create an iterator for the edges out from the specified vertex.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	public AdjacencyIterator createAdjacencyIterator(int from) 
			throws IndexOutOfBoundsException {
		if ((from < 0) || (getVertexCapacity() <= from))
			throw new IndexOutOfBoundsException("WeightedDigraph.createAdjacencyIterator() " +
											    "vertex out of range");
		return new AdjacencyIterator(this, from);
	}
	
	// An iterator over the edges the pointing out from the specified vertex,
	// returning multiple edges to the same destination vertex in an array.
	// It can be used in a loop like the following:
	// "for (ArrayList<Edge> e = iterator.begin(); !iterator.done(); e = iterator.next())"
	
	public class AdjacencyMultipleIterator 
		extends Digraph<WeightedDigraph.Edge>.AdjacencyMultipleIterator {
		
		@Override
		public ArrayList<Edge> begin() {
			return super.begin();
		}
		
		@Override
		public ArrayList<Edge> next() {
			return super.next();
		}
		
		protected AdjacencyMultipleIterator(WeightedDigraph graph, int from) {
			super(graph, from);
		}
	}
	
	// Create an iterator for the edges out from the specified vertex,
	// returning multiple edges to the same destination vertex in an array.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	public AdjacencyMultipleIterator createAdjacencyMultipleIterator(int from) 
			throws IndexOutOfBoundsException {
		if ((from < 0) || (getVertexCapacity() <= from))
			throw new IndexOutOfBoundsException("WeightedDigraph.createAdjacencyIterator() " +
											    "vertex out of range");
		return new AdjacencyMultipleIterator(this, from);
	}
}