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

import java.util.Stack;

// Determine the strong components (strongly-connected components) of a digraph.
// Vertices v and w are in the same strong component if v is reachable from w and
// w is reachable from v.

public class StrongComponents <E extends Digraph.Edge> {
	
	// The implementation is Tarjan's algorithm, adapted from the C++ code in
	// Sedgewick's "Algorithms, Third Edition, Part Five: Graph Algorithms" (2002).
	
	public StrongComponents(Digraph<E> graph) {
		this.graph = graph;
		
		pre = new int[graph.getVertexCapacity()];
		low = new int[graph.getVertexCapacity()];
		id = new int[graph.getVertexCapacity()];
		
		for (int i = 0; i < pre.length; i++)
			pre[i] = Digraph.NO_VERTEX;
		
		stack = new Stack<Integer>();
		
		for (int v = 0; v < graph.getVertexCapacity(); v++)
			if (pre[v] == Digraph.NO_VERTEX)
				build(v);
	}
	
	public boolean isStronglyReachable(int v, int w) {
		return (id[v] == id[w]);
	}
	
	//
	
	private void build(int w) {
		int t;
		int min = low[w] = pre[w] = cnt++;
		stack.push(w);
		Digraph<E>.AdjacencyIterator it = graph.createAdjacencyIterator(w);
		for (E edge = it.begin(); !it.done(); edge = it.next()) {
			t = edge.getTo();
			if (pre[t] == Digraph.NO_VERTEX)
				build(t);
			if (low[t] < min)
				min = low[t];
		}
		if (min < low[w]) {
			low[w] = min;
			return;
		}
		do {
			id[t = stack.pop()] = scnt;
			low[t] = graph.getVertexCapacity();
		} while (t != w);
		scnt++;
	}

	private Digraph<E> graph;
	private int[] pre;
	private int[] low;
	private int[] id;
	int cnt;
	int scnt;
	Stack<Integer> stack;
}
