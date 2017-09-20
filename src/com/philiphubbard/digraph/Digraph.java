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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;


// An abstract base class for a directed graph.
// This class is a generic, parameterized by the edge class.
// Algorithms that do not need to add edges can work with this class' interface.

public abstract class Digraph<E extends Digraph.Edge> {
	
	// Constructor.  It can have edges involving vertices with indices
	// in the range from 0 to vertexCapacity - 1.  The EdgeMultiples enum
	// specifies whether the graph can have more than one edge between
	// a pair of vertices or not.
	
	public enum EdgeMultiples { ENABLED, DISABLED }
	
	public Digraph(int vertexCapacity, EdgeMultiples multiples) {
		allowMultiples = (multiples == EdgeMultiples.ENABLED);
		edges = new ArrayList<EdgeLink>(vertexCapacity);
		iterators = new ArrayList<ArrayList<WeakReference<EdgeHolder>>>(vertexCapacity);
		for (int i = 0; i < vertexCapacity; i++) {
			edges.add(null);
			iterators.add(null);
		}
	}
	
	// A constant for an index that corresponds to no vertex.
	
	public static final int NO_VERTEX = -1;
	
	// A base class for the edges of the graph.
	
	public static class Edge {
		
		// Constructor, specifying the the vertex the edge is directed to.
		// The vertex it is directed from is implicit as the argument to the
		// Digraph.addEdge() routine.
		
		public Edge(int to) {
			this.to = to;
		}
		
		// The vertex the edge is pointing to.
		
		public int getTo() {
			return to;
		}
		
		private int to;
	}
	
	// Base class for an iterator over the edges adjacent to (directed from)
	// the specified vertex.  It can be used in a loop like the following:
	// "for (Edge e = iterator.begin(); !iterator.done(); e = iterator.next())"
	
	public class AdjacencyIterator extends EdgeHolder {
		
		// Returns the first edge in the iteration.
		
		public E begin() {
			current = graph.edges.get(from);
			return (current != null) ? current.edge : null;
		}
		
		// Returns the next edge in the iteration.
		
		public E next() {
			if (current != null)
				current = current.next;
			return (current != null) ? current.edge : null;
		}
		
		// Returns true if the iteration is done.
		
		public boolean done() {
			return (current == null);
		}
		
		// The derived class function that creates this iterator should
		// ensure that the vertex is in range.
		
		protected AdjacencyIterator(Digraph<E> graph, int from) {
			this.graph = graph;
			this.from = from;
			current = null;
			
			if (graph.iterators.get(from) == null) {
				graph.iterators.set(from, new ArrayList<WeakReference<EdgeHolder>>());
			}
			else {
				// Since there is no automatic removal of weak references to iterators
				// that have become null, now is a reasonable time to try explicit removal.
				
				cleanupIterators(from);
			}
			graph.iterators.get(from).add(new WeakReference<EdgeHolder>(this));
		}
		
		private Digraph<E> graph;
		private int from;
	}
	
	// A derived class must define this function to create the iterator
	// for edges from the specified vertex.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	abstract public AdjacencyIterator createAdjacencyIterator(int from)
			throws IndexOutOfBoundsException;
	
	// An iterator over the edges the pointing out from the specified vertex,
	// returning multiple edges to the same destination vertex in an array.
	// It can be used in a loop like the following:
	// "for (ArrayList<Edge> e = iterator.begin(); !iterator.done(); e = iterator.next())"
	
	public class AdjacencyMultipleIterator extends EdgeHolder {
		// Returns the first list of edges to a common vertex in the iteration.
		
		public ArrayList<E> begin() {
			current = graph.edges.get(from);
			return matchingEdges();
		}
		
		// Returns the next list of edges to a common vertex in the iteration.
		
		public ArrayList<E> next() {
			if (current != null)
				current = current.next;
			return matchingEdges();
		}
		
		// Returns true if the iteration is done.
		
		public boolean done() {
			return (current == null);
		}
		
		// The derived class function that creates this iterator should
		// ensure that the vertex is in range.
		
		protected AdjacencyMultipleIterator(Digraph<E> graph, int from) {
			this.graph = graph;
			this.from = from;
			current = null;
			
			if (graph.iterators.get(from) == null) {
				graph.iterators.set(from, new ArrayList<WeakReference<EdgeHolder>>());
			}
			else {
				// Since there is no automatic removal of weak references to iterators
				// that have become null, now is a reasonable time to try explicit removal.
				
				cleanupIterators(from);
			}
			graph.iterators.get(from).add(new WeakReference<EdgeHolder>(this));
		}
		
		// Return the edges matching the current edge (i.e., pointing to the
		// same vertex).
		
		private ArrayList<E> matchingEdges() {
			if (current == null)
				return null;
			ArrayList<E> result = new ArrayList<E>();
			EdgeLink next = current;
			do {
				current = next;
				result.add(current.edge);
				next = current.next;
			} while ((next != null) && (next.edge.getTo() == current.edge.getTo()));
			return result;			
		}

		private Digraph<E> graph;
		private int from;
	}
	
	// A derived class must define this function to create the iterator
	// for edges from the specified vertex.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	abstract public AdjacencyMultipleIterator createAdjacencyMultipleIterator(int from)
			throws IndexOutOfBoundsException;
	
	// The graph can have vertices with indices in the range from 
	// 0 to vertexCapacity() - 1.
	
	public int getVertexCapacity() {
		return edges.size();
	}
	
	// Whether the graph can have more than one edge between
	// a pair of vertices or not.
	
	public EdgeMultiples getEdgeMultiples() {
		if (allowMultiples)
			return EdgeMultiples.ENABLED;
		else
			return EdgeMultiples.DISABLED;
	}
	
	// The number of edges directed out from the specified vertex.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	public int getOutDegree(int from) throws IndexOutOfBoundsException {
		if ((from < 0) || (getVertexCapacity() <= from))
			throw new IndexOutOfBoundsException("Digraph.outDegree() " +
											    "vertex out of range");
		cacheDegrees();
		return outDegrees.get(from);
	}
	
	// The number of edges directed in to the specified vertex.
	// Throws IndexOutOfBoundsException if the vertex is out of range.
	
	public int getInDegree(int to) throws IndexOutOfBoundsException {
		if ((to < 0) || (getVertexCapacity() <= to))
			throw new IndexOutOfBoundsException("Digraph.inDegree() " +
											    "vertex out of range");
		cacheDegrees();
		return inDegrees.get(to);
	}
	
	// Returns true if the specified vertex is a sink (i.e., it has
	// no edges pointing to other vertices).
	
	public boolean isSink(int v) {
		return (edges.get(v) == null);
	}
	
	// Remove all edges from the specified vertex to the other vertex.
	// Silently does nothing if either vertex does not exist or is
	// out of range.

	public void removeEdge(int from, int to) {
		if ((from < 0) || (getVertexCapacity() <= from) ||
				(to < 0) || (getVertexCapacity() <= to))
			return;

		EdgeLink link = edges.get(from);
		EdgeLink prev = null;
		while (link != null) {
			if (link.edge.getTo() == to) {
				// Update iterators that might be referring to the EdgeLink
				// that is about to be removed.

				cleanupIterators(from);
				ArrayList<WeakReference<EdgeHolder>> its = iterators.get(from);
				for (WeakReference<EdgeHolder> ref : its)
					ref.get().update(link);

				if (prev != null)
					prev.next = link.next;
				else
					edges.set(from, link.next);
				
				if (inDegrees != null) {
					int inDegree = inDegrees.get(to);
					inDegrees.set(link.edge.getTo(), (inDegree != -1) ? inDegree - 1 : 0);
				}
				if (outDegrees != null) {
					int outDegree = outDegrees.get(from);
					outDegrees.set(from, (outDegree != -1) ? outDegree - 1 : 0);
				}
				
				break;
			}
			else if (to < link.edge.getTo()) {
				break;
			}
			prev = link;
			link = link.next;	
		}		
	}
	
	//
	
	// Helper function for adding an edge.
	
	protected void addEdge(int from, E newEdge) {
		if ((from < 0) || (edges.size() <= from))
			return;
		if ((newEdge.getTo() < 0) || (edges.size() <= newEdge.getTo()))
			return;
		
		// Keep edges sorted by getTo() to improve average-case performance
		// and to support AdjacencyMultipleIterator.
		
		EdgeLink link = edges.get(from);
		EdgeLink prev = null;
		while (link != null) {
			if (newEdge.getTo() < link.edge.getTo()) {
				break;
			}
			else if (newEdge.getTo() == link.edge.getTo()) {
				if (allowMultiples)
					break;
				else
					return;
			}
			prev = link;
			link = link.next;	
		}
		if (prev == null)
			edges.set(from, new EdgeLink(newEdge, edges.get(from)));
		else
			prev.next = new EdgeLink(newEdge, link);

		if (inDegrees != null) {
			int inDegree = inDegrees.get(newEdge.getTo());
			inDegrees.set(newEdge.getTo(), (inDegree == -1) ? 1 : inDegree + 1);
		}
		if (outDegrees != null) {
			int outDegree = outDegrees.get(from);
			outDegrees.set(from, (outDegree == -1) ? 1 : outDegree + 1);
		}
	}
	
	//
	
	// Helper function for computing and storing the in degree and out degree
	// once for all vertices.  Since a vertex does not know the edges pointing
	// to it, a loop over all vertices is needed to compute the degree for
	// any one vertex, and it makes sense to store it for all vertices.
	
	private void cacheDegrees() {
		if ((inDegrees != null) && (outDegrees != null))
			return;
		
		inDegrees = new ArrayList<Integer>();
		outDegrees = new ArrayList<Integer>();
		for (int v = 0; v < edges.size(); v++) {
			inDegrees.add(-1);
			outDegrees.add(-1);
		}
		for (int v = 0; v < edges.size(); v++) {
			AdjacencyIterator it = createAdjacencyIterator(v);
			int outDegree = 0;
			for (E e = it.begin(); !it.done(); e = it.next()) {
				outDegree++;
				int to = e.getTo();
				int inDegree = inDegrees.get(to);
				inDegrees.set(to, (inDegree == -1) ? 1 : inDegree + 1);
				if (outDegrees.get(to) == -1)
					outDegrees.set(to, 0);
			}
			if (outDegree > 0) {
				outDegrees.set(v, outDegree);
				if (inDegrees.get(v) == -1)
					inDegrees.set(v, 0);
			}
		}
	}
	
	// Since there is no automatic removal of weak references to iterators
	// that have become null, this routine forces explicit removal.
	
	private void cleanupIterators(int vertex) {
		Iterator<WeakReference<EdgeHolder>> it = iterators.get(vertex).iterator();
		while (it.hasNext()) {
			WeakReference<EdgeHolder> ref = it.next();
			if (ref.get() == null)
				it.remove();
		}
	}
	
	// Internal representation of an edge, in a linked list of edges.
	
	private class EdgeLink {
		EdgeLink(E edge, EdgeLink next) {
			this.edge = edge;
			this.next = next;
		}
		
		E edge;
		EdgeLink next;
	}
	
	// A base class for classes that hold references to edges, like iterators.
	// Since the referred edge may be removed, this class can be updated to
	// advance past the removed edge.  This base class makes it simpler to
	// maintain a list of weak references to every instance that could need
	// to be updated.
	
	private class EdgeHolder {
		void update(EdgeLink link) {
			if ((link != null) && (link == current))
				current = current.next;
		}
		
		EdgeLink current;
	}
	
	private boolean allowMultiples;
	private ArrayList<EdgeLink> edges;
	private ArrayList<Integer> inDegrees;
	private ArrayList<Integer> outDegrees;
	private ArrayList<ArrayList<WeakReference<EdgeHolder>>> iterators;
}
