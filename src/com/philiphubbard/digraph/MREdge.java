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

import org.apache.hadoop.io.BytesWritable;


// An edge in a directed graph for use with Hadoop MapReduce (MR) algorithms.
// This class is not used by MRVertex to represent the vertex's edges, but can be used 
// with MRVertex, to add a new edge to a MRVertex instance, for example.

public class MREdge {

	// Returns true if the BytesWritable describes an MREdge, without actually
	// reconstructing the MREdge.
	
	public static boolean getIsMREdge(BytesWritable writable) {
		byte [] bytes = writable.getBytes();
		return (bytes[0] == WRITABLE_TYPE_ID);
	}
	
	// Construct and edge from the specified vertex index to the specified 
	// vertex index.
	
	public MREdge(int from, int to) {
		this.from = from;
		this.to = to;
	}
	
	// Construct an edge from a BytesWritable.
	
	public MREdge(BytesWritable writable) {
		byte [] bytes = writable.getBytes();
		from = getInt(bytes, 1);
		to = getInt(bytes, 5);
	}

	// Store an edge to a BytesWritable.
	
	public BytesWritable toWritable() {
		int numBytes = 1 + 4 + 4;
		byte[] result = new byte[numBytes];
		
		int i = putHeader(result);
		i = putInt(from, result, i);
		i = putInt(to, result, i);
		
		return new BytesWritable(result);
	}
	
	// Get the index of the vertex this edge points from.
	
	public int getFrom() {
		return from;
	}
	
	// Get the index of the vertex this edge poitns to.
	
	public int getTo() {
		return to;
	}
	
	//
	
	// Put into the byte array the header, for using the byte array in
	// a BytesWritable.  Returns the index at which to put the next part
	// of the description.
	
	protected int putHeader(byte[] array) {
		array[0] = WRITABLE_TYPE_ID;
		return 1;
	}
	
	//
	
	// Put into the byte array an int, for using the byte array in
	// a BytesWritable.  Returns the index at which to put the next part
	// of the description.
	
	private int putInt(int value, byte[] array, int i) {
		// 32 bits
		array[i] = (byte) ((value & 0xff000000) >>> 24);
		array[i+1] = (byte) ((value & 0xff0000) >>> 16);
		array[i+2] = (byte) ((value & 0xff00) >>> 8);
		array[i+3] = (byte) (value & 0xff);
		return i + 4;
	}
	
	// Get from the byte array an int, for a byte array that came from
	// a BytesWritable.
	
	private int getInt(byte[] array, int i) {
		int result = 0;
		result |= ((0xff & ((int) array[i])) << 24);
		result |= ((0xff & ((int) array[i+1])) << 16);
		result |= ((0xff & ((int) array[i+2])) << 8);
		result |= (0xff & ((int) array[i+3]));
		return result;
	}
	
	private static final byte WRITABLE_TYPE_ID = 2;

	private int from;
	private int to;
}
