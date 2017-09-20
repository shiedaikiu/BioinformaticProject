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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


// A mapper and reducer for a Hadoop MapReduce algorithm to build the vertices of a
// directed graph, optionally performing several operations on all the vertices:
// * set the vertex representation, MRVertex, to include edges from other vertices
//   in addition to the standard edges to other vertices;
// * partition the vertices into branches and chains;
// * omit vertices that are likely to be errors due to having insufficient
//   coverage (i.e., insufficient edges to and from).
//
// The mapper class has a virtual function for building MRVertex instances from
// a hadoop.io.Text input.  Derived classes can define this function to do the
// building from application-specific data formats.
// The reducer class has a virtual function for building a MRVertex instance from
// a hadoop.io.BytesWritable input.  Derived classes can define this function to
// ensure that the correct class derived from MRVertex is constructed from the
// representation of the vertex.

public class MRBuildVertices {

	// Required setup.
	
	// Set up the Hadoop job for building the vertices.  The Path arguments
	// specify the input file and output directory.  Output files in the output
	// directory have the traditional Hadoop file names like "part-r-<nnnnn>"
	// where "<nnnnn>" is  a five-digit ID. 
	// If partitioning of vertices into branches and chains is enabled (see below)
	// then the output files will be in sub-directories "branch" and "chain" of the
	// output directory.
	
	public static void setupJob(Job job, Path inputPath, Path outputPath) 
			throws IOException {
		job.setJarByClass(MRBuildVertices.class);
		job.setMapperClass(MRBuildVertices.Mapper.class);
		job.setCombinerClass(MRBuildVertices.Reducer.class);
		job.setReducerClass(MRBuildVertices.Reducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		
        job.setOutputFormatClass(SequenceFileOutputFormat.class);  

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
	}
	
	// Optional setup.
	
	// Use this property with hadoop.conf.Configuration.setBoolean() to make each MRVertex
	// that gets built include edges from the other vertices.  By default, it does not
	// include these edges, and includes only edges to other vertices.
	
	public static final String CONFIG_INCLUDE_FROM_EDGES = "CONFIG_INCLUDE_FROM_EDGES";

	// Use this property with hadoop.conf.Configuration.setBoolean() to set whether the 
	// output is partitioned into separate files for branch vertices and chain vertices.
	// By default, partitioning is enabled.
	
	public static final String CONFIG_PARTITION_BRANCHES_CHAINS = "CONFIG_PARTITION_BRANCHES_CHAINS";

	// Use this property with hadoop.conf.Configuration.setInt() to set the coverage
	// used for omitting vertices that are likely to be errors.  "Coverage" is the
	// number of edges expected to point to and from each vertex.  A vertex is likely
	// to be an error (and thus is omitted) if the number of edges to it or from it
	// is less than ceiling(coverage / 2.0).  By default, the coverage is set to
	// DISABLE_COVERAGE, which means that coverage is not checked and no vertices are
	// omitted.
	
	public static final String CONFIG_COVERAGE = "CONFIG_COVERAGE";
	public static final int DISABLE_COVERAGE = -1;
	
	//
	
	// The mapper.  An input tuple has a key that is a long, the line in the input file,
	// and a value that is a hadoop.io.Text.  That value is processed by the virtual 
	// verticesFromInputValue() function, which returns a list of MRVertex instances.  
	// A output tuple has a key that is an int, the vertex index, and a value that is a
	// hadoop.io.BytesWritable, a compact representation of a MRVertex.
	
	public static class Mapper 
	extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, BytesWritable> {

		// By default, this function assumes that the Text value is what would be produced
		// by MRVertex.toText(MRVertex.EdgeFormat.EDGES_TO).  Derived classes can override this
		// function to take a Text value of a different format, as long as it is convertable
		// to one or more MRVertex instances.
		
		protected ArrayList<MRVertex> verticesFromInputValue(Text value, Configuration config) {
			ArrayList<MRVertex> result = new ArrayList<MRVertex>();
			result.add(new MRVertex(value, config));
			return result;
		}
				
		// The actual mapping function.
		
		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			ArrayList<MRVertex> vertices = verticesFromInputValue(value, context.getConfiguration());
			for (MRVertex vertex : vertices) {
				context.write(new IntWritable(vertex.getId()), 
							  vertex.toWritable(MRVertex.EdgeFormat.EDGES_TO));
				
				MRVertex.AdjacencyIterator itTo = vertex.createToAdjacencyIterator();
				for (int toId = itTo.begin(); !itTo.done(); toId = itTo.next()) {
					MREdge edge = new MREdge(vertex.getId(), toId);
					context.write(new IntWritable(toId), edge.toWritable());
				}
			}
		}
	}

	// The reducer.  The input tuple has a key that is an int, the index of a vertex, and
	// a value that is list of hadoop.io.BytesWritable instances, which can be a description
	// of the MRVertex with that index, or MREdges pointing to the vertex with that index.  
	// The reducer merges all the descriptions of the vertex and the edges to produce a final
	// description of the vertex, possibly writing the final vertex to different files
	// depending on whether it is a branch or a chain, or not writing it at all of coverage
	// determines that the vertex is likely to be an error.  The output tuple has a key that 
	// is an int, the index of the vertex, and a value that is a hadoop.io.BytesWritable, 
	// describing the final MRVertex.
	
	public static class Reducer 
	extends org.apache.hadoop.mapreduce.Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
		
		// By default, this function builds an instance of the MRVertex base class from the
		// BytesWritable.  Derived classes can override this function to instead build an
		// instance of a class derived from MRVertex.
				
		protected MRVertex createMRVertex(BytesWritable value, Configuration config) {
			return new MRVertex(value, config);
		}
		
		// The actual reducing function.
		
		@Override
		protected void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			Configuration config = context.getConfiguration();
			int coverage = config.getInt(CONFIG_COVERAGE, DISABLE_COVERAGE);
			boolean includeFromEdges = config.getBoolean(CONFIG_INCLUDE_FROM_EDGES, false);
			boolean partition = config.getBoolean(CONFIG_PARTITION_BRANCHES_CHAINS, true);

			ArrayList<MREdge> edges = new ArrayList<MREdge>();
			MRVertex vertex = null;
			
			for (BytesWritable value : values) {
				
				if (MRVertex.getIsMRVertex(value)) {
					if (vertex == null) {
						// If we have not yet built a vertex for the index, do so.
						vertex = createMRVertex(value, config);
					}
					else {
						// If we have built a vertex, then the vertex we just got is
						// another representation of part of that vertex, made by another
						// mapper.  So merge it into the vertex we already built.
						
						vertex.merge(createMRVertex(value, config));
					}
				}
				else if (MREdge.getIsMREdge(value)) {
					// If we found an edge, save it for later merging into the vertex.
					
					edges.add(new MREdge(value));
				}
			}
			
			if (vertex != null) {
				for (MREdge edge : edges)
					vertex.addEdge(edge);
				
				if (coverage != DISABLE_COVERAGE) {
					boolean sufficientlyCoveredFrom = 
							removeUndercoveredEdges(vertex, Which.FROM, coverage);
					boolean sufficientlyCoveredTo = 
							removeUndercoveredEdges(vertex, Which.TO, coverage);
					
					// Do not output a vertex that is likely to be an error.
					
					if (!sufficientlyCoveredFrom && !sufficientlyCoveredTo)
						return;
				}
				
				vertex.computeIsBranch();
				vertex.computeIsSourceSink();
				
				MRVertex.EdgeFormat format = includeFromEdges ? MRVertex.EdgeFormat.EDGES_TO_FROM : 
					MRVertex.EdgeFormat.EDGES_TO;
				BytesWritable value = vertex.toWritable(format);
				
				if (partition) {
					if (MRVertex.getIsBranch(value))
						multipleOutputs.write(key, value, "branch/part");
					else
						multipleOutputs.write(key, value, "chain/part");					
				}
				else {
					context.write(key, value);
				}
			}
			
		}
		
		//
		
		// The Hadoop framework automatically calls this function before running the mapping
		// and reducing.
		
		@Override
		protected void setup(Context context)
	            throws IOException, InterruptedException {
			if (context.getConfiguration().getBoolean(CONFIG_PARTITION_BRANCHES_CHAINS, true))
				multipleOutputs = new MultipleOutputs<IntWritable, BytesWritable>(context);
		}

		// The Hadoop framework automatically calls this function after running the mapping
		// and reducing.
		
		@Override
		protected void cleanup(Context context)
	            throws IOException, InterruptedException {
			if (context.getConfiguration().getBoolean(CONFIG_PARTITION_BRANCHES_CHAINS, true))
				multipleOutputs.close();
		}
		
		// 
		
		// A helper function that returns true if coverage indicates that the vertex is
		// likely an error and should be omitted.  The coverage analysis is symmetrical
		// for the edges from other vertices to this vertex, and the edges to other vertices
		// from this vertex, so which case to process is an argument.
		
		private enum Which { FROM, TO };
		
		private boolean removeUndercoveredEdges(MRVertex vertex, Which which, int coverage) {
			int minCoverage = (int) Math.ceil(coverage / 2.0);
			
			MRVertex.AdjacencyMultipleIterator it = (which == Which.FROM) ? 
					vertex.createFromAdjacencyMultipleIterator() :
						vertex.createToAdjacencyMultipleIterator();
					
			int numSufficientlyCovered = 0;
			ArrayList<Integer> others = it.begin();
			while (!it.done()) {
				ArrayList<Integer> nexts = it.next();
				if (others.size() < minCoverage) {
					int other = others.get(0);
					for (int i = 0; i < others.size(); i++) {
						if (which == Which.FROM)
							vertex.removeEdgeFrom(other);
						else
							vertex.removeEdgeTo(other);
					}
				}
				else {
					numSufficientlyCovered++;
				}
				others = nexts;
			}
			
			return (numSufficientlyCovered > 0);
		}
		
		private MultipleOutputs<IntWritable, BytesWritable> multipleOutputs = null;

	}
    
}
