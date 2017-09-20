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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


// A mapper and reducer for a Hadoop MapReduce algorithm to compress chains
// of MRVertex instances, representing the vertices from a directed graph.  
// A chain is a sequence of vertices, V0, V1, ..., VN, where Vi has edges
// from only Vi-1 and to only Vi+1 (0 < i < N).  Compressing Vi+1 into Vi
// is implemented by the MRVertex.compressChain() routine, which makes
// Vi have edges to all the vertices Vi+1 has edges to, and calls the
// virtual function MRVertex.compressChainInternal() to allow classes derived
// from MRVertex to add their own specific compression behaviors.
// 
// The decision of which vertices to compress is made with a randomized
// algorithm.  Each vertex in the chain is assigned a random boolean value.
// If Vi is assigned true, then it is eligible for having Vi+1 compressed
// into it, but only if Vi+1 is assigned false.  This approach allows only
// compressions that will allow further compressions (e.g., it does not
// allow compressing V1 -> V2 -> V3 into V1 -> V3 and V2 -> V3 -> V4 into
// V2 -> V4, which prevents further compression).
// 
// In the MapReduce version of this randomized algorithm, the mapper gets
// a MRVertex as input and gets its output key from MRVertex.getCompressChainKey(),
// which returns the index of the vertex itself or the vertex it points to
// based on the random boolean value.  The reducer will get Vi and Vi+1 only if
// both vertices agree that Vi+1 is eligible for being compressed into Vi.
// Repeated iterations of this mapping and reducing are executed until
// M consecutive iterations are unable to perform any successful compressions,
// where M is a configuration parameter (M = 1 by default).
// 
// Both the mapper and reducer classes have a virtual function for building a 
// MRVertex instance from a hadoop.io.BytesWritable input.  Derived classes can 
// define this function to ensure that the correct class derived from MRVertex is 
// constructed from the representation of the vertex.

public class MRCompressChains {
	
	// Optional setup
	
	// Use this property with hadoop.conf.Configuration.setInt() to set the condition
	// for terminating the iterations of compression: termination should occur when
	// there are the specified number of consecutive iterations without any successful
	// compressions.  By default, the count is 1: no more iterations will be tried
	// after any iteration has no successful compressions.
	
	public static final String CONFIG_TERMINATION_COUNT = "CONFIG_TERMINATION_COUNT";
	
	// Required setup
	
	// The following routines allow the iterative compression algorithm to be controlled
	// as follows:
	//
	//     boolean keepGoing = true;
	//     MRCompressChains.beginIteration();
	//     while (keepGoing) {
	//         Job job = Job.getInstance(config);
	//         MRCompressChains.setupIterationJob(job, inputPath, outputPath);
	//         compressJob.waitForCompletion(true);
	//         keepGoing = MRCompressChains.continueIteration(job, inputPath, outputPath);
	//    }
	//
	// The inputPath is a directory containing the input files, which contain the MRVertex
	// instances for the chains to be compressed.  The outputPath is a directory in which
	// the output files will appear, containing the compressed chains.
	
	public static void beginIteration() {
		iter = 0;
		numIterWithoutCompressions = 0;
	}
	
	public static void setupIterationJob(Job job, Path inputPathOrig, Path outputPathOrig)
			throws IOException {
		job.setJarByClass(MRCompressChains.class);
		job.setMapperClass(MRCompressChains.Mapper.class);
		job.setCombinerClass(MRCompressChains.Reducer.class);
		job.setReducerClass(MRCompressChains.Reducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);  

		
		Path inputPath;
		if (iter == 0)
			inputPath = inputPathOrig;
		else
			inputPath = new Path(outputPathOrig.toString() + (iter - 1));
		Path outputPath = new Path(outputPathOrig.toString() + iter);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
	}
	
	public static boolean continueIteration(Job job, Path inputPathOrig, Path outputPathOrig) 
			throws IOException {
		FileSystem fileSystem = FileSystem.get(job.getConfiguration());

		if (iter > 0) {
			Path outputPathOld = new Path(outputPathOrig.toString() + (iter - 1));
			if (fileSystem.exists(outputPathOld))
				fileSystem.delete(outputPathOld, true);
		}

		Counters jobCounters = job.getCounters();
		long numCompressions = 
				jobCounters.findCounter(MRCompressChains.CompressionCounter.numCompressions).getValue();
		if (numCompressions == 0)
			numIterWithoutCompressions++;
		else
			numIterWithoutCompressions = 0;
		int limit = job.getConfiguration().getInt(CONFIG_TERMINATION_COUNT, 1);
		boolean keepGoing = (numIterWithoutCompressions < limit);

		if (keepGoing) {
			iter++;
		}
		else {
			Path outputPath = new Path(outputPathOrig.toString() + iter);
			fileSystem.rename(outputPath, outputPathOrig);
		}

		return keepGoing;
	}
	
	//
	
	// The mapper. The input tuple has a key that is an int, the index of a vertex, and
	// a value that is a hadoop.io.BytesWritable, the MRVertex representation of the vertex.
	// The output tuple has a key that is an int, the index of the vertex or the other vertex
	// it has an edge to, depending on the random algorithm described above.  The value of
	// the output tuple is a hadoop.io.BytesWritable, the MRVertex representation of the
	// vertex again.

	public static class Mapper 
	extends org.apache.hadoop.mapreduce.Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> {
		
		// By default, this function builds an instance of the MRVertex base class from the
		// BytesWritable.  Derived classes can override this function to instead build an
		// instance of a class derived from MRVertex.

		protected MRVertex createMRVertex(BytesWritable value, Configuration config) {
			return new MRVertex(value, config);
		}
		
		// The actual mapping function.
		
		@Override
		protected void map(IntWritable key, BytesWritable value, Context context) 
				throws IOException, InterruptedException {
			if (MRVertex.getIsBranch(value))
				throw new IOException("MRCompressChains.Mapper.map(): input vertex is a branch");

			MRVertex vertex = createMRVertex(value, context.getConfiguration());
			IntWritable keyOut = new IntWritable(vertex.getCompressChainKey(random));
			context.write(keyOut, vertex.toWritable(MRVertex.EdgeFormat.EDGES_TO));
		}
		
		private Random random = new Random();
	}
	
	// The reducer.  The input tuple has a key that is an int, the index of a vertex, and
	// a value that is list of hadoop.io.BytesWritable instances, representing the MRVertex
	// instances to be compressed.  The output tuple has a key that is an int, the index of 
	// the vertex into which the compression occurred, and a value that is a 
	// hadoop.io.BytesWritable, describing that MRVertex.

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
			MRVertex vertex1 = null;
			MRVertex vertex2 = null;
			
			Configuration config = context.getConfiguration();
			
			for (BytesWritable value : values) {
				if (MRVertex.getIsBranch(value))
					throw new IOException("MRCompressChains.Reducer.reduce(): input vertex is a branch");

				if (vertex1 == null)
					vertex1 = createMRVertex(value, config);
				else
					vertex2 = createMRVertex(value, config);
			}
			
			if (vertex1 == null)
				throw new IOException("MRCompressChains.Reducer.reduce(): insufficient input vertices");
			
			// The output key does not really matter.
			
			if (vertex2 == null) {
				IntWritable keyOut = new IntWritable(vertex1.getId());
				context.write(keyOut, vertex1.toWritable(MRVertex.EdgeFormat.EDGES_TO));
			}
			else {
				int compressionKey = key.get();
				MRVertex vertexCompressed = 
						MRVertex.compressChain(vertex1, vertex2, compressionKey);
				
				if (vertexCompressed != null) {
					IntWritable keyOut = new IntWritable(vertexCompressed.getId());
					context.write(keyOut, vertexCompressed.toWritable(MRVertex.EdgeFormat.EDGES_TO));
				
					context.getCounter(CompressionCounter.numCompressions).increment(1);
				}
				else {
					// If V1 has an edge to V3, and V2 has an edge to V3, then it is possible
					// to get V1 with key V3 and V2 with key V3.  In this case, no compression
					// is possible, but V1 and V2 must both be written as output.

					IntWritable keyOut1 = new IntWritable(vertex1.getId());
					context.write(keyOut1, vertex1.toWritable(MRVertex.EdgeFormat.EDGES_TO));
					
					IntWritable keyOut2 = new IntWritable(vertex2.getId());
					context.write(keyOut2, vertex2.toWritable(MRVertex.EdgeFormat.EDGES_TO));
				}
			}
		}
	}

	// The counter used for the termination condition, using the Hadoop convention for how a
	// counter is represented.
	
	private static enum CompressionCounter {
		numCompressions;
	}
	
	private static int iter;
	private static int numIterWithoutCompressions;
	
}
