package org.ellitron.titandbbenchmarks;

import org.ellitron.titandbbenchmarks.TwitterWorkloadThread;

public class TwitterWorkloadGenerator {

	public static void main(String[] args) {
		int serverNumber 		= Integer.parseInt(args[0]); // our server number
		int baseServerNumber 	= Integer.parseInt(args[1]); // base server number
		int numServers 			= Integer.parseInt(args[2]); // total number of servers used in the workload generation
		int numThreads 			= Integer.parseInt(args[3]); // total number of threads to use (spread evenly across the servers)
		double runTime 			= Double.parseDouble(args[4]); // time to run in minutes
		double streamProb		= Double.parseDouble(args[5]); // probability of stream transaction
		long totUsers			= Long.parseLong(args[6]); // total number of users in the graph
		int streamTxPgSize		= Integer.parseInt(args[7]); // the number of tweets to read in a stream
		int workingSetSize		= Integer.parseInt(args[8]); // number of nodes to operate over
		String outputDir 		= args[9];
		String datFilePrefix = null;
		if(args.length > 10)
			datFilePrefix = args[10]; // prefix for output data files
		
		// calculate number of threads to spin up on this server
		int numLocalThreads = numThreads / numServers;
		numLocalThreads += ((numThreads%numServers) > (serverNumber-baseServerNumber)) ? 1 : 0;
		
		System.out.println("Running workload...");
		
		for(int i = 0; i<numLocalThreads; i++) {
			// start up threads
			(new TwitterWorkloadThread(serverNumber, i, runTime, streamProb, totUsers, streamTxPgSize, datFilePrefix, workingSetSize, outputDir)).start();
		}
	}

}
