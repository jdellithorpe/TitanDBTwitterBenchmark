package org.ellitron.titandbbenchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class TwitterWorkloadThread extends Thread {
	
	public int serverNumber;
	public int threadNumber;
	public double runTime;
	public double streamProb;
	public long totUsers; 
	public int streamTxPgSize;
	public String datFilePrefix;
	public int workingSetSize;
	public String outputDir;
	
	public TwitterWorkloadThread(int serverNumber, int threadNumber, double runTime, double streamProb, long totUsers, int streamTxPgSize, String datFilePrefix, int workingSetSize, String outputDir) {
		this.serverNumber = serverNumber;
		this.threadNumber = threadNumber;
		this.runTime = runTime;
		this.streamProb = streamProb;
		this.totUsers = totUsers;
		this.streamTxPgSize = streamTxPgSize;
		this.datFilePrefix = datFilePrefix;
		this.workingSetSize = workingSetSize;
		this.outputDir = outputDir;
	}
	
	public void run() {
		try {
			Configuration conf = new BaseConfiguration();
			conf.setProperty("storage.backend","cassandra");
			conf.setProperty("storage.read-consistency-level", "ONE");
			conf.setProperty("storage.write-consistency-level", "ALL");
			conf.setProperty("storage.hostname","192.168.1.156");
			TitanGraph g = TitanFactory.open(conf);
			
			String tweetString = "The problem addressed here concerns a set of isolated processors, some unknown subset of which may be faulty, that communicate only by means";

			// stat recording
			SummaryStatistics stSumStats = new SummaryStatistics();	// for stream transaction latencies
			SummaryStatistics twSumStats = new SummaryStatistics(); // for tweet transaction latencies

			boolean enableMiniReport = false;
			SummaryStatistics miniStSumStats = new SummaryStatistics();
			long miniReportNumber = 0;
			long miniReportInterval = 2; // report interval in seconds
			
			// open file for recording individual latency measurements
			String latFileName;
			if(datFilePrefix != null)
				latFileName = datFilePrefix + "_s" + String.valueOf(serverNumber) + "_t" + String.valueOf(threadNumber) + ".lat";
			else
				latFileName = "s" + String.valueOf(serverNumber) + "_t" + String.valueOf(threadNumber) + ".lat";

			BufferedWriter latFileWriter = new BufferedWriter(new FileWriter(outputDir + "/" + latFileName));
			latFileWriter.write(String.format("%12s%12s%12s\n", "#USERID", "TXTYPE", "LATENCY"));
			
			if(enableMiniReport)
				System.out.println(String.format("%12s%12s%12s%12s%12s%12s", "thread", "time(s)", "samples", "min(ms)", "mean(ms)", "max(ms)"));
			
			// perform transactions in a loop for runTime minutes
			long threadStartTime = System.currentTimeMillis();	
			while(System.currentTimeMillis() - threadStartTime < runTime*60*1000) {
				double randDouble = Math.random();

				if(randDouble < streamProb) {
					// read a user's stream
					// select user uniformly at random from the set of all users
					long userId;
					if( workingSetSize == 0 )
						userId = (long)(Math.random()*(double)totUsers);
					else {
						userId = (long)(Math.random()*(double)workingSetSize);
						userId = userId * (long)(totUsers/workingSetSize);
					}

					int c = 0;
					long startTime = System.nanoTime();
					Vertex userNode = g.getVertices("eid", userId).iterator().next();
					Iterable<Vertex> tweetNodes = userNode.query().direction(Direction.OUT).labels("stream").limit(streamTxPgSize).vertices();
					for( Vertex v : tweetNodes )
						v.getProperty("text");
					long endTime = System.nanoTime();
					
					// output measurement to file
					latFileWriter.write(String.format("%12d%12s%12.2f\n", userId, "ST", (endTime - startTime)/1e6));

					// update real time statistics
					if(enableMiniReport) {
						miniStSumStats.addValue((endTime - startTime)/1e6);
						if((System.currentTimeMillis() - threadStartTime) > miniReportNumber*miniReportInterval*1000l) {
							System.out.println(String.format("%12d%12d%12d%12.2f%12.2f%12.2f", threadNumber, miniReportNumber*miniReportInterval, miniStSumStats.getN(), miniStSumStats.getMin(), miniStSumStats.getMean(), miniStSumStats.getMax()));
							miniStSumStats.clear();
							miniReportNumber++;
						}
					}
					
					// update stats
					stSumStats.addValue((endTime - startTime)/1e6);
				} else {
					// publish a tweet
					// select user uniformly at random from the set of all users
					long userId;
					if( workingSetSize == 0 )
						userId = (long)(Math.random()*(double)totUsers);
					else {
						userId = (long)(Math.random()*(double)workingSetSize);
						userId = userId * (long)(totUsers/workingSetSize);
					}


					long startTime = System.nanoTime();
					Vertex userNode = g.getVertices("eid", userId).iterator().next();
					Iterable<Vertex> followerNodes = userNode.query().direction(Direction.IN).labels("follows").vertices();
					
					String tweetText = tweetString.substring(0, (int)(Math.random()*140));
					long tweetTime = (new Date()).getTime()/1000l;
	    			
					Vertex tweetNode = g.addVertex(null);
					tweetNode.setProperty("text", tweetText);
					tweetNode.setProperty("time", tweetTime);
					
					Edge tweetEdge = g.addEdge(null, userNode, tweetNode, "tweet");
					tweetEdge.setProperty("time", tweetTime);
					
					for(Vertex follower : followerNodes) {
						Edge streamEdge = g.addEdge(null, follower, tweetNode, "stream");
						streamEdge.setProperty("time", tweetTime);
					}
					
					g.commit();
					long endTime = System.nanoTime();

					// output measurement to file
					latFileWriter.write(String.format("%12d%12s%12.2f\n", userId, "TW", (endTime - startTime)/1e6));

					// update stats
					twSumStats.addValue((endTime - startTime)/1e6);
				}
			}
			long threadEndTime = System.currentTimeMillis();
			
			latFileWriter.flush();
			latFileWriter.close();
			
			String datFileName;
			if(datFilePrefix != null)
				datFileName = datFilePrefix + "_s" + String.valueOf(serverNumber) + "_t" + String.valueOf(threadNumber) + ".dat";
			else
				datFileName = "s" + String.valueOf(serverNumber) + "_t" + String.valueOf(threadNumber) + ".dat";

			BufferedWriter datFileWriter = new BufferedWriter(new FileWriter(outputDir + "/" + datFileName));
			
			// write out the experiment results
			datFileWriter.write(String.format("#%15s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s%16s\n", "threadStartTime", "threadEndTime", "threadTotTime", "streamTxCount", "tweetTxCount", "totTxCount", "streamTxAvgTPS", "tweetTxAvgTPS", "streamTxTotTime", "tweetTxTotTime", "totTxTime", "streamTxAvgTime", "tweetTxAvgTime", "streamTxMinTime", "tweetTxMinTime", "streamTxMaxTime", "tweetTxMaxTime", "streamTxStdTime", "tweetTxStdTime"));
			datFileWriter.write(String.format("%16d%16d%16d%16d%16d%16d%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f%16.2f\n", threadStartTime, threadEndTime, (threadEndTime - threadStartTime)/1000l, stSumStats.getN(), twSumStats.getN(), stSumStats.getN() + twSumStats.getN(), (double)stSumStats.getN()/((threadEndTime - threadStartTime)/1000l), (double)twSumStats.getN()/((threadEndTime - threadStartTime)/1000l), stSumStats.getSum(), twSumStats.getSum(), stSumStats.getSum() + twSumStats.getSum(), stSumStats.getMean(), twSumStats.getMean(), stSumStats.getMin(), twSumStats.getMin(), stSumStats.getMax(), twSumStats.getMax(), stSumStats.getStandardDeviation(), twSumStats.getStandardDeviation()));

			// flush and close data file
			datFileWriter.flush();
			datFileWriter.close();

		} catch(Exception e) {
			System.out.println("Error: " + e);
			return;
		}
		
		
//		Vertex v = g.addVertex(null);
//		Vertex z = g.addVertex(null);
//		v.setProperty("occupation", "johnson");
//		z.setProperty("profession", "williamson");
//		g.addEdge(null, v, z, "FOLLOWS");
//		g.addEdge(null, v, z, "FRIEND");
//		
//		//System.out.println(v);
//		
//		for( Iterator<Vertex> i = g.getVertices().iterator(); i.hasNext(); ) {
//			Vertex a = i.next();
//			System.out.println(a);
//			//a.remove();
//		}
//		
//		g.commit();
//		g.shutdown();
	}
}
