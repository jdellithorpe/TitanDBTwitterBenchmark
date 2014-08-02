package org.ellitron.titandbbenchmarks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;

public class TwitterGraphBatchLoader {

	public static void main(String[] args) {
		long TWEETS_PER_USER = Long.parseLong(args[0]);	// number of tweets to add to each user, added in round robin order
		String edgelistFilename = args[1];
		
		// internal parameters
    	long STARTING_TWEET_TIME	= 1230800000;	// roughly seconds passed since 1970 on 2009-01-01
    	int TWEETS_PER_SECOND		= 1000;			// a single tweet takes up 1/TWEETS_PER_SECOND seconds of time
    	int NUM_DATASET_CLONES		= 1;			// how many clones of the dataset we want
    	
    	Pattern pattern = Pattern.compile("([0-9]+)");
		Matcher matcher = pattern.matcher(edgelistFilename);
		
		if(!matcher.find()) {
			System.out.println("Failed to parse filename >_<");
			return;
		}
		
		long NUM_USERS = Long.parseLong(matcher.group());
		
		if(!matcher.find()) {
			System.out.println("Failed to parse filename >_<");
			return;
		}
		
		long FRIENDS_AND_FOLLOWERS_PER_USER = Long.parseLong(matcher.group());
    	
		Configuration conf = new BaseConfiguration();
		conf.setProperty("storage.backend","cassandra");
		conf.setProperty("storage.batch-loading", true);
		conf.setProperty("storage.keyspace", "titan_rep3");
		conf.setProperty("storage.replication-factor", 3);
		conf.setProperty("storage.hostname","192.168.1.156");
		TitanGraph g = TitanFactory.open(conf);		
		
		TitanKey time = g.makeKey("time").dataType(Long.class).make();
		TitanKey text = g.makeKey("text").dataType(String.class).make();
		TitanKey id = g.makeKey("eid").dataType(Long.class).indexed(Vertex.class).make();
		
		TitanLabel follows = g.makeLabel("follows").make();
		TitanLabel tweet = g.makeLabel("tweet").sortKey(time).sortOrder(Order.DESC).make();
		TitanLabel stream = g.makeLabel("stream").sortKey(time).sortOrder(Order.DESC).make();

		Map<String, Object> userNodeProperties = new HashMap<String, Object>();
    	Map<String, Object> tweetNodeProperties = new HashMap<String, Object>();
    	Map<String, Object> tweetRelProperties = new HashMap<String, Object>();
    	Map<String, Object> streamRelProperties = new HashMap<String, Object>();
		
		BatchGraph<TitanGraph> bgraph = new BatchGraph<TitanGraph>(g, VertexIDType.NUMBER, 1000);
		
		bgraph.setVertexIdKey("eid");
		bgraph.setLoadingFromScratch(false);
		
		String tweetString = "The problem addressed here concerns a set of isolated processors, some unknown subset of which may be faulty, that communicate only by means";

		System.out.println("Creating database files for a regular graph of size " + NUM_USERS + " users and " + FRIENDS_AND_FOLLOWERS_PER_USER + " friends and followers per user with " + TWEETS_PER_USER + " tweets per user...");
		
		if(TWEETS_PER_USER>0)
			System.out.println("Creating users...");
		else
			System.out.println("Creating users and tweets...");
		
		long userCount = 0;
		long tweetCount = 0;
		
		long startTime = System.currentTimeMillis();
    	for(long userId = 0; userId < NUM_USERS; userId++) {
    		userNodeProperties.put("eid", userId);
    		
    		Vertex userNode = bgraph.addVertex(userId, userNodeProperties);
    		
    		userCount++;
    		
    		for(int i = 0; i<TWEETS_PER_USER; i++) {
    			long tweetIdOffset = (NUM_USERS * i) + userId;
    			long tweetId = NUM_USERS + tweetIdOffset;
    			long tweetTimestamp = STARTING_TWEET_TIME + (tweetIdOffset/TWEETS_PER_SECOND);

    			String tweetText = tweetString.substring(0, (int)(Math.random()*140));

    			tweetNodeProperties.put("eid", tweetId);
    			tweetNodeProperties.put("text", tweetText);
    			tweetNodeProperties.put("time", tweetTimestamp);

    			Vertex tweetNode = bgraph.addVertex(tweetId, tweetNodeProperties);
    			
    			tweetRelProperties.put("time", tweetTimestamp);
    			
    			bgraph.addEdge(null, userNode, tweetNode, "tweet", tweetRelProperties);

    			tweetCount++;
    		}
    		
    		if(userId % 1000000 == 0)
				System.out.printf("%3d%% @%.2fminutes\n", 100*userId/NUM_USERS, (System.currentTimeMillis() - startTime)/60e3);
    	}
    	long endTime = System.currentTimeMillis(); 	
    	System.out.println();
    	
    	System.out.printf("Imported %d users, %d tweets, and %d tweet relationships in %.2f minutes\n\n", userCount, tweetCount, tweetCount, (endTime - startTime)/60e3);
		
    	long numFollowsEdges = 0;
    	long numStreamEdges = 0;
    	
    	System.out.println("Creating edges...");
    	startTime = System.currentTimeMillis();
    	try {
    		BufferedReader br = new BufferedReader(new FileReader(edgelistFilename));
    		String line;
    		while ((line = br.readLine()) != null) {
    			String[] lineParts = line.split(" ");
    			long userId1 = Long.parseLong(lineParts[0]);
    			long userId2 = Long.parseLong(lineParts[1]);
    			
    			Vertex user1 = bgraph.getVertex(userId1);
    			Vertex user2 = bgraph.getVertex(userId2);
    			
    			bgraph.addEdge(null, user1, user2, "follows");
    			
    			numFollowsEdges++;

    			for(int i = 0; i<TWEETS_PER_USER; i++) {
					long tweetIdOffset = (NUM_USERS * i) + userId2;
					long tweetId = NUM_USERS + tweetIdOffset;
					long tweetTimestamp = STARTING_TWEET_TIME + (tweetIdOffset/TWEETS_PER_SECOND);
					
					Vertex tweetNode = bgraph.getVertex(tweetId);
					
					streamRelProperties.put("time", tweetTimestamp);
					
					bgraph.addEdge(null, user1, tweetNode, "stream", streamRelProperties);
					
					numStreamEdges++;
				}
    			
    			bgraph.addEdge(null, user2, user1, "follows");
    			
    			numFollowsEdges++;
    			
    			for(int i = 0; i<TWEETS_PER_USER; i++) {
					long tweetIdOffset = (NUM_USERS * i) + userId1;
					long tweetId = NUM_USERS + tweetIdOffset;
					long tweetTimestamp = STARTING_TWEET_TIME + (tweetIdOffset/TWEETS_PER_SECOND);
					
					Vertex tweetNode = bgraph.getVertex(tweetId);
					
					streamRelProperties.put("time", tweetTimestamp);
					
					bgraph.addEdge(null, user2, tweetNode, "stream", streamRelProperties);
					
					numStreamEdges++;
				}

    			if(numFollowsEdges % 1000000 == 0) {
    				System.out.printf("%3d%% @%.2fminutes\n", 100*numFollowsEdges/(FRIENDS_AND_FOLLOWERS_PER_USER*NUM_USERS), (System.currentTimeMillis() - startTime)/60e3);
    			}
    		}
    		br.close();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	endTime = System.currentTimeMillis(); 	
    	System.out.println();

    	System.out.printf("Imported %d follower relationships and %d stream relationships in %.2f minutes\n\n", numFollowsEdges, numStreamEdges, (endTime - startTime)/60e3);
    	
    	bgraph.commit();
    	bgraph.shutdown();
	}

}
