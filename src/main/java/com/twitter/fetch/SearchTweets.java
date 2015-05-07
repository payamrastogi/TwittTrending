package com.twitter.fetch;


import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;

/**
 * @author Yusuke Yamamoto - yusuke at mac.com
 * @since Twitter4J 2.1.7
 */
public class SearchTweets {
    /**
     * Usage: java twitter4j.examples.search.SearchTweets [query]
     *
     * @param args search query
     */
    public static void main(String[] args) 
    {
    	ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
          .setOAuthConsumerKey("deGvJNAbPQZtEHO3IPY4woB33")
          .setOAuthConsumerSecret("MOUYRi40ueN6g7Ik5SZPZJ7IBuRPlmXpZ8BOffiyFjudd8Yec4")
          .setOAuthAccessToken("63542770-PYeonIeKKUB52pEHNUmudnSrnl0HLh6nLq0jf2Pnx")
          .setOAuthAccessTokenSecret("i9xTQjHKH5TaccH7LhNJfdxy8j9gwgrobKkRL3cXcUdTI");
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        try 
        {
            Query query = new Query("");
            query.setSince("2015-04-19");
            query.setUntil("2015-04-25");
            QueryResult result;
            do 
            {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) 
                {
                	if(tweet.getLang().equals("en"))
                	{
                		StringBuilder sb = new StringBuilder();
                		String tweetsTxt = tweet.getText().replaceAll(",", " ");
                		sb.append(tweet.getCreatedAt()).append(", ").append(tweetsTxt).append(", ");
                		if(tweet.getGeoLocation()!=null)
                		{
                			sb.append(tweet.getGeoLocation().getLatitude()).append(", ");
                			sb.append(tweet.getGeoLocation().getLongitude()).append(", ");
                		}
                		else
                		{
                			sb.append(tweet.getGeoLocation()).append(", ");
                			sb.append(tweet.getGeoLocation()).append(", ");
                		}
                	}
                }
            } 
            while ((query = result.nextQuery()) != null);
            
            System.exit(0);
        } 
        catch (TwitterException te) 
        {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
    }
}

