package com.twitter.fetch;

import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.fetch.TweetsData;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public final class TweetsGet 
{
	LinkedBlockingQueue<TweetsData> queue;
	String[] keywords = {"#Salman", "#Bail4SalmanNot4Saints","#SalmanGuilty"};
	
	public TweetsGet(LinkedBlockingQueue<TweetsData> queue)
	{
		this.queue = queue;
	}
	
    public void streamTweets()throws TwitterException 
    {
    	//just fill this
    	 ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.setDebugEnabled(true)
           .setOAuthConsumerKey("deGvJNAbPQZtEHO3IPY4woB33")
           .setOAuthConsumerSecret("MOUYRi40ueN6g7Ik5SZPZJ7IBuRPlmXpZ8BOffiyFjudd8Yec4")
           .setOAuthAccessToken("63542770-PYeonIeKKUB52pEHNUmudnSrnl0HLh6nLq0jf2Pnx")
           .setOAuthAccessTokenSecret("i9xTQjHKH5TaccH7LhNJfdxy8j9gwgrobKkRL3cXcUdTI");
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        
        StatusListener listener = new StatusListener() 
        {
            public void onStatus(Status status) 
            {
            	if("en".equals(status.getLang()))
            	{
            	
            		TweetsData td = new TweetsData();
            		if(status.getGeoLocation()!=null)
            		{
            			td.setLatitude(status.getGeoLocation().getLatitude());
            			td.setLongitude(status.getGeoLocation().getLongitude());
            		}
            		else
            		{
            			td.setLatitude(0.0);
            			td.setLongitude(0.0);
            		}
            		td.setCreatedAt(new java.sql.Timestamp(status.getCreatedAt().getTime()));
            		td.setTweet(status.getText().replaceAll(",", " "));
            		queue.add(td);
            	}
            	
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) 
            {
                //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) 
            {
               // System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            public void onScrubGeo(long userId, long upToStatusId)
            {
                //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning warning) 
            {
                //System.out.println("Got stall warning:" + warning);
            }

            public void onException(Exception ex) 
            {
                ex.printStackTrace();
            }
        };
        FilterQuery fq = new FilterQuery();
        fq.track(keywords);
        twitterStream.addListener(listener);
        twitterStream.filter(fq);
    }
    
}