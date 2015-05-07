package com.worker.main;

import twitter4j.TwitterException;

import com.twitter.fetch.TweetsGet;

public class TweetsWorker implements Runnable
{
	private TweetsGet tweetsGet;
	
	TweetsWorker(TweetsGet tweetsGet)
	{
		this.tweetsGet = tweetsGet;
	}
	
	public void run()
	{
		try
		{
			this.tweetsGet.streamTweets();
		}
		catch(TwitterException te)
		{
			te.printStackTrace();
		}
	}
}
