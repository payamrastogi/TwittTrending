package com.worker.main;

import com.writer.TweetsWriter;

public class TweetsWriterWorker implements Runnable
{
	private TweetsWriter tweetsWriter;
	
	public TweetsWriterWorker(TweetsWriter tweetsWriter)
	{
		this.tweetsWriter = tweetsWriter;
	}
	
	public void run()
	{
		this.tweetsWriter.tweetsWriter();
	}
}
