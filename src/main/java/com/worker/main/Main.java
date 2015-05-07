package com.worker.main;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.twitter.fetch.TweetsData;
import com.twitter.fetch.TweetsGet;
import com.writer.TweetsWriter;

public class Main
{
	public static void main(String args[])
	{
		try
		{
			LinkedBlockingQueue<TweetsData> queue = new LinkedBlockingQueue<TweetsData>();
			TweetsGet tweetsGet = new TweetsGet(queue);
			TweetsWriter tweetsWriter = new TweetsWriter(queue);
			TweetsWriterWorker tweetsWriterWorker = new TweetsWriterWorker(tweetsWriter);
			TweetsWorker tweetsWorker = new TweetsWorker(tweetsGet);
			ExecutorService executor = Executors.newFixedThreadPool(2);
			executor.submit(tweetsWorker);
			executor.submit(tweetsWriterWorker);
			executor.shutdownNow();
			executor.awaitTermination(100, TimeUnit.SECONDS);
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
		}
	}
}
