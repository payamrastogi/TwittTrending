package com.writer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.fetch.TweetsData;
import com.worker.main.UploadWorker;

public class TweetsWriter
{
	LinkedBlockingQueue<TweetsData> queue;
	AtomicInteger count = new AtomicInteger(0);
	FileOutputStream fos;
	BufferedOutputStream bos;
	File file;
	
	public TweetsWriter(LinkedBlockingQueue<TweetsData> queue)
	{
		this.queue = queue;
	}
	
	public void tweetsWriter()
	{
		while(true)
		{
			try
			{
				TweetsData td = null;
				td = queue.poll(10, TimeUnit.SECONDS);
				StringBuffer sb = new StringBuffer();
        		sb.append(td.getCreatedAt()).append(", ")
        		.append(td.getTweet()).append(", ")
        		.append(td.getLatitude()).append(", ")
        		.append(td.getLongitude());
        		this.fileWrite(sb);
			}
			catch(InterruptedException ioe )
			{
				ioe.printStackTrace();
			}
		}	
	}
	
	public void fileWrite(StringBuffer sb)
	{
		try
		{
			file = new File("output-"+count);
			fos = new FileOutputStream(file, true);
			bos = new BufferedOutputStream(fos);
			bos.write(("\n"+sb.toString()).getBytes());
			bos.close();
			fos.close();
			if(file.length()>=1024000)
			{
				
				Thread thread = new Thread(new UploadWorker(file.getName(), file.getName()));
				thread.start();
				count.getAndIncrement();
			}
		
		}
		catch(IOException ioe)
		{
			ioe.printStackTrace();
		}
	}
}
