package com.worker.main;

import com.s3.upload.UploadObject;

public class UploadWorker implements Runnable
{
	private UploadObject uploadObject;
	private String keyName;
	private String uploadFileName;
	
	public UploadWorker(String keyName, String uploadFileName)
	{
		this.uploadObject = new UploadObject();
		this.uploadFileName = uploadFileName;
		this.keyName = keyName;
	}
	
	public void run()
	{
		this.uploadObject.upload(this.keyName, this.uploadFileName);
	}
}
