package com.amazonaws.samples;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;



public class RankingReportRDD {
	
	public void generateRankingReport(int topValue)
	{
		
		AWSCredentials credentials_profile = null;		
		try {
			credentials_profile = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                    "Make sure that the credentials file exists and the profile name is specified within it.",
                    e);
        }
		
		AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
			.withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
			.withRegion(Regions.US_EAST_2)
			.build();


	    // Run a custom jar file as a step
	    HadoopJarStepConfig hadoopConfig1 = new HadoopJarStepConfig()
	       .withJar("command-runner.jar") 
	       .withArgs("spark-submit","--executor-memory","1g","--class","com.virtualpairprogrammers.Main","s3://s3-spark-demo/spark3.jar",Integer.toString(topValue));	 
	    StepConfig myCustomJarStep = new StepConfig().withName("Spark Step").withHadoopJarStep(hadoopConfig1).withActionOnFailure("CONTINUE");

	    AddJobFlowStepsResult result = emr.addJobFlowSteps(new AddJobFlowStepsRequest().withJobFlowId("j-1P8FOJKSBO702").withSteps(myCustomJarStep));
	    
          System.out.println("generateRankingReport is completed with stepId : "+result.getStepIds());

		
	}
	
	public RankingResults getTopRanks()
	{
		
		AWSCredentials credentials_profile = null;		
		try {
			credentials_profile = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                    "Make sure that the credentials file exists and the profile name is specified within it.",
                    e);
        }
		
		Regions clientRegion = Regions.US_EAST_2;
        String bucketName = "s3-spark-demo";
        String stringObjKeyName = "rankingResults.txt";
        
        RankingResults rankingResults = new RankingResults();
        
        try {
        	

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            		.withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
                    .withRegion(clientRegion)
                    .build();

            // Upload a text string as a new object.
            S3Object result= s3Client.getObject(bucketName, stringObjKeyName);
            S3ObjectInputStream s3InputStream= result.getObjectContent();

            
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            for (int length; (length = s3InputStream.read(buffer)) != -1; ) {
            	outputStream.write(buffer, 0, length);
            }
            // StandardCharsets.UTF_8.name() > JDK 7
            System.out.println("getTopRanks is completed ");
            
            rankingResults.setStatus("Completed");
            rankingResults.setResults( outputStream.toString("UTF-8"));
            return rankingResults;
            
            
            // StringWriter writer = new StringWriter();
            // IOUtils.copy(s3InputStream, writer, "UTF-8");
            // return writer.toString();
            
            

 
        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            //e.printStackTrace();
            rankingResults.setStatus("NotCompleted");
            return rankingResults;
            
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        return null;
	}
	
	  /*
     * Returns the next wait interval, in milliseconds, using an exponential
     * backoff algorithm.
     */
    public static long getWaitTimeExp(int retryCount) {
        if (0 == retryCount) {
            return 0;
        }

        long waitTime = ((long) Math.pow(2, retryCount) * 100L);

        return waitTime;
    }

}
