package com.amazonaws.samples;

import com.amazonaws.AmazonClientException;
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

public class SubmitJob {

	public static void main(String[] args) {
		
		System.out.println("first arg is "+args[0]);
		/*AWSCredentials credentials_profile = null;		
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
	       .withArgs("spark-submit","--executor-memory","1g","--class","com.virtualpairprogrammers.Main","s3://s3-spark-demo/spark3.jar","10");	 
	    StepConfig myCustomJarStep = new StepConfig().withName("Spark Step").withHadoopJarStep(hadoopConfig1).withActionOnFailure("CONTINUE");

	    AddJobFlowStepsResult result = emr.addJobFlowSteps(new AddJobFlowStepsRequest().withJobFlowId("j-3FLUF1V0DW35S").withSteps(myCustomJarStep));
	    
          System.out.println(result.getStepIds());*/
		
	}

}
