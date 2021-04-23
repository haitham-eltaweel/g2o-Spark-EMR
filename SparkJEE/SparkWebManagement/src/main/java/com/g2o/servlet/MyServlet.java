package com.g2o.servlet;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.amazonaws.samples.RankingReportRDD;
import com.amazonaws.samples.RankingResults;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class MyServlet
 */
@WebServlet("/MyServlet")
public class MyServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

    /**
     * Default constructor. 
     */
    public MyServlet() {
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
        PrintWriter out = response.getWriter();
        out.println (
                  "<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" +" +
                      "http://www.w3.org/TR/html4/loose.dtd\">\n" +
                  "<html> \n" +
                    "<head> \n" +
                      "<meta http-equiv=\"Content-Type\" content=\"text/html; " +
                        "charset=ISO-8859-1\"> \n" +
                      "<title> Java RDD Spark With AWS Demo  </title> \n" +
                    "</head> \n" +
                    "<body> <div align='center'> \n" +
                      "<style= \"font-size=\"12px\" color='black'\"" + "\">" +
                        "Top courses resutls will be stored at the local Data Storage  <br> " +
                    "</font></body> \n" +
                  "</html>" 
                );
        
        out.close();
        
		String topNumber = request.getParameter("topNumber");
		
		RankingReportRDD rankingReport= new RankingReportRDD();
		
		rankingReport.generateRankingReport(Integer.parseInt(topNumber));			
   
		RankingResults rankingResults= new RankingResults();
        int retries = 0;
        boolean retry = false;
        int MAX_RETRIES = 6;
        int MAX_WAIT_INTERVAL = 90000; // milliseconds

        do {
            long waitTime = Math.min(getWaitTimeExp(retries), MAX_WAIT_INTERVAL);
            System.out.print("Retry wait time: "+ waitTime + "\n");

            try {
                // Wait for the result.
                Thread.sleep(waitTime);
                
                // Get the result of the asynchronous operation.
                rankingResults= rankingReport.getTopRanks();

                if (rankingResults.getStatus().equals("Completed")) {
                    retry = false;
                } else if (rankingResults.getStatus().equals("NotCompleted")) {
                    retry = true;
                } else {
                    // Some other error occurred, so stop calling the API.
                    retry = false;
                }

            } catch (IllegalArgumentException | InterruptedException e) {
                System.out.println("Error sleeping thread: " + e.getMessage());
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        } while (retry && (retries++ < MAX_RETRIES));
		
        
        System.out.println("The results are : "+rankingResults.getResults());
		
        String fileName="C:\\My_work\\Spark\\RankingResults.txt";
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        writer.write(rankingResults.getResults());
        writer.close();	
        
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
	
	 /*
     * Returns the next wait interval, in milliseconds, using an exponential
     * backoff algorithm.
     */
    public static long getWaitTimeExp(int retryCount) {
        if (0 == retryCount) {
            return 0;
        }

        long waitTime = ((long) Math.pow(2, retryCount) * 1000L);

        return waitTime;
    }

}
