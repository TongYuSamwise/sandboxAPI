package hello;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.net.HttpURLConnection;
import java.net.URL;


import javax.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpStatus;

@RestController
public class GreetingController {

    private String sendPost(String jobID, String jobTimestamp, String taskID, String flag, String type) throws Exception {

//        String url = "http://10.190.88.204:8080/dmp-api/external/isFlagCopeSandBoxData";
        String url = "http://localhost:8080/dmp-api/external/isFlagCopeSandBoxData";
        String urlParameters = String.format("jobID=%s&jobTimestamp=%s&taskID=%s&flag=%s&type=%s", jobID, jobTimestamp, taskID, flag, type);


//
//        URL obj = new URL(url);
//        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        //添加请求头
//        con.setRequestMethod("POST");
//        con.setRequestProperty("User-Agent", USER_AGENT);
//        con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");



        //发送Post请求
//        con.setDoOutput(true);
//        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
//        wr.writeBytes(urlParameters);
//        wr.flush();
//        wr.close();

        // Send GET request
        URL obj = new URL(url+"?"+urlParameters);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.connect();

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'POST' request to URL : " + url);
        System.out.println("Post parameters : " + urlParameters);
        System.out.println("Response Code : " + responseCode);

        String result = new String();
        result = result + "\nSending 'POST' request to URL : " + url
                + "\nPost parameters : " + urlParameters
                + "\nResponse Code : " + responseCode;

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //打印结果
        System.out.println(response.toString());
        result = result + "\n" + response.toString();
        return result;
    }

    private static final String template = "Hello %s!";
    private final AtomicLong counter = new AtomicLong();

    @RequestMapping("/greeting")
    public String greeting(@RequestParam(value="name", defaultValue="World") String name) {
        return String.format(template, name);
    }

    private static final String info = "Sandbox IP: %s\nTable Name: %s";

    private synchronized int processExecute(String cmd) throws Exception {
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(cmd);
        InputStream stderr = process.getErrorStream();
        InputStreamReader isr = new InputStreamReader(stderr);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        System.out.println("<ERROR>");
        while ( (line = br.readLine()) != null)
            System.out.println(line);

        /* Process.waitFor()
         Returns the exit value of the Process object. The value 0 indicates normal termination.
         Method waitFor() is blocked until the subprocess exits.
         Contrarily, method exitValue() will immediately return the exit value,
         throw IllegalThreadStateException if the process is not terminated.
         */
        return process.waitFor();
    }

    @RequestMapping(value="/api/loadData", method=RequestMethod.POST)
    public synchronized HttpStatus loadData(HttpServletRequest request) {

        Map map = request.getParameterMap();

        Set<String> key = map.keySet();
        for (Iterator it = key.iterator(); it.hasNext();) {
            String s = (String) it.next();
            System.out.println(String.format("%s: %s", s, ((String[])map.get(s))[0]));
        }

        String srcURL = request.getParameter("sourceDataURL");
        String tableName = request.getParameter("tableName");
        String tableMeta = request.getParameter("tableMeta");
        String jobID = request.getParameter("jobID");
        String jobTimestamp = request.getParameter("jobTimestamp");
        String taskID = request.getParameter("taskID");
        boolean noRes = Boolean.parseBoolean(request.getParameter("noRes"));


        try {
            // Cut out file name, based on the rightmost slash
            String fileName = srcURL.substring(srcURL.lastIndexOf('/') + 1);

            /*----------------
             STEP 1: Copy file from s3 storage to hdfs.
             Hadoop distcp usage:
             hadoop distcp hdfs://sourcePath hdfs://destinationPath
             */
            String filePathHDFS = String.format("/user/hive/warehouse/%s/%s", jobID, fileName);
            String hadoopDistcp = String.format("/opt/hadoop/bin/hadoop distcp %s hdfs:%s", srcURL, filePathHDFS);
            System.out.println(filePathHDFS);
            System.out.println(hadoopDistcp);
            int exitVal = processExecute(hadoopDistcp);
            System.out.println(exitVal);
            String flag = "true";
            if (exitVal != 0) flag = "false";


            /*--------------------------
             STEP 2: Create hive table.
             SQL command:
             create table if not exists <tableName>(<tableMeta>) row format delimited fields terminated by ',';
             */


            PrintWriter out;
            String hiveCreateTable;
            hiveCreateTable =
                    "drop table if exists " + tableName + ";create table if not exists " + tableName + tableMeta + "row format delimited fields terminated by ',';";
            System.out.println(hiveCreateTable);
            out = new PrintWriter("./temp.ql");
            out.println(hiveCreateTable);
            out.close();
            System.out.println(
                    processExecute("/opt/hive/bin/hive -v -f ./temp.ql"));


            String hiveLoadData =
                    "load data inpath '" + filePathHDFS + "' overwrite into table " + tableName + ";";
            System.out.println(hiveLoadData);
            out = new PrintWriter("./temp.ql");
            out.println(hiveLoadData);
            out.close();
            System.out.println(
                    processExecute("/opt/hive/bin/hive -v -f ./temp.ql"));


            if (!noRes) sendPost(jobID, jobTimestamp, taskID, flag, "0");
        }

        catch (Exception e)
        {
            e.printStackTrace();
        }
        return HttpStatus.OK;
    }
    @RequestMapping(value="/api/loadResult", method=RequestMethod.POST)
    public synchronized HttpStatus loadResult(HttpServletRequest request) {
        Map map = request.getParameterMap();
        Set<String> key = map.keySet();
        for (Iterator it = key.iterator(); it.hasNext();) {
            String s = (String) it.next();
            System.out.println(String.format("%s: %s", s, ((String[])map.get(s))[0]));
        }

        String tableName = request.getParameter("tableName");
        String dstURL = request.getParameter("destinationURL");
        String jobID = request.getParameter("jobID");
        String jobTimestamp = request.getParameter("jobTimestamp");
        String taskID = request.getParameter("taskID");
        boolean noRes = Boolean.parseBoolean(request.getParameter("noRes"));

        if(!dstURL.substring(dstURL.length()-1).equals("/"))
            dstURL = dstURL + "/";

        try {

            String hiveLoadResult =
                    "drop table if exists result2fs; create table result2fs row format delimited fields terminated by ',' as select * from "+tableName+";";
            System.out.println(hiveLoadResult);
            PrintWriter out = new PrintWriter("./temp.ql");
            out.println(hiveLoadResult);
            out.close();
            System.out.println(
                    processExecute("/opt/hive/bin/hive -v -f ./temp.ql"));

            String filePathHDFS = String.format("/user/hive/warehouse/result2fs");

            // Copy file from sandbox to s3 storage.
            String hadoopDistcp = String.format("hadoop distcp -overwrite hdfs:%s %s%s", filePathHDFS, dstURL, tableName);
            System.out.println(hadoopDistcp);
            int exitVal = processExecute(hadoopDistcp);
            System.out.println(exitVal);
            String flag = "true";
            if (exitVal != 0) flag = "false";

            if (!noRes) sendPost(jobID, jobTimestamp, taskID, flag, "1");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return HttpStatus.OK;
    }
    @RequestMapping(value="/dmp-api/external/isFlagCopeSandBoxData", method=RequestMethod.GET)
    public HttpStatus check(HttpServletRequest request) {

        Map map = request.getParameterMap();

        System.out.println("Parameters list: ");
        System.out.println(request.getQueryString());

        Set<String> key = map.keySet();
        for (Iterator it = key.iterator(); it.hasNext();) {
            String s = (String) it.next();
            System.out.println(String.format("%s: %s", s, ((String[])map.get(s))[0]));
        }
        boolean noRes = Boolean.parseBoolean(request.getParameter("noRes"));
        if (noRes) System.out.println("No response: ");
        System.out.println(noRes);

        return HttpStatus.OK;
    }

}