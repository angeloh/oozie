/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.Proxy.Type;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

@SuppressWarnings("deprecation")
public class YCAMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

    private JobConf conf;
    private Random val = new Random();
    private static final String PARALLEL_THREADS = "parallel.threads";
    // private static final String URL_LABEL = "https://gsbl90522.blue.ygrid.yahoo.com:4443";
    private static final String URL_WS = "http://gsbl90714.blue.ygrid.yahoo.com:4080/v1/yca/test";
    private static final String AUTHORIZATION_HEADER = "Yahoo-App-Auth";
    String ycaCertificate = "v=2;a=yahoo.griduser.httpproxy.testuser;u=strat_ci;p=98.137.97.216|98.137.97.215|67.195.100.43|67.195.100.45|67.195.100.46;t=1293221095;s=kMbas6HRxHaoCWl0QagcMlbeMQtXGSMbHr702p5lfl8Xf8l3KZ3_8RxqQWzi_DY_9k1Bme5kiI3RdAJnXNxbYQ--";

    private Semaphore tokens;
    private ExecutorService pool;

    @Override
    public void configure(JobConf jobConf) {
        conf = jobConf;
        int threads = conf.getInt(PARALLEL_THREADS, 5);
        tokens = new Semaphore(threads);
        pool = Executors.newFixedThreadPool(threads);
    }

    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> collector, Reporter reporter)
            throws IOException {
        try {
            tokens.acquire();
        }
        catch (InterruptedException ie) {
            throw new IOException(ie);
        }

        Runnable runnable = new Runnable() {
            public void run() {
                try {
                    download(URL_WS);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        pool.execute(runnable);
        collector.collect(key, value);
    }

    @Override
    public void close() throws IOException {
    }

    private void download(String fileURL) {
        long startTime = System.nanoTime();

        InputStream in = null;
        OutputStream out = null;

        SSLContext sslContext = null;

        TrustManager[] trustManager = new TrustManager[] { new TrustEverythingTrustManager() };

        String threadName = Thread.currentThread().getName();
        System.out.println(threadName + "-Executing " + fileURL);

        try {

            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustManager, new java.security.SecureRandom());

            InetSocketAddress inet = new InetSocketAddress("gsbl90718.blue.ygrid.yahoo.com", 4080);

            Proxy proxy = new Proxy(Type.HTTP, inet);

            URL server = new URL(fileURL);

            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            HttpsURLConnection con;

            con = (HttpsURLConnection) server.openConnection(proxy);
            System.out.println("Proxy : " + con.usingProxy());
            con.setHostnameVerifier(new VerifyEverythingHostnameVerifier());

            con.setRequestMethod("GET");

            con.addRequestProperty(AUTHORIZATION_HEADER, ycaCertificate);

            System.out.println(threadName + "before getinputstream");
            in = con.getInputStream();

            System.out.println(threadName + "after getinputstream");
            FileSystem fs = FileSystem.get(conf);
            Path outFile = new Path(FileOutputFormat.getOutputPath(conf), String.valueOf(val.nextLong()));
            out = fs.create(outFile);

            IOUtils.copyBytes(in, out, 4096, false);
            IOUtils.closeStream(out);
            String text = threadName + fileURL + ", FileSize: " + fs.getFileStatus(outFile).getLen()
                    + ", ELAPSED (ns): " + (System.nanoTime() - startTime);
            System.out.println(text);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (in != null)
                IOUtils.closeStream(in);
            if (out != null)
                IOUtils.closeStream(out);

            tokens.release(1);
        }
    }

}
