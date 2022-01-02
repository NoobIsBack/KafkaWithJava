package com.mayank.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public static void main(String[] args) {
            new TwitterProducer().run();
    }


    String consumerKey = "usVEmXusD03Wng5BHPo4Rmhhq";
    String consumerSecret = "aBNtmM67x8IDXxsbmujgyDZ66hCnOSdjSwVtuCZmIaClM1M2wX";
    String token = "1477514139815727109-oPxDdWObHCq0JkIovBZxREoF41S8ts";
    String secret = "GSIcsLz7HrqDGZrHoDn9BeVj3dAZBv9qzfBpIypLUzRpJ";
    public void run() {
        System.out.println("Set up");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
//        create a twitter client
        Client client = creataeTwitterClient(msgQueue);
        client.connect();
        System.out.println("clienti is connected");
//        kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();
//      loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = "mayank is here";
            System.out.println("inside while loop");
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                System.out.println("we have received from message: "+ msg);
                producer.send(new ProducerRecord<>
                        ("twitter_tweets", null, msg), new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            System.out.println("Something bad happened " + e);
                        }
                    }
                });
            }
        }
        System.out.println("End of application");
    }
    public Client creataeTwitterClient(BlockingQueue<String> msgQueue) {

        System.out.println("Inside client creation");
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
// Attempts to establish a connection.

    }

    public KafkaProducer createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }


}
