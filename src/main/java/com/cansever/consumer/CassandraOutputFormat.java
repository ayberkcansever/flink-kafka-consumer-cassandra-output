package com.cansever.consumer;


import com.cansever.consumer.message.MXParser;
import com.cansever.consumer.message.MessageObject;
import com.cansever.consumer.message.XmppPacketReader;
import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.IOException;
import java.util.Date;

/**
 * User: TTACANSEVER
 */
public class CassandraOutputFormat implements OutputFormat<MessageObject> {

    private String cassandraHosts;
    private int cassandraPort;
    private String cassandraUsername;
    private String cassandraPassword;
    private String keyspace;
    private int ttl;
    private Cluster cassandraCluster;
    private Session session;
    private PreparedStatement statement;
    private PreparedStatement statementSummary;
    private XmppPacketReader packetReader;
    private XmlPullParserFactory factory;

    private static final String encryptionKey   = "gokturk  gokturk"; // 128 bit key
    private static final String initVector      = "vezir   tonyukuk"; // 16 bytes IV

    private DateTimeFormatter df;

    public CassandraOutputFormat(String cassandraHosts, int cassandraPort, String cassandraUsername,
                                 String cassandraPassword, String keyspace, int ttl) {
        this.cassandraHosts = cassandraHosts;
        this.cassandraPort = cassandraPort;
        this.cassandraUsername = cassandraUsername;
        this.cassandraPassword = cassandraPassword;
        this.keyspace = keyspace;
        this.ttl = ttl;
    }

    @Override
    public void configure(Configuration configuration) {
        df = DateTimeFormat.forPattern("yyyyMM");
        packetReader = new XmppPacketReader();
        try {
            factory = XmlPullParserFactory.newInstance(MXParser.class.getName(), null);
            factory.setNamespaceAware(true);
        } catch (XmlPullParserException e) {
            e.printStackTrace();
        }
        packetReader.setXPPFactory(factory);

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 2000)
                .setHeartbeatIntervalSeconds(60)
                .setPoolTimeoutMillis(0);

        Cluster.Builder clusterBuilder = Cluster.builder();
        for(String host : cassandraHosts.split(",")) {
            clusterBuilder.addContactPoint(host);
        }
        clusterBuilder.withPort(cassandraPort)
                .withCredentials(cassandraUsername, cassandraPassword)
                .withPoolingOptions(poolingOptions);
        cassandraCluster = clusterBuilder.build();
    }

    @Override
    public void open(int i, int i1) throws IOException {
        session = cassandraCluster.connect("\"" + keyspace + "\"");
        if(ttl > 0) {
            statement = session.prepare("INSERT INTO \"" + keyspace + "\".\"MESSAGE_HISTORY\" (message_id, username, jid, date_partition, sent_time, stanza) VALUES (?,?,?,?,?,?) USING TTL " + ttl);
        } else {
            statement = session.prepare("INSERT INTO \"" + keyspace + "\".\"MESSAGE_HISTORY\" (message_id, username, jid, date_partition, sent_time, stanza) VALUES (?,?,?,?,?,?)");
        }
        statement = statement
                .setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setRetryPolicy(new NewDowngradingConsistencyRetryPolicy(5));

        statementSummary = session.prepare("INSERT INTO \"" + keyspace + "\".\"MESSAGE_HISTORY_SUMMARY\" (username, jid, first_date_partition) VALUES (?,?,?)")
                            .setConsistencyLevel(ConsistencyLevel.QUORUM)
                            .setRetryPolicy(new NewDowngradingConsistencyRetryPolicy(5));
    }

    @Override
    public void writeRecord(MessageObject msg) throws IOException {
        try {
            Date sentDate = new Date(msg.getSentTime());
            String encryptedStanza = Encryptor.encrypt(encryptionKey, initVector, msg.getStanza());
            if(encryptedStanza != null) {
                String datePartition = df.print(msg.getSentTime()).concat("M");

                BoundStatement bindSummary = statementSummary.bind(msg.getUsername(), msg.getJid(), datePartition);
                ResultSetFuture futureSummary = session.executeAsync(bindSummary);
                Futures.addCallback(futureSummary,
                        new FutureCallback<ResultSet>() {
                            @Override public void onSuccess(ResultSet result) {

                            }
                            @Override public void onFailure(Throwable t) {

                            }
                        }, MoreExecutors.sameThreadExecutor()
                );

                BoundStatement bind = statement.bind(msg.getMsgId(), msg.getUsername(), msg.getJid(), datePartition, sentDate, encryptedStanza);
                ResultSetFuture future = session.executeAsync(bind);
                Futures.addCallback(future,
                        new FutureCallback<ResultSet>() {
                            @Override public void onSuccess(ResultSet result) {

                            }
                            @Override public void onFailure(Throwable t) {

                            }
                        }, MoreExecutors.sameThreadExecutor()
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if(cassandraCluster != null) {
            cassandraCluster.close();
        }
    }

}
