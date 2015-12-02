package com.cloudera.fce.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ReplicationMonitor extends Configured implements Tool {

  public static class ReplicationReport {
    public final String server;
    public final ReplicationLoadSink sinkMetrics;
    public final List<ReplicationLoadSource> sourceMetrics;

    public ReplicationReport(String server, ReplicationLoadSink sinkMetrics, List<ReplicationLoadSource> sourceMetrics) {
      this.server = server;
      this.sinkMetrics = sinkMetrics;
      this.sourceMetrics = sourceMetrics;
    }

    @Override
    public String toString() {
      String report = String.format("Server => %s\n", server);
      report += String.format("  Sink Metrics:\n");
      report += String.format("    Age of last applied op => %d\n", sinkMetrics.getAgeOfLastAppliedOp());
      report += String.format("    Timestamp of last applied op => %d\n", sinkMetrics.getTimeStampsOfLastAppliedOp());

      report += String.format("  Source Metrics:\n");
      for (ReplicationLoadSource source : sourceMetrics) {
        report += String.format("    Source Peer ID => %s\n", source.getPeerID());
        report += String.format("      Age of last shipped op => %d\n", source.getAgeOfLastShippedOp());
        report += String.format("      Timestamp of last shipped op => %d\n", source.getTimeStampOfLastShippedOp());
        report += String.format("      Size of log queue => %d\n", source.getSizeOfLogQueue());
        report += String.format("      Replication lag => %d\n", source.getReplicationLag());
      }

      return report;
    }
  }

  Connection conn;

  ReplicationMonitor() {

  }

  public void initialise(String clientConfig, String user, String keytabLocation) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path(clientConfig));
    conf.set("hadoop.security.authentication", "kerberos");

    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keytabLocation);

    conn = ConnectionFactory.createConnection(conf);
  }

  public List<ReplicationReport> getReplicationReports() throws IOException {
    if (null == conn) {
      throw new IllegalStateException("HBase connection not established");
    }

    Admin admin = conn.getAdmin();

    // Get replication stats
    ClusterStatus status = admin.getClusterStatus();
    System.out.println("Cluster ID: " + status.getClusterId());
    Collection<ServerName> liveServers = status.getServers();
    System.out.println("Live servers: " + liveServers);
    List<ReplicationReport> replicationReports = new ArrayList<>();
    if (!liveServers.isEmpty()) {
      for (ServerName server : liveServers) {
        System.out.println();
        ServerLoad load = status.getLoad(server);
        ReplicationLoadSink sink = load.getReplicationLoadSink();
        List<ReplicationLoadSource> sources = load.getReplicationLoadSourceList();
        replicationReports.add(new ReplicationReport(server.getServerName(), sink, sources));
      }
    }

    return replicationReports;
  }

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.printf("Usage %s CONFIG USER KEYTAB\n", this.getClass().getSimpleName());
    }

    String config = args[0];
    String user = args[1];
    String keytab = args[2];

    int retcode = 0;
    try {
      initialise(config, user, keytab);
      System.out.println("Replication Reports");
      System.out.println("===================\n");
      List<ReplicationReport> reports = getReplicationReports();
      for (ReplicationReport report : reports) {
        System.out.println(report);
      }
    } catch (IOException e) {
      System.err.println("ERROR: " + e.getMessage());
      retcode = 1;
    }

    return retcode;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ReplicationMonitor(), args);
    System.exit(res);
  }


}
