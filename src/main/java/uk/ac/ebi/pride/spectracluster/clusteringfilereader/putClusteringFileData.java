package uk.ac.ebi.pride.spectracluster.clusteringfilereader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import uk.ac.ebi.pride.spectracluster.clusteringfilereader.io.ClusteringFileReader;
import uk.ac.ebi.pride.spectracluster.clusteringfilereader.io.IClusterSourceReader;
import uk.ac.ebi.pride.spectracluster.clusteringfilereader.objects.ICluster;
import uk.ac.ebi.pride.spectracluster.clusteringfilereader.objects.ISpectrumReference;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ubuntu on 17-7-17.
 */
public class putClusteringFileData {

    public static void main(String[] args) {
        String tablename1 = "201504_Clusters_data";
        byte[] Cluster = Bytes.toBytes("Cluster");
        byte[] av_precursor_mz = Bytes.toBytes("Av_precursor_mz");
        byte[] av_precursor_intens = Bytes.toBytes("Av_precursor_intens");
        byte[] consensus_mz = Bytes.toBytes("Consensus_mz");
        byte[] consensus_intens = Bytes.toBytes("Consensus_intens");
        byte[] SPEC_Title = Bytes.toBytes("SPEC_Title");

        String tablename2 = "201504_clusterSPEC_data";
        byte[] SPEC = Bytes.toBytes("SPEC");
        byte[] title = Bytes.toBytes("Title");
        byte[] PrecursorMz = Bytes.toBytes("PrecursorMz");
        byte[] Charge = Bytes.toBytes("Charge");
        byte[] SimilarityScore = Bytes.toBytes("SimilarityScore");
        byte[] Species = Bytes.toBytes("Species");
        byte[] ClusterId = Bytes.toBytes("ClusterId");
        byte[] ProjectId = Bytes.toBytes("ProjectId");

        String tablename3 = "201504_SPECInfo_data";
        byte[] SpecInfo = Bytes.toBytes("SpecInfo");
        byte[] clusterId = Bytes.toBytes("ClusterId");
        byte[] projectId = Bytes.toBytes("ProjectId");

        File clustersFile = new File(args[0]);
        IClusterSourceReader reader = new ClusteringFileReader(clustersFile);
        List<ICluster> allClusters = null;
        try {
            allClusters = reader.readAllClusters();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //put the data from Cluster and Spectrum into HBase
        Configuration config = HBaseConfiguration.create();
        try {
            HTable table1 = new HTable(config, tablename1);
            HTable table2 = new HTable(config, tablename2);
            HTable table3 = new HTable(config, tablename3);
            HBaseAdmin admin = new HBaseAdmin(config);
            if (!admin.tableExists(tablename1)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tablename1);
                HColumnDescriptor columnDescriptor1 = new HColumnDescriptor(Cluster);
                HColumnDescriptor columnDescriptor2 = new HColumnDescriptor(SPEC_Title);
                tableDescriptor.addFamily(columnDescriptor1);
                tableDescriptor.addFamily(columnDescriptor2);
                admin.createTable(tableDescriptor);
            }
            if (!admin.tableExists(tablename2)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tablename2);
                HColumnDescriptor columnDescriptor1 = new HColumnDescriptor(SPEC);
                tableDescriptor.addFamily(columnDescriptor1);
                admin.createTable(tableDescriptor);
            }
            if (!admin.tableExists(tablename3)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tablename3);
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(SpecInfo);
                tableDescriptor.addFamily(columnDescriptor);
                admin.createTable(tableDescriptor);
            }


            //将数据自动提交功能关闭
            table1.setAutoFlush(false);
            table2.setAutoFlush(false);
            table3.setAutoFlush(false);

            //设置数据缓存区域
            table1.setWriteBufferSize(64 * 1024 * 1024);
            table2.setWriteBufferSize(64 * 1024 * 1024);
            table3.setWriteBufferSize(64 * 1024 * 1024);

            //put the data from Cluster and Spectrum into HBase
            Iterator<ICluster> itr = allClusters.iterator();
            while (itr.hasNext()) {
                ICluster cluster = itr.next();
                List<ISpectrumReference> allSpectrum = cluster.getSpectrumReferences();
                Iterator<ISpectrumReference> specItr = allSpectrum.iterator();

                //进行数据插入
                Put put1 = new Put(Bytes.toBytes(cluster.getId()));
                put1.addColumn(Cluster, av_precursor_mz, Bytes.toBytes(String.valueOf(cluster.getAvPrecursorMz())));
                put1.addColumn(Cluster, av_precursor_intens, Bytes.toBytes(String.valueOf(cluster.getAvPrecursorIntens())));
                put1.addColumn(Cluster, consensus_mz, Bytes.toBytes(String.valueOf(cluster.getConsensusMzValues())));
                put1.addColumn(Cluster, consensus_intens, Bytes.toBytes(String.valueOf(cluster.getConsensusIntensValues())));
                int i = 0;
                while (specItr.hasNext()) {
                    ISpectrumReference ISpec = specItr.next();
                    String[] value = ISpec.getSpectrumId().split(";");
                    String projtId = value[0];
                    put1.addColumn(SPEC_Title, Bytes.toBytes("spec[" + i + "]"), Bytes.toBytes(ISpec.getSpectrumId()));
                    i++;

                    Put put2 = new Put(Bytes.toBytes(ISpec.getSpectrumId()));
                    put2.addColumn(SPEC, title, Bytes.toBytes(String.valueOf(ISpec.getSpectrumId())));
                    put2.addColumn(SPEC, PrecursorMz, Bytes.toBytes(String.valueOf(ISpec.getPrecursorMz())));
                    put2.addColumn(SPEC, Charge, Bytes.toBytes(String.valueOf(ISpec.getCharge())));
                    put2.addColumn(SPEC, SimilarityScore, Bytes.toBytes(String.valueOf(ISpec.getSimilarityScore())));
                    put2.addColumn(SPEC, Species, Bytes.toBytes(String.valueOf(cluster.getSpecies())));
                    put2.addColumn(SPEC, ClusterId, Bytes.toBytes(cluster.getId()));
                    put2.addColumn(SPEC, ProjectId, Bytes.toBytes(projtId));
                    table2.put(put2);

                    Put put3 = new Put(Bytes.toBytes(ISpec.getSpectrumId()));
                    put3.addColumn(SpecInfo, clusterId, Bytes.toBytes(cluster.getId()));
                    put3.addColumn(SpecInfo, projectId, Bytes.toBytes(projtId));
                    table3.put(put3);
                }
                table1.put(put1);
            }
            //关闭表连接
            table1.close();
            table2.close();
            table3.close();

        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}