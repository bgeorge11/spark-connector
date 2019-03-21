package com.marklogic.spark.java.connector;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.expression.PlanBuilder;
import com.marklogic.client.expression.PlanBuilder.ModifyPlan;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.row.RowManager;


public class MarkLogicDataSetPartitioner  {

	public List<SparkDocument> getDocumentsFromParts(SparkContext sc, MarkLogicPartition partition) {
		String mlHost = sc.getConf().get("MarkLogic_Host");
		int mlPort = Integer.parseInt(sc.getConf().get("MarkLogic_Port"));
		String mlDbName = sc.getConf().get("MarkLogic_Database");
		String mlUser = sc.getConf().get("MarkLogic_User");
		String mlPwd = sc.getConf().get("MarkLogic_Password");
		List<SparkDocument> sparkDocs = new ArrayList<SparkDocument>();
		DatabaseClient client = DatabaseClientFactory.newClient(mlHost, mlPort, mlDbName,
				new DatabaseClientFactory.DigestAuthContext(mlUser, mlPwd));
		GenericDocumentManager docMgr = client.newDocumentManager();
		DocumentPage docPage = docMgr.read(partition.getUris());
		while (docPage.hasNext()) {
			DocumentRecord doc = docPage.next();
			sparkDocs.add(new SparkDocument(doc));
		}
		return sparkDocs;
		
	}
	
	public Dataset<String> getDataUsingTemplate(SparkSession spark, String schema, 
										String view, String sqlCondition) {
		SparkContext sc = spark.sparkContext();
		String mlHost = sc.getConf().get("MarkLogic_Host");
		int mlPort = Integer.parseInt(sc.getConf().get("MarkLogic_Port"));
		String mlDbName = sc.getConf().get("MarkLogic_Database");
		String mlUser = sc.getConf().get("MarkLogic_User");
		String mlPwd = sc.getConf().get("MarkLogic_Password");
		DatabaseClient client = DatabaseClientFactory.newClient(mlHost, mlPort, mlDbName,
				new DatabaseClientFactory.DigestAuthContext(mlUser, mlPwd));
		RowManager rowMgr = client.newRowManager();
		PlanBuilder p = rowMgr.newPlanBuilder();
		ModifyPlan plan = p.fromView(schema, view)
				   .select()
				   .where(p.sqlCondition(sqlCondition));

		StringHandle stringHandle = new StringHandle();
		stringHandle.setMimetype("text/csv");
		rowMgr.resultDoc(plan, stringHandle);
		String lines[] = stringHandle.get().split("\\n");
		List<String> rows = new ArrayList<String>();
		for(String line: lines) {
		    rows.add(line);
		}
		return spark.createDataset(rows, Encoders.STRING());
	}
	
	public MarkLogicPartition[] getPartitions(SparkContext sc, String query) {
		String mlHost = sc.getConf().get("MarkLogic_Host");
		int mlPort = Integer.parseInt(sc.getConf().get("MarkLogic_Port"));
		String mlDbName = sc.getConf().get("MarkLogic_Database");
		String mlUser = sc.getConf().get("MarkLogic_User");
		String mlPwd = sc.getConf().get("MarkLogic_Password");
        MarkLogicHostPartitions hostPartitions = new MarkLogicHostPartitions();
		DatabaseClient client = DatabaseClientFactory.newClient(mlHost, mlPort, mlDbName,
				new DatabaseClientFactory.DigestAuthContext(mlUser, mlPwd));
		DataMovementManager dmvmgr = client.newDataMovementManager();
		QueryManager qmgr = client.newQueryManager();
		RawCombinedQueryDefinition qdef = qmgr.newRawCombinedQueryDefinitionAs(Format.XML,query);
		QueryBatcher uriBatcher = dmvmgr.newQueryBatcher(qdef)
                .withConsistentSnapshot()
                .withJobName("Query Partitioning")
                .withBatchSize(100)
                .onUrisReady(batch -> {
                            //populate partition and add it to the map
                            MarkLogicPartition part = new MarkLogicPartition(batch.getJobBatchNumber(),
                                    batch.getItems(),
                                    batch.getForest().getHost(),
                                    batch.getForest().getForestName(),
                                    client.getPort(),
                                    client.getDatabase(),
                                    client.getSecurityContext());
                            hostPartitions.addPartition(part);
                        }
                )
                .onQueryFailure(exception -> {
                            exception.printStackTrace();
                        }
                );
		JobTicket uriBatcherTicket = dmvmgr.startJob(uriBatcher);
		uriBatcher.awaitCompletion();
		dmvmgr.stopJob(uriBatcherTicket);
        MarkLogicPartition[] parts = hostPartitions.getDistributedPartitions();
        return parts;
	}
}
