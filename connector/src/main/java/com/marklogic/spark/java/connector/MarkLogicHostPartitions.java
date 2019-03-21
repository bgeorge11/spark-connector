package com.marklogic.spark.java.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;


public class MarkLogicHostPartitions {

    private HashMap<String, MarkLogicForestPartitions> hostPartitionMap = null;
    MarkLogicHostPartitions(){
        hostPartitionMap = new HashMap<>();
    }

    void addPartition(MarkLogicPartition part){
        String hostName = part.getHost();
        MarkLogicForestPartitions forestParts = hostPartitionMap.getOrDefault(hostName, null);
        if(forestParts == null ){
            //hostName encountered for the first time
            forestParts = new MarkLogicForestPartitions();
            hostPartitionMap.put(hostName, forestParts);
        }
        forestParts.addPartition(part);
    }

    MarkLogicPartition[] getDistributedPartitions(){

        Set<String> hostSet = hostPartitionMap.keySet();
        ArrayList<ArrayList<MarkLogicPartition>> hostSplits = new ArrayList<>();
        int hostIdx = 0;
        for(String host : hostSet){
            //for each host get flattened array of partitions
            MarkLogicForestPartitions forestSplits = hostPartitionMap.get(host);
            ArrayList<MarkLogicPartition> distributedForestSplits = forestSplits.getDistributedParitionList();
            hostSplits.add(hostIdx, distributedForestSplits);
            hostIdx++;
        }

        ArrayList<MarkLogicPartition> splits = new ArrayList<>();
        boolean more = true;
        int distro = 0;
        while(more) {
            more = false;
            for (ArrayList<MarkLogicPartition> forestSplits : hostSplits) {
                splits.add(forestSplits.get(distro));
                more = more || (forestSplits.size() > (distro+1));
            }
            distro += 1;
        }

        MarkLogicPartition[] parts = new MarkLogicPartition[splits.size()];
        return splits.toArray(parts);
    }

}