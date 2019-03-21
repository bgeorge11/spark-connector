package com.marklogic.spark.java.connector;

// IMPORTANT: Do not edit. This file is generated.

import com.marklogic.client.io.Format;
import java.io.Reader;


import com.marklogic.client.DatabaseClient;

import com.marklogic.client.impl.BaseProxy;

/**
 * Provides a set of operations on the database server
 */
public interface lookupOrders {
    /**
     * Creates a lookupOrders object for executing operations on the database server.
     *
     * The DatabaseClientFactory class can create the DatabaseClient parameter. A single
     * client object can be used for any number of requests and in multiple threads.
     *
     * @param db	provides a client for communicating with the database server
     * @return	an object for session state
     */
    static lookupOrders on(DatabaseClient db) {
        final class lookupOrdersImpl implements lookupOrders {
            private BaseProxy baseProxy;

            private lookupOrdersImpl(DatabaseClient dbClient) {
                baseProxy = new BaseProxy(dbClient, "/inventory/lookUpOrders/");
            }

            @Override
            public Reader lookUpOrders(String productName) {
              return BaseProxy.TextDocumentType.toReader(
                baseProxy
                .request("lookUpOrders.sjs", BaseProxy.ParameterValuesKind.SINGLE_ATOMIC)
                .withSession()
                .withParams(
                    BaseProxy.atomicParam("productName", false, BaseProxy.StringType.fromString(productName)))
                .withMethod("POST")
                .responseSingle(false, Format.TEXT)
                );
            }

        }

        return new lookupOrdersImpl(db);
    }

  /**
   * Invokes the lookUpOrders operation on the database server
   *
   * @param productName	provides input
   * @return	as output
   */
    Reader lookUpOrders(String productName);

}
