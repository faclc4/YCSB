/*
 * (c) 2011 Universidade do Minho. All rights reserved.
 * Written by Pedro Gomes and Nuno Carvalho.
 */

package com.yahoo.ycsb.measurements;


import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A rapper class to allow an easy access to the ResultHandler features
 */
public class ResultStorage {


    public static final int AGGREGATED_RESULTS = 0;

    public static final int PER_CLIENT_RESULTS = 1;

    public static final int BOTH = 2;


	/**
	 * The existing result handlers that measure the queries latencies
	 */
	private static List<ResultHandler> query_resultHandlers;

	/**
	 * The existing result handlers that measure the client call throughput and establishment latency
	 */
	private static List<ResultHandler> client_resultHandlers;

	/**
	 * The global result handlers for output
	 */
	private static ResultHandler globalResultHandler;


	/**
	 * The output folder
	 */
	private static String output_folder;

	/**
	 * Result printing mode. Aggregated or a per client vision of the results.
	 */
	private static int printing_mode;

    /**The given benchmark name*/
    private static String benchmark_name;

	static {

		query_resultHandlers = new CopyOnWriteArrayList<ResultHandler>();

		client_resultHandlers = new CopyOnWriteArrayList<ResultHandler>();

	}
    
    public static void configure(String benchmark_name,String output_folder, int printing_mode){

        ResultStorage.output_folder = output_folder;

        ResultStorage.printing_mode = printing_mode;

        ResultStorage.benchmark_name = benchmark_name;

        globalResultHandler = new ResultHandler(benchmark_name);

    }

	/**
	 * Aggregates all the result handlers and prints the merged output
	 */
	public static void collect_and_print() {


		if (printing_mode == AGGREGATED_RESULTS) {


			for (ResultHandler resultHandler : query_resultHandlers) {
				globalResultHandler.addResults(resultHandler);
			}

			for (ResultHandler resultHandler : client_resultHandlers) {
				globalResultHandler.addResults(resultHandler);
			}

			globalResultHandler.listDatatoFiles(output_folder, ".csv");


		} else {


			Map<String,ResultHandler> client_resultHandler_map = new TreeMap<String, ResultHandler>();
			Map<String,ResultHandler> query_resultHandler_map = new TreeMap<String, ResultHandler>();

			for (int i = 0; i < query_resultHandlers.size(); i++) {

				ResultHandler query_resultHandler = query_resultHandlers.get(i);
				ResultHandler client_stat_resultHandler = client_resultHandlers.get(i);

				query_resultHandler_map.put(query_resultHandler.test_name,query_resultHandler);
				client_resultHandler_map.put(client_stat_resultHandler.test_name,client_stat_resultHandler);


			}

			if (printing_mode == BOTH) {

				File base_folder = globalResultHandler.prepareBaseFolder(output_folder);

				int i =1;

				for (Map.Entry<String, ResultHandler> resultHandlerEntry : query_resultHandler_map.entrySet()) {



					ResultHandler client_resultHandler = new ResultHandler(benchmark_name);


					ResultHandler query_resultHandler = resultHandlerEntry.getValue();
					client_resultHandler.addResults(query_resultHandler);

					ResultHandler call_stat_resultHandler = client_resultHandler_map.get(resultHandlerEntry.getKey());
					client_resultHandler.addResults(call_stat_resultHandler);




					client_resultHandler.listDatatoClientFolder(base_folder, i + "", ".csv");

					globalResultHandler.addResults(call_stat_resultHandler);
					globalResultHandler.addResults(query_resultHandler);

					i++;
				}

				globalResultHandler.listDatatoClientFolder(base_folder, "global", ".csv");
				System.out.println();

			} else {

				File base_folder = globalResultHandler.prepareBaseFolder(output_folder);

				int i =1;

				for (Map.Entry<String, ResultHandler> resultHandlerEntry : query_resultHandler_map.entrySet()) {

					ResultHandler client_resultHandler = new ResultHandler(benchmark_name);

					ResultHandler query_resultHandler = resultHandlerEntry.getValue();
					client_resultHandler.addResults(query_resultHandler);

					ResultHandler call_stat_resultHandler = client_resultHandler_map.get(resultHandlerEntry.getKey());
					client_resultHandler.addResults(call_stat_resultHandler);

					client_resultHandler.listDatatoClientFolder(base_folder, i + "", ".csv");


					i++;
				}


//				for (int i = 0; i < query_resultHandlers.size(); i++) {
//
//					ResultHandler client_resultHandler = new ResultHandler(benchmark_name);
//					ResultHandler query_resultHandler = query_resultHandlers.get(i);
//					client_resultHandler.addResults(query_resultHandler);
//					ResultHandler call_stat_resultHandler = client_resultHandlers.get(i);
//					client_resultHandler.addResults(call_stat_resultHandler);
//
//					client_resultHandler.listDatatoClientFolder(base_folder, i + "", ".csv");
//
//				}
				System.out.println();

			}
		}
	}

	/**
	 * Returns the global data handler
	 *
	 * @return an ResultHandler object
	 */
	public static ResultHandler getGlobalResultHandler() {
		return globalResultHandler;
	}


	/**
	 * Returns an instance of an independent ResultHandler object in a synchronized manner.
	 * the instance is stored on the class and future aggregation is possible
	 *
	 * @param id the result handler id for result matching between different types of loggers
	 * @return an ResultHandler object
	 */
	public synchronized static ResultHandler getQueryResultHandlerInstance(String id) {
		ResultHandler resultHandler = new ResultHandler(id);
		query_resultHandlers.add(resultHandler);
		return resultHandler;
	}

	/**
	 * Returns an instance of an independent ResultHandler object in a synchronized manner.
	 * the instance is stored on the class and future aggregation is possible
	 *
	 * @param id the result handler id for result matching between different types of loggers
	 * @return an ResultHandler object
	 */
	public synchronized static ResultHandler getClientResultHandlerInstance(String id) {
		ResultHandler resultHandler = new ResultHandler(id);
		client_resultHandlers.add(resultHandler);
		return resultHandler;
	}


		/**
	 * Returns an instance of an independent ResultHandler object in a synchronized manner.
	 * the instance is stored on the class and future aggregation is possible
	 *
	 * @return an ResultHandler object
	 */
	public synchronized static ResultHandler getQueryResultHandlerInstance_() {
		ResultHandler resultHandler = new ResultHandler("Query" + query_resultHandlers.size());
		query_resultHandlers.add(resultHandler);
		return resultHandler;
	}

	/**
	 * Returns an instance of an independent ResultHandler object in a synchronized manner.
	 * the instance is stored on the class and future aggregation is possible
	 *
	 * @return an ResultHandler object
	 */
	public synchronized static ResultHandler getClientResultHandlerInstance_() {
		ResultHandler resultHandler = new ResultHandler("Client" + client_resultHandlers.size());
		client_resultHandlers.add(resultHandler);
		return resultHandler;
	}

	/**
	 * Returns the result handlers associated with the queries in the system
	 *
	 * @return a list of result handlers, one per executing client.
	 */
	public static List<ResultHandler> getQuery_resultHandlers() {
		return query_resultHandlers;
	}

	/**
	 * Returns the result handlers associated with the calls statistics in the system
	 *
	 * @return a list of result handlers, one per executing client.
	 */
	public static List<ResultHandler> getClient_resultHandlers() {
		return client_resultHandlers;
	}
}
