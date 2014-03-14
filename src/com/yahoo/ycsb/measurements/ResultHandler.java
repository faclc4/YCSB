/*
 * (c) 2011 Universidade do Minho. All rights reserved.
 * Written by Pedro Gomes and Nuno Carvalho.
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.Pair;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResultHandler {


	Map<String, String> bechmark_info = null;

	String test_name;


	/**
	 * results -> for each operation store the result in(currentTime:result) *
	 */
	private HashMap<String, ArrayList<Pair<Long, Long>>> time_results;

	/**
	 * results -> for each operation store data as a list of Objects *
	 */
	private HashMap<String, ArrayList<List<Object>>> data_results;

	//Store information in 2 maps, allowing for example, the storage of several data per event,
	// like the time and place of a racer.
	private HashMap<String, HashMap<String, ArrayList<Object>>> data;

	//Data header to when printing data, should be on the printing method.
	private TreeMap<String, ArrayList<String>> dataHeader;

	//result events by type -> for each operation store the event occurrence like this (BankOperation,deposit,3)
	private HashMap<String, HashMap<String, Long>> events;

	//Abstract result set -> to store results of some kind
	private HashMap<String, Object> resulSet;

	/**
	 * @param name The name of the Benchmark
	 */
	public ResultHandler(String name) {

		this.test_name = name;

		time_results = new HashMap<String, ArrayList<Pair<Long, Long>>>();

		data_results = new HashMap<String, ArrayList<List<Object>>>();

		events = new HashMap<String, HashMap<String, Long>>();

		data = new HashMap<String, HashMap<String, ArrayList<Object>>>();

		dataHeader = new TreeMap<String, ArrayList<String>>();

		resulSet = new HashMap<String, Object>();

	}

	/**
	 * **LOG OPERATIONS****
	 */

	public void logResult(String operation, long result) {

		Pair<Long, Long> resultPair = new Pair<Long, Long>(System.currentTimeMillis(), result);
		if (!time_results.containsKey(operation)) {
			time_results.put(operation, new ArrayList<Pair<Long, Long>>());
		}
		time_results.get(operation).add(resultPair);
	}

	public void logData(String operation, List<Object> data) {

		if (!data_results.containsKey(operation)) {
			data_results.put(operation, new ArrayList<List<Object>>());
		}
		data_results.get(operation).add(data);
	}

    public void recordTimeline(String operation, long init, long end) {

        List<Object> data = new ArrayList<Object>(2);
        data.add(init);
        data.add(end);
        data.add(end-init);

		if (!data_results.containsKey(operation)) {
			data_results.put(operation, new ArrayList<List<Object>>());
		}
		data_results.get(operation).add(data);
	}

	public void countEvent(String eventType, String event, long number) {

		if (!events.containsKey(eventType)) {
			HashMap<String, Long> new_events = new HashMap<String, Long>();
			new_events.put(event, number);
			events.put(eventType, new_events);
		} else {
			if (!events.get(eventType).containsKey(event)) {
				events.get(eventType).put(event, number);
			} else {
				long count = events.get(eventType).get(event) + number;
				events.get(eventType).put(event, count);
			}
		}

	}

	public synchronized void concurrent_countEvent(String eventType, String event, long number) {

		if (!events.containsKey(eventType)) {
			HashMap<String, Long> new_events = new HashMap<String, Long>();
			new_events.put(event, number);
			events.put(eventType, new_events);
		} else {
			if (!events.get(eventType).containsKey(event)) {
				events.get(eventType).put(event, number);
			} else {
				long count = events.get(eventType).get(event) + number;
				events.get(eventType).put(event, count);
			}
		}

	}


	public void recordData(String eventType, String event, List<Object> record_data) {

		if (!data.containsKey(eventType)) {
			HashMap<String, ArrayList<Object>> data_slot = new HashMap<String, ArrayList<Object>>();
			ArrayList<Object> data_list = new ArrayList<Object>(record_data);
			data_slot.put(event, data_list);
			data.put(eventType, data_slot);
		} else {
			ArrayList<Object> data_list = new ArrayList<Object>(record_data);
			data.get(eventType).put(event, data_list);
		}

	}




	public HashMap<String, Object> getResulSet() {
		return resulSet;
	}

	public void setResulSet(HashMap<String, Object> resulSet) {
		this.resulSet = resulSet;
	}

	public void setBechmark_info(Map<String, String> bechmark_info) {
		this.bechmark_info = bechmark_info;
	}

	public Map<String, String> getBechmark_info() {
		return bechmark_info;
	}


	public HashMap<String, ArrayList<Pair<Long, Long>>> getTime_results() {
		return time_results;
	}

	public HashMap<String, ArrayList<List<Object>>> getData_results() {
		return data_results;
	}

	/**
	 * UTILITIES***
	 */

	public void cleanResults() {
		time_results.clear();
		data.clear();
		events.clear();
		System.gc();

	}

	public void setDataHeader(String EventType, ArrayList<String> dataHeader) {
		this.dataHeader.put(EventType, dataHeader);

	}

	public void addResults(ResultHandler other_results) {

		HashMap<String, HashMap<String, ArrayList<Object>>> new_data = other_results.data;

		for (String event_name : new_data.keySet()) {

			if (!this.data.containsKey(event_name)) {

				this.data.put(event_name, data.get(event_name));
			} else {

				//replaces of already existent
				for (Map.Entry<String, ArrayList<Object>> l : new_data.get(event_name).entrySet()) {
					this.data.get(event_name).put(l.getKey(), l.getValue());
				}
			}
		}


		Map<String, ArrayList<List<Object>>> new_dataResults = other_results.data_results;

		for (String event_name : new_dataResults.keySet()) {

			if (!this.data_results.containsKey(event_name)) {

				this.data_results.put(event_name, new_dataResults.get(event_name));
			} else {
				for (List<Object> l : new_dataResults.get(event_name)) {
					this.data_results.get(event_name).add(l);
				}
			}
		}


		Map<String, ArrayList<Pair<Long, Long>>> new_results = other_results.time_results;

		for (String event_name : new_results.keySet()) {
			if (!this.time_results.containsKey(event_name)) {
				this.time_results.put(event_name, new_results.get(event_name));
			} else {
				for (Pair<Long, Long> l : new_results.get(event_name)) {
					this.time_results.get(event_name).add(l);
				}
			}
		}


		Map<String, HashMap<String, Long>> new_events = other_results.events;

		for (String event_name : new_events.keySet()) {
			if (!this.events.containsKey(event_name)) {
				this.events.put(event_name, new_events.get(event_name));
			} else {
				HashMap<String, Long> new_event_count = new_events.get(event_name);
				HashMap<String, Long> this_event_count = this.events.get(event_name);
				for (String event_count_name : new_event_count.keySet()) {
					if (this_event_count.containsKey(event_count_name)) {
						this_event_count.put(event_count_name, this_event_count.get(event_count_name) + new_event_count.get(event_count_name));
					} else {
						this_event_count.put(event_count_name, new_event_count.get(event_count_name));
					}
				}
			}
		}
	}


	/**
	 * OUTPUT***
	 */


	public void listDataToSOutput() {

		System.out.println("\n\n------- RESULTS FOR: " + test_name + "-------");
		for (String dataOperation : time_results.keySet()) {
			System.out.println("OPERATION: " + dataOperation);
			ArrayList<Pair<Long, Long>> result_data = time_results.get(dataOperation);


			int total_amount = 0;
			ArrayList<Long> run_result = new ArrayList<Long>();
			for (Pair<Long, Long> res : result_data) {

				run_result.add(res.right);
				total_amount += res.right;


			}
			if (!result_data.isEmpty()) {

				System.out.println("----TOTAL RESULTS:----");
				double average = (total_amount * 1.0d) / (result_data.size() * 1.0d);
				System.out.println("Average: " + average);
				double variance = 0.0;
				long aux = 0;
				for (Pair<Long, Long> run_res : result_data) {
					aux += Math.pow((run_res.right - average), 2);
				}
				variance = aux * (1d / (result_data.size() - 1d));
				System.out.println("Variance: " + variance + "\n\n");
			}
		}
		if (!events.isEmpty()) {
			System.out.println("****EVENT COUNT****");
			for (String eventType : events.keySet()) {
				System.out.println("+EVENT TYPE: " + eventType);
				for (String event : events.get(eventType).keySet()) {
					System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));
				}
			}

		}
		if (!data.isEmpty()) {
			System.out.println("\n\n***DATA RECORDS ARE NOT SHOWN IN THIS METHOD - USE SAVE TO FILE OPTIONS****\n");
		}

	}


//    public void listDataToFile(String filename) {
//    }
//
//    public void listDataToFile(File filename) {
//    }
//
//    public void doRstatistcs(String filePerfix) {
//
//
//    }

	//use a folder per client

	public File prepareBaseFolder(String folder_name) {

		int unknown = 0;

		System.out.println("\n\n-------WRITING RESULTS FOR: " + test_name + "-------");
		File enclosing_folder = new File(folder_name);
		System.out.println("OUTPUT PATH: " + enclosing_folder.getAbsolutePath());
		if (!enclosing_folder.exists()) {
			System.out.println("RESULT DEFINED PARENT FOLDER DOES NOT EXISTS - CREATING");
			boolean created = enclosing_folder.mkdir();
			if (!created) {
				System.out.println("RESULT DEFINED PARENT FOLDER DOES NOT EXISTS AND CANT BE CREATED - TRYING ENCLOSING FOLDER");
				enclosing_folder = enclosing_folder.getParentFile();
			}

		} else if (!enclosing_folder.isDirectory()) {
			enclosing_folder = enclosing_folder.getParentFile();
			System.out.println("NOT A FOLDER: ENCLOSING FOLDER USED -> " + enclosing_folder);
		}

		GregorianCalendar date = new GregorianCalendar();
		String suffix = date.get(GregorianCalendar.YEAR) + "_" + (date.get(GregorianCalendar.MONTH) + 1) + "_" + date.get(GregorianCalendar.DAY_OF_MONTH) + "_" + date.get(GregorianCalendar.HOUR_OF_DAY) + "_" + date.get(GregorianCalendar.MINUTE) + "";

		File folder = new File(enclosing_folder.getAbsolutePath() + "/" + test_name + suffix);

		if (!folder.exists()) {
			boolean created = folder.mkdir();
			if (!created) {
				System.out.println("RESULT FOLDER DOES NOT EXISTS AND CANT BE CREATED - USING EXECUTION FOLDER");
				File exe_folder = new File("./Results" + "/" + test_name + suffix);
				if (!exe_folder.exists()) {
					created = exe_folder.mkdir();
					if (!created) {
						System.out.println("EXECUTION FOLDER CANT BE USED, LEAVING...");
						return null;
					} else {
						folder = exe_folder;
					}
				}
			}
		}
		System.out.println("OUTPUT FOLDER: " + folder.getName() + "\n");
		return folder;
	}

	public void listDatatoClientFolder(File folder_name, String client_folder, String extension) {

		int unknown = 0;

		System.out.print("\r------- WRITING RESULTS FOR: " + test_name + "/" + client_folder + " -------");

		File folder = new File(folder_name.getAbsolutePath() + "/" + client_folder);

		if (!folder.exists()) {
			boolean created = folder.mkdir();
			if (!created) {
				System.out.println("RESULT FOLDER DOES NOT EXISTS AND CANT BE CREATED - USING EXECUTION FOLDER");
				return;
			}
		}
		///	System.out.println("OUTPUT FOLDER: " + folder.getName());

		for (String dataOperation : time_results.keySet()) {


			ArrayList<Pair<Long, Long>> result_data = time_results.get(dataOperation);

			if (dataOperation.trim().equals("")) {

				dataOperation = (unknown == 0) ? "UNKNOWN" : "UNKNOWN_" + unknown;
				unknown++;
			}

			File operation_results_file = new File(folder.getPath() + "/" + dataOperation + extension);


			try {
				operation_results_file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
				continue;
			}

			FileOutputStream out = null;
			BufferedOutputStream stream = null;

			try {
				out = new FileOutputStream(operation_results_file);

				stream = new BufferedOutputStream(out);

			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}


			try {


				stream.write("results , time\n".getBytes());


			} catch (IOException e) {
				e.printStackTrace();
			}

			//        resultComparator comparator = new resultComparator();
			//     Collections.sort(result_data, comparator);

			int length = result_data.size();

			try {

				for (int z = 0; z < length; z++) {

					Pair<Long, Long> res = result_data.get(z);

					String result_line = res.right + " , " + res.left + "";

					result_line = result_line + "\n";

					byte[] bytes = result_line.getBytes();

					stream.write(bytes, 0, bytes.length);

				}

			} catch (IOException e) {
				e.printStackTrace();
				continue;
			}

			try {
				stream.flush();
				stream.close();
			} catch (IOException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			} finally {
				try {
					out.close();
				} catch (IOException ex) {
					Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
				}
			}


		}
		if (!events.isEmpty()) {
			//	System.out.println("****WRITING EVENT COUNT****");


			for (String eventType : events.keySet()) {
				File event_results_file = new File(folder.getPath() + "/" + eventType + extension);
				FileOutputStream out = null;
				BufferedOutputStream stream = null;
				try {


					out = new FileOutputStream(event_results_file);
					stream = new BufferedOutputStream(out);

				} catch (FileNotFoundException e) {
					e.printStackTrace();
					continue;
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}


				//		System.out.println("+EVENT TYPE: " + eventType);

				try {
					for (String event : events.get(eventType).keySet()) {
						String line = event + " , " + events.get(eventType).get(event) + "\n";
						byte[] bytes = line.getBytes();
						stream.write(bytes,0,bytes.length);

						// System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));
					}

				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}
				try {
					stream.flush();
					stream.close();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						out.close();
					} catch (IOException ex) {
						Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
					}


				}

			}

		}


		if (!data.isEmpty()) {


			for (String eventType : data.keySet()) {
				File event_results_file = new File(folder.getPath() + "/" + eventType + extension);
				FileOutputStream out = null;
				BufferedOutputStream stream = null;
				try {
					out = new FileOutputStream(event_results_file);
					stream = new BufferedOutputStream(out);

				} catch (FileNotFoundException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}


				//			System.out.println("+DATA EVENT TYPE: " + eventType);
				int i = 0;
				try {

					if (dataHeader.get(eventType) == null) {
						dataHeader.put(eventType, new ArrayList<String>());
					}

					for (String header_name : dataHeader.get(eventType)) {
						if (i != 0)
							stream.write(" , ".getBytes());

						stream.write(header_name.getBytes());
						i++;
					}
					stream.write("\n".getBytes());
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}

				   		try {
				for (String event : data.get(eventType).keySet()) {

					stream.write(event.getBytes());
					for (Object o : data.get(eventType).get(event)) {
						String line = " , " + o.toString();
						stream.write(line.getBytes());

					}
					stream.write("\n".getBytes());
				}


				} catch (IOException e) {
					e.printStackTrace();
				}

				try {
					stream.flush();
					stream.close();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						out.close();
					} catch (IOException ex) {
						Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
					}


				}

			}

		}

		if (!data_results.isEmpty()) {
//			System.out.println("****WRITING DATA COUNT****");


			for (String eventType : data_results.keySet()) {
				File event_results_file = new File(folder.getPath() + "/" + eventType + extension);
				FileOutputStream out = null;
				BufferedOutputStream stream = null;
				try {
					out = new FileOutputStream(event_results_file);
					stream = new BufferedOutputStream(out);

				} catch (FileNotFoundException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
					continue;
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}


				int i = 0;
				try {

					if (dataHeader.get(eventType) == null) {
						dataHeader.put(eventType, new ArrayList<String>());
					}

					for (String header_name : dataHeader.get(eventType)) {
						if (i != 0)
							stream.write(" , ".getBytes());

						stream.write(header_name.getBytes());
						i++;
					}
					stream.write("\n".getBytes());
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}

				  try {
				for (List<Object> events : data_results.get(eventType)) {


					int index = 0;
					for (Object data : events) {

						if (index == 0) {
							stream.write(data.toString().getBytes());

						} else {
							String line = " , " + data.toString();
							stream.write(line.getBytes());

						}
						index++;
					}

					stream.write("\n".getBytes());



				}
				  	} catch (IOException e) {
						e.printStackTrace();
					}

				try {
					stream.flush();
					stream.close();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						out.close();
					} catch (IOException ex) {
						Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
					}


				}

			}

		}

		if (bechmark_info != null && !bechmark_info.isEmpty()) {
			File event_results_file = new File(folder.getPath() + "/" + "BENCHMARK_INFO.txt");
			FileWriter out = null;
			BufferedWriter stream = null;
			try {
				out = new FileWriter(event_results_file);
				stream = new BufferedWriter(out);

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			for (String field : bechmark_info.keySet()) {
				// System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));
				try {
					stream.write((field + " - " + bechmark_info.get(field) + "\n"));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				stream.flush();
				stream.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					out.close();
				} catch (IOException ex) {
					Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
				}


			}


		}

	}

	//use a unique folder

	public void listDatatoFiles(String folder_name, String extension) {

		int unknown = 0;

		System.out.println("\n\n-------WRITING RESULTS FOR: " + test_name + "-------");
		File enclosing_folder = new File(folder_name);
		System.out.println("OUTPUT PATH: " + enclosing_folder.getAbsolutePath());
		if (!enclosing_folder.exists()) {
			System.out.println("RESULT DEFINED PARENT FOLDER DOES NOT EXISTS - CREATING");
			boolean created = enclosing_folder.mkdir();
			if (!created) {
				System.out.println("RESULT DEFINED PARENT FOLDER DOES NOT EXISTS AND CANT BE CREATED - TRYING ENCLOSING FOLDER");
				enclosing_folder = enclosing_folder.getParentFile();
			}

		} else if (!enclosing_folder.isDirectory()) {
			enclosing_folder = enclosing_folder.getParentFile();
			System.out.println("NOT A FOLDER: ENCLOSING FOLDER USED -> " + enclosing_folder);
		}

		GregorianCalendar date = new GregorianCalendar();
		String suffix = date.get(GregorianCalendar.YEAR) + "_" + (date.get(GregorianCalendar.MONTH) + 1) + "_" + date.get(GregorianCalendar.DAY_OF_MONTH) + "_" + date.get(GregorianCalendar.HOUR_OF_DAY) + "_" + date.get(GregorianCalendar.MINUTE) + "";

		File folder = new File(enclosing_folder.getAbsolutePath() + "/" + test_name + suffix);

		if (!folder.exists()) {
			boolean created = folder.mkdir();
			if (!created) {
				System.out.println("RESULT FOLDER DOES NOT EXISTS AND CANT BE CREATED - USING EXECUTION FOLDER");
				File exe_folder = new File("./Results" + "/" + test_name + suffix);
				if (!exe_folder.exists()) {
					created = exe_folder.mkdir();
					if (!created) {
						System.out.println("EXECUTION FOLDER CANT BE USED, LEAVING...");
						return;
					} else {
						folder = exe_folder;
					}
				}
			}
		}
		System.out.println("OUTPUT FOLDER: " + folder.getName());

		for (String dataOperation : time_results.keySet()) {


			ArrayList<Pair<Long, Long>> result_data = time_results.get(dataOperation);

			if (dataOperation.trim().equals("")) {

				dataOperation = (unknown == 0) ? "UNKNOWN" : "UNKNOWN_" + unknown;
				unknown++;
			}


			File operation_results_file = new File(folder.getPath() + "/" + dataOperation + extension);


			try {
				operation_results_file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}

			FileOutputStream out = null;
			BufferedOutputStream stream = null;

			try {
				out = new FileOutputStream(operation_results_file);
				stream = new BufferedOutputStream(out);

			} catch (Exception e) {
				e.printStackTrace();
			}


			try {

				stream.write(("results , time\n").getBytes());


			} catch (IOException e) {
				e.printStackTrace();
			}

			//        resultComparator comparator = new resultComparator();
			//     Collections.sort(result_data, comparator);

			int length = result_data.size();
			for (int z = 0; z < length; z++) {

				Pair<Long, Long> res = result_data.get(z);

				String result_line = res.right + " , " + res.left + "";

				result_line = result_line + "\n";


				try {
					stream.write(result_line.getBytes());

				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			try {
				stream.flush();
				stream.close();
			} catch (IOException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			} finally {
				try {
					out.close();
				} catch (IOException ex) {
					Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
				}
			}


		}
		if (!events.isEmpty()) {
			System.out.println("****WRITING EVENT COUNT****");


			for (String eventType : events.keySet()) {
				File event_results_file = new File(folder.getPath() + "/" + eventType + extension);
				FileOutputStream out = null;
				BufferedOutputStream stream = null;
				try {
					out = new FileOutputStream(event_results_file);
					stream = new BufferedOutputStream(out);

				} catch (FileNotFoundException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}


				System.out.println("+EVENT TYPE: " + eventType);
				for (String event : events.get(eventType).keySet()) {
					// System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));
					try {
						stream.write((event + " , " + events.get(eventType).get(event) + "\n").getBytes());
					} catch (IOException e) {
						e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
					}
				}
				try {
					stream.flush();
					stream.close();
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				} finally {
					try {
						out.close();
					} catch (IOException ex) {
						Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
					}


				}

			}

		}


		if (!data.isEmpty()) {
			System.out.println("****WRITING DATA COUNT****");


			for (String eventType : data.keySet()) {
				File event_results_file = new File(folder.getPath() + "/" + eventType + extension);
				FileOutputStream out = null;
				BufferedOutputStream stream = null;
				try {
					out = new FileOutputStream(event_results_file);
					stream = new BufferedOutputStream(out);

				} catch (FileNotFoundException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}


				System.out.println("+DATA EVENT TYPE: " + eventType);
				int i = 0;
				try {

					if (dataHeader.get(eventType) == null) {
						dataHeader.put(eventType, new ArrayList<String>());
					}

					for (String header_name : dataHeader.get(eventType)) {
						if (i != 0)
							stream.write(" , ".getBytes());

						stream.write(header_name.getBytes());
						i++;
					}
					stream.write("\n".getBytes());
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}


				for (String event : data.get(eventType).keySet()) {
					try {
						stream.write((event).getBytes());
						for (Object o : data.get(eventType).get(event)) {
							stream.write((" , " + o.toString()).getBytes());
						}
						stream.write("\n".getBytes());

					} catch (IOException e) {
						e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
					}
				}


				try {
					stream.flush();
					stream.close();
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				} finally {
					try {
						out.close();
					} catch (IOException ex) {
						Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
					}


				}

			}

		}

		if (!data_results.isEmpty()) {
			System.out.println("****WRITING DATA COUNT****");


			for (String eventType : data_results.keySet()) {
				File event_results_file = new File(folder.getPath() + "/" + eventType + extension);
				FileOutputStream out = null;
				BufferedOutputStream stream = null;
				try {
					out = new FileOutputStream(event_results_file);
					stream = new BufferedOutputStream(out);

				} catch (FileNotFoundException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}


				System.out.println("+DATA RESULT TYPE: " + eventType);
				int i = 0;
				try {

					if (dataHeader.get(eventType) == null) {
						dataHeader.put(eventType, new ArrayList<String>());
					}

					for (String header_name : dataHeader.get(eventType)) {
						if (i != 0)
							stream.write(" , ".getBytes());

						stream.write(header_name.getBytes());
						i++;
					}
					stream.write("\n".getBytes());
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}


				for (List<Object> events : data_results.get(eventType)) {


					try {

						int index = 0;
						for (Object data : events) {

							if (index == 0) {
								stream.write(data.toString().getBytes());

							} else {
								stream.write((" , " + data.toString()).getBytes());

							}
							index++;
						}

						stream.write("\n".getBytes());

					} catch (IOException e) {
						e.printStackTrace();
					}
				}


				try {
					stream.flush();
					stream.close();
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				} finally {
					try {
						out.close();
					} catch (IOException ex) {
						Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
					}


				}

			}

		}

		if (bechmark_info != null && !bechmark_info.isEmpty()) {
			File event_results_file = new File(folder.getPath() + "/" + "BENCHMARK_INFO.txt");
			FileOutputStream out = null;
			BufferedOutputStream stream = null;
			try {
				out = new FileOutputStream(event_results_file);
				stream = new BufferedOutputStream(out);

			} catch (FileNotFoundException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			}

			for (String field : bechmark_info.keySet()) {
				// System.out.println("\t>>" + event + " : " + events.get(eventType).get(event));
				try {
					stream.write((field + " - " + bechmark_info.get(field) + "\n").getBytes());
				} catch (IOException e) {
					e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
				}
			}
			try {
				stream.flush();
				stream.close();
			} catch (IOException e) {
				e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			} finally {
				try {
					out.close();
				} catch (IOException ex) {
					Logger.getLogger(ResultHandler.class.getName()).log(Level.SEVERE, null, ex);
				}
			}
		}
	}


	class resultComparator implements Comparator {

		public int compare(Object o1, Object o2) {

			if (!(o1 instanceof Pair) || !(o2 instanceof Pair))
				return 0;

			Pair<Long, Long> p1 = (Pair<Long, Long>) o1;
			Pair<Long, Long> p2 = (Pair<Long, Long>) o2;

			if (p1.left > p2.left)

				return 1;

			else if (p1.left < p2.left)

				return -1;

			else

				return 0;
		}
	}

}

