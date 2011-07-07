package filters;

import java.io.*;
import java.util.*;

public class SimpleListFilter {
	
	protected File arffInput;
	
	public SimpleListFilter(String arffPath) {
		arffInput = new File(arffPath);
	}
	
	public static void makeSimpleList(String inputARFFPath, String outputARFFPath) throws IOException{
		FileInputStream inStream = new FileInputStream(inputARFFPath);
		
		// Get the object of DataInputStream
		DataInputStream in = new DataInputStream(inStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		
		// Read only data
		// designed for Enron email logs db, with attributes:
		
		// get to data
		while ((line = br.readLine()) != null) {
			//System.out.println(line);
			if (line.contains("@DATA")) break;
		}
		
		//HashSet<String> senders = new HashSet<String>();
		//HashSet<String> recipients = new HashSet<String>();
		SortedSet<String> ids = new TreeSet<String>();
		
		// build set of ids
		while ((line = br.readLine()) != null)   {
			//System.out.println(line);
			String[] split = line.split(",");
			//senders.add(split[1]);
			//recipients.add(split[2]);
			ids.add(split[1]);
			ids.add(split[2]);
		}
		
		// get sorted list of ids
		
		// create new ARFF file for the output
		FileOutputStream outStream = new FileOutputStream(outputARFFPath);
		DataOutputStream out = new DataOutputStream(outStream);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		// write header
		bw.write("% Partial Enron email logs\n\n");
		bw.write("@RELATION Enron_email_log_database_filtered\n\n");
		bw.write("@ATTRIBUTE timestamp DATE \"yyMMdd\"\n");
		bw.write("@ATTRIBUTE sender STRING\n\n");
		
		// write all id attributes and create map to index
		Map<String,Integer> indices = new HashMap<String,Integer>();
		
		int i = 0;
		String curr;
		for (Iterator<String> it = ids.iterator(); it.hasNext(); ) {
			curr = it.next();
			bw.write("% #"+(i+2)+"\n");
			bw.write("@ATTRIBUTE "+curr+" INTEGER\n");
			indices.put(curr,(i+2));
			
			i++;
			if (i % 100 == 0) {
				bw.flush();
			}
		}
		bw.write("\n@DATA\n");
		bw.flush();
		
		// build new data according to new format
		// map from date and id concatenated to records (contacts and email numbers)
		Map<String,Map<String,Integer>> records = new TreeMap<String,Map<String,Integer>>();
		
		// go over the data again and collect contacts per date per sender
		
		in.close();
		inStream = new FileInputStream(inputARFFPath);
		in = new DataInputStream(inStream);
		br = new BufferedReader(new InputStreamReader(in));
		
		// get to data
		while ((line = br.readLine()) != null) {
			if (line.contains("@DATA")) break;
		}
		
		String id;
		String recipient;
		Map<String,Integer> contacts;
		String[] split;
		
		while ((line = br.readLine()) != null)   {
			split = line.split(",");
			
			// get / create date-id entry
			
			id = split[0]+","+split[1]; // "<date>,<sender>"
			contacts = records.get(id);
			if (contacts == null) {
				contacts = new HashMap<String,Integer>();
			}
			
			// add data (aggregate if necessary)
			recipient = split[2];
			if (!contacts.containsKey(recipient)) {
				contacts.put(recipient, Integer.parseInt(split[4]));
			} else {
				contacts.put(recipient,contacts.get(recipient) + Integer.parseInt(split[4]));
			}
			
			records.put(id, contacts);
		}

		/* -- print for debugging -- */
		System.out.println("Records");
		System.out.println("=======");
		for (Iterator<String> a = records.keySet().iterator(); a.hasNext(); ) {
			String r = a.next();
			contacts = records.get(r);
			System.out.println(r+":");
			System.out.println("-----------------------------------------------");
			for (Iterator<String> b = contacts.keySet().iterator(); b.hasNext(); ) {
				String con = b.next();
				System.out.println("   "+con+" -> "+contacts.get(con));
			}
			System.out.println();
		}
		/* -- */
		
		// iterate over all records and write them to file
		List<String> sortedContacts;
		String key;
		String date;
		Iterator<String> conit;
		String contact;
		
		i = 0;
		for (Iterator<String> it = records.keySet().iterator(); it.hasNext(); ) {
			 key = it.next();
			 split = key.split(",");
			 date = split[0];
			 id = split[1];
			 
			 bw.write("{0 "+date+", 1 "+id);
			 
			 // get contacts sorted by attribute list index
			 contacts = records.get(key);
			 sortedContacts = new ArrayList<String>();
			 for (conit = contacts.keySet().iterator(); conit.hasNext(); ) {
				 sortedContacts.add(conit.next());
			 }
			 Collections.sort(sortedContacts,new IDComparator<String>(indices));
			 
			 // iterate over sorted contacts and write
			 for (conit = sortedContacts.iterator(); conit.hasNext(); ) {
				 contact = conit.next();
				 bw.write(", "+indices.get(contact)+" "+contacts.get(contact));
			 }
			 bw.write("}\n");
			 
			 i++;
			 if (i % 100 == 0) bw.flush();
		}
		bw.flush();
		
		
		// Close streams
		in.close();
		out.close();
	}
	
	public static void enumerateStrings(String inpath, String outpath) throws IOException {
		FileInputStream inStream = new FileInputStream(inpath);
		DataInputStream in = new DataInputStream(inStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		
		// Read only data
		// designed for Enron email logs db, with attributes
		
		// get to data
		String header = "";
		while ((line = br.readLine()) != null) {
			header += line.replace("STRING", "INTEGER")+"\n";
			if (line.contains("@DATA")) break;
		}
		
		// read lines and save id mapping
		int i = 0;
		String[] split;
		Map<String,Integer> map = new HashMap<String,Integer>();
		String sender, recipient;
		
		while ((line = br.readLine()) != null) {
			split = line.split(",");
			sender = split[1];
			recipient = split[2];
			if (!map.containsKey(sender)) {
				map.put(sender, i++);
			}
			if (!map.containsKey(recipient)) {
				map.put(recipient, i++);
			}
		}

		// create new ARFF file for the output
		FileOutputStream outStream = new FileOutputStream(outpath);
		DataOutputStream out = new DataOutputStream(outStream);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		bw.write(header);
		bw.flush();
		
		in.close();
		inStream = new FileInputStream(inpath);
		in = new DataInputStream(inStream);
		br = new BufferedReader(new InputStreamReader(in));
		
		while ((line = br.readLine()) != null) {
			if (line.contains("@DATA")) break;
		}
		
		// go over data again and switch strings with integers
		i = 0;
		while ((line = br.readLine()) != null) {
			split = line.split(",");
			bw.write(split[0]+","+map.get(split[1])+","+map.get(split[2])+","+split[3]+","+split[4]+"\n");
			if (i++ % 100 == 0) bw.flush();
		}
		bw.flush();
		
		in.close();
		out.close();
	}
	
	public static void main(String[] args) {
		String path = "D:\\data\\documents\\Workspace\\wharton.reidentification\\raw-data\\arff";
		String baseFileName = "enron";
		try {
			makeSimpleList(path+"\\"+baseFileName+".arff",path+"\\"+baseFileName+"_filtered.arff");
			enumerateStrings(path+"\\"+baseFileName+".arff", path+"\\"+baseFileName+"_enum.arff");
			makeSimpleList(path+"\\"+baseFileName+"_enum.arff",path+"\\"+baseFileName+"_filtered_enum.arff");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
