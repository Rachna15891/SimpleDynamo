package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	private static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
	private static final String[] EMULATOR_ID = {"5554", "5556", "5558", "5560", "5562"};

	static String myNodeId = null;
	static String myPort = null;
	static String myNodeIdGenHash = null;

	private static final String INSERT_ALL = "INSERT_ALL";
	private static final String INSERT_REPLICATE = "INSERT_REPLICATE";
	private static final String INSERT_ACK = "INSERT_ACK";

	private static final String DELETE_FILE_REPLICATED = "DELETE_FILE_REPLICATED";
	private static final String DELETE_FILE_FROM_ALL = "DELETE_FILE_FROM_ALL";
	private static final String DELETE_ALL_MSG = "DELETE_ALL_MSG";
	private static final String DELETE_ACK = "DELETE_ACK";

	private static final String QUERY_FILE= "QUERY_FILE";
	private static final String QUERY_FILE_REPLY = "QUERY_FILE_REPLY";
	private static final String QUERY_ALL_MSG= "QUERY_ALL_MSG";
	private static final String QUERY_ALL_REPLY= "QUERY_ALL_REPLY";


	private static final String RECOVERY_GET_REPLICATED = "RECOVERY_GET_REPLICATED";
	private static final String RECOVERY_GET_ORIGINAL = "RECOVERY_GET_ORIGINAL";
	private static final String SEND_ORIG_FILE = "SEND_ORIG_FILE";
	private static final String SEND_REPLICA_FILE = "SEND_REPLICA_FILE";

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private static ArrayList<String> ListOfAliveNodes = new ArrayList<String>();
	private HashMap<String,String> SenderGenHashMap = new HashMap();
	private HashMap<String, String> queryMap= new HashMap<String, String>();
	private HashMap<String, String> queryInsertWaitMap= new HashMap<String, String>();
	private HashMap<String,HashMap<String,String>> queryAllMap = null;
	private Integer queryAllCount = 1;
	private Integer NoOfRepliesExpected = 4;

	public Uri getProviderUri() {
		return providerUri;
	}

	public Uri providerUri = null;
	/**
	 * buildUri() demonstrates how to build a URI for a
	 * @param authority
	 * @return the URI
	 */
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

		myNodeId = portStr; // eg 5554
		myPort = String.valueOf((Integer.parseInt(portStr) * 2)); ///eg 11108
		try {
			myNodeIdGenHash = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "On create : No Such Algorithm exception caught ");
			e.printStackTrace();
		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {

			Log.e(TAG, "Can't create a ServerSocket");
			return  false;
		}

		createSenderGenHashMap();
		//if i am back from recovery , i need to send request to my successors for my original (key,value) pairs
		// also after recovery i need to store replicated keys of my 2 predecessor nodes, which i wud have missed, can get from immediate prev node too
		// so send request to get the keys & store in ur file system

		int indexOfMyNode = ListOfAliveNodes.indexOf(myNodeIdGenHash);
		// who is my successor
		String mySuccessorNode = SenderGenHashMap.get(ListOfAliveNodes.get((indexOfMyNode + 1) % 5));
		MessageObj msgObj = new MessageObj();
		msgObj.setMsgType(RECOVERY_GET_ORIGINAL);
		msgObj.setReceiverForMsg(mySuccessorNode);
		msgObj.setSenderNodeID(myNodeId);

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);

		String myImmediatePredecessor1 = null;
		String myImmediatePredecessor2 = null;
		if(indexOfMyNode==0) {
			myImmediatePredecessor1 = SenderGenHashMap.get(ListOfAliveNodes.get(4));
			myImmediatePredecessor2 = SenderGenHashMap.get(ListOfAliveNodes.get(3));
		} else if(indexOfMyNode==1) {
			myImmediatePredecessor1 = SenderGenHashMap.get(ListOfAliveNodes.get(0));
			myImmediatePredecessor2 = SenderGenHashMap.get(ListOfAliveNodes.get(4));
		} else {
			myImmediatePredecessor1 = SenderGenHashMap.get(ListOfAliveNodes.get((indexOfMyNode - 1) % 5));
			myImmediatePredecessor2 = SenderGenHashMap.get(ListOfAliveNodes.get((indexOfMyNode - 2) % 5));
		}

		MessageObj msgObj2 = new MessageObj();
		msgObj2.setMsgType(RECOVERY_GET_REPLICATED);
		msgObj2.setReceiverForMsg(myImmediatePredecessor1);
		msgObj2.setSuccessor1(myImmediatePredecessor2);
		msgObj2.setSenderNodeID(myNodeId);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj2);
		return true;

	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {


		// selection can be a key directly, @ or *

		if( selection.equals("@") ) { // delete all key value pairs from local AVD

			Log.e(TAG, "Entered Delete File, delete all the files in my local : (selection ) :  " + selection);
			try {
				String files[] = getContext().fileList();
				File dir = getContext().getFilesDir();
				for(int i = 0 ; i<files.length ; i++) {
					File file = new File(dir, files[i]);
					boolean deleteflag = file.delete();
					Log.e(TAG, "Deleting file :"+i+" filename : "+files[i] + " deleteflag : "+deleteflag);

				}

			} catch (Exception e) {
				Log.e(TAG, "File delete failed");
				e.printStackTrace();
			}
			Log.v(TAG," : In delete(@) Completed !! ");
			return 1;

		} else if (selection.equals("*")) { // delete all files from all the 5 AVDs

			Log.e(TAG, "Entered Delete File, delete all the files in my local : (selection ) :  " + selection);
			try {
				String files[] = getContext().fileList();
				File dir = getContext().getFilesDir();
				for(int i = 0 ; i<files.length ; i++) {
					File file = new File(dir, files[i]);
					boolean deleteflag = file.delete();
					Log.e(TAG, "Deleting file :"+i+" filename : "+files[i] + " deleteflag : "+deleteflag);

				}

			} catch (Exception e) {
				Log.e(TAG, "File delete failed");
				e.printStackTrace();
			}

			MessageObj msgObj = new MessageObj();
			msgObj.setMsgType(DELETE_ALL_MSG);
			msgObj.setQueryFile("@");
			msgObj.setSenderNodeID(myNodeId);
			Log.e(TAG, "Fwding To Client Task DELETE_ALL_MSG from original Sender :  " + myNodeId);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
			Log.v(TAG," : In delete(*) ...Original Sender Code Run Completed !! ");
			return 1;

		} else {
			// when the selection gives the filename directly
			String fileName = selection;
			String fileNameHash = null;
			String msgType = null;
			if(selectionArgs !=null) {
				msgType = selectionArgs[0];
				// so don't do any processing logic of where the file to delete is stored..just delete the file !
				try {
					File dir = getContext().getFilesDir();
					File file = new File(dir, fileName);
					boolean deleted = file.delete();
					Log.v(TAG, "DELETE() : file deleted successfully..."+fileName+ "deleteFlag : "+deleted);

				} catch (Exception e) {
					Log.e(TAG, "File delete failed");
					e.printStackTrace();
				}
				return 1;

			} else {
				Log.e(TAG, "I am the FIRST Node to Receive Delete Request");

			}

			try {
				fileNameHash = genHash(fileName);

			} catch(NoSuchAlgorithmException e ) {
				Log.e(TAG, "In Delete() For filename : No Such Algorithm exception caught ");
				e.printStackTrace();
			}

			Log.e(TAG, "Entered Delete File, checking if the file is stored in my avd ");

			int indexOfMyNode = ListOfAliveNodes.indexOf(myNodeIdGenHash);
			int indexToStoreKey = 0;
			if(((fileNameHash.compareTo(ListOfAliveNodes.get(4)))>0) ||(fileNameHash.compareTo(ListOfAliveNodes.get(0))<=0)) {
				//boundary cases where the (key,value) should be stored in first Node
				indexToStoreKey = 0;
			} else {
				for (int i =1 ; i<ListOfAliveNodes.size(); i++) {
					if( (fileNameHash.compareTo(ListOfAliveNodes.get(i-1))>0) && (fileNameHash.compareTo(ListOfAliveNodes.get(i))<=0) ) {
						indexToStoreKey = i;
						break;
					}
				}
			}

			String originalNodeToDelKey = SenderGenHashMap.get(ListOfAliveNodes.get(indexToStoreKey));
			String successor1ToDelKey = SenderGenHashMap.get(ListOfAliveNodes.get((indexToStoreKey + 1) % 5));
			String successor2ToDelKey = SenderGenHashMap.get(ListOfAliveNodes.get((indexToStoreKey+2)%5));
			Log.e(TAG, "DELETE() : myNodeId: "+myNodeId+" originalNodeToDelKey: "+originalNodeToDelKey+ " successor1ToDelKey: "+successor1ToDelKey+"  successor2ToDelKey: "+successor2ToDelKey);

			if(indexOfMyNode == indexToStoreKey) {
				// I have the original file which is to be deleted
				try {
					File dir = getContext().getFilesDir();
					File file = new File(dir, fileName);
					boolean deleted = file.delete();
					Log.v(TAG, "DELETE() : file deleted successfully..."+fileName+ "deleteFlag : "+deleted);

				} catch (Exception e) {
					Log.e(TAG, "File delete failed");
					e.printStackTrace();
				}
				//Also forward delete request to successor nodes, to delete the replicated files
				MessageObj msgObj = new MessageObj();
				msgObj.setMsgType(DELETE_FILE_REPLICATED);
				msgObj.setQueryFile(fileName);
				msgObj.setSenderNodeID(myNodeId);
				msgObj.setSuccessor1(successor1ToDelKey);
				msgObj.setSucessor2(successor2ToDelKey);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
				Log.e(TAG, "DELETE() : DELETE_FILE_REPLICATED msg send to Client task , msg : " + msgObj.toString());

			} else {
				// forward the request to 3 nodes ( 1 parent 2 for replication)
				MessageObj msgObj = new MessageObj();
				msgObj.setMsgType(DELETE_FILE_FROM_ALL);
				msgObj.setQueryFile(fileName);
				msgObj.setSenderNodeID(myNodeId);
				msgObj.setReceiverForMsg(originalNodeToDelKey);
				msgObj.setSuccessor1(successor1ToDelKey);
				msgObj.setSucessor2(successor2ToDelKey);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
				Log.e(TAG, "DELETE() : DELETE_FILE_FROM_ALL msg send to Client task with msg" + msgObj.toString());

			}

			return 1;
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public void createSenderGenHashMap () {
		for ( int i = 0 ; i<EMULATOR_ID.length ; i++) {

			try {
				String genHashVal = genHash(EMULATOR_ID[i]);
				SenderGenHashMap.put(genHashVal, EMULATOR_ID[i]);
				ListOfAliveNodes.add(genHashVal);

			} catch(NoSuchAlgorithmException e ) {
				Log.e(TAG, "In Insert() : No Such Algorithm exception caught ");
				e.printStackTrace();
			}
		}
		// Sort the list of Gen Hashed Node Ids to know each node's position in chord
		Collections.sort(ListOfAliveNodes, new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				return s1.compareToIgnoreCase(s2);
			}
		});
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {

		String key = (String)values.get("key");
		String value = (String)values.get("value");
		String hashOfKey = null;
		Log.e(TAG, " INSERT(): Entered Insert() with ....  key: "+key+" value: "+value);

		Log.e(TAG, " INSERT() : Checking where to insert the key.........");

		// Now you have the key and value - hash the key to find the correct location of key
		try {
			hashOfKey  = genHash(key);

		} catch(NoSuchAlgorithmException e ) {
			Log.e(TAG, "In Insert() : No Such Algorithm exception caught ");
			e.printStackTrace();
		}

		int indexOfMyNode = ListOfAliveNodes.indexOf(myNodeIdGenHash);
		int indexToStoreKey = 0;
		if(((hashOfKey.compareTo(ListOfAliveNodes.get(4)))>0) ||(hashOfKey.compareTo(ListOfAliveNodes.get(0))<=0)) {
			//boundary cases where the (key,value) should be stored in first Node
			indexToStoreKey = 0;
		} else {
			for (int i =1 ; i<ListOfAliveNodes.size(); i++) {
				if( (hashOfKey.compareTo(ListOfAliveNodes.get(i-1))>0) && (hashOfKey.compareTo(ListOfAliveNodes.get(i))<=0) ) {
					indexToStoreKey = i;
					break;
				}
			}
		}

		String originalNodeToStoreKey = SenderGenHashMap.get(ListOfAliveNodes.get(indexToStoreKey));
		String successor1ToStoreKey = SenderGenHashMap.get(ListOfAliveNodes.get((indexToStoreKey+1)%5));
		String successor2ToStoreKey = SenderGenHashMap.get(ListOfAliveNodes.get((indexToStoreKey+2)%5));
		Log.e(TAG, "INSERT() : myNodeId: "+myNodeId+" originalNodeToStoreKey: "+originalNodeToStoreKey+ " successor1ToStoreKey: "+successor1ToStoreKey+"  successor2ToStoreKey: "+successor2ToStoreKey);

		if(indexOfMyNode == indexToStoreKey) {
			// i need to store this key value pair in my local file system and forward to next 2 nodes
			Log.e(TAG, "I am the original sender for key : "+key +" value : "+value);
			FileOutputStream outputStream;
			try {
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE); // check the value of key & Value to be inserted is correct
				outputStream.write(value.getBytes());
				outputStream.flush();
				outputStream.close();


			} catch (Exception e) {
				Log.e(TAG, "File write failed");
			}
			Log.v(TAG, "INSERT() : Provider msg inserted : "+values.toString());
			//Also send the msg to other 2 successor nodes ( Replication)

			MessageObj msgObj = new MessageObj();
			msgObj.setMsgType(INSERT_REPLICATE);
			msgObj.setmContentValues(key, value);
			msgObj.setSenderNodeID(myNodeId);
			msgObj.setReceiverForMsg(myNodeId);
			msgObj.setSuccessor1(successor1ToStoreKey);
			msgObj.setSucessor2(successor2ToStoreKey);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
			Log.e(TAG, "INSERT() : INSERT_REPLICATE for replication msg send to Client task to succ 1: " + successor1ToStoreKey + " succ2: " +successor2ToStoreKey);


		} else {
			// forward the request to 3 nodes ( 1 parent 2 for replication)
			MessageObj msgObj = new MessageObj();
			msgObj.setMsgType(INSERT_ALL);
			msgObj.setmContentValues(key, value);
			msgObj.setSenderNodeID(myNodeId);
			msgObj.setReceiverForMsg(originalNodeToStoreKey);
			msgObj.setSuccessor1(successor1ToStoreKey);
			msgObj.setSucessor2(successor2ToStoreKey);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
			Log.e(TAG, "INSERT() : I am the original node to receive insert request for key : " + key);
			Log.e(TAG, "INSERT() : INSERT_ALL send to Client task with msg" + msgObj.toString());

		}
		return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		Log.e(TAG,"QUERY() method invoked....with selection : "+selection);
		if( selection.equals("@") ) { // return all key value pairs from local AVD
			String fileName = null;
			FileInputStream fis;
			MatrixCursor mc = new MatrixCursor(new String[] { "key", "value"});
			StringBuffer fileContent = new StringBuffer("");
			int n=0;
			String fileNamesList[] = getContext().fileList();
			int noOfFiles = fileNamesList.length;
			for ( int counter = 0 ; counter < noOfFiles ; counter++) {
				fileName = fileNamesList[counter];
				n=0;
				fileContent = new StringBuffer("");

				try {
					fis = getContext().openFileInput(fileName);
					byte[] buffer = new byte[1024];
					while ((n=fis.read(buffer)) != -1)
					{
						fileContent.append(new String(buffer, 0, n));
					}
				} catch (FileNotFoundException e) {
					Log.e(TAG,"File Not Found To Read for iteration : "+counter);
				} catch (IOException e) {
					Log.e(TAG, "Error Reading File for iteration : "+counter);
				}

				//read one file now store its corresponding values as a new row in matrix cursor
				mc.addRow(new String[] {fileName, fileContent.toString() });
				Log.v(counter+ " : In query(Sel) : ",selection);
				Log.v(counter+" : In query...", "Filename : "+fileName+" & File Content : "+fileContent.toString());

			}

			Log.v(TAG," : In query(@) replying with cursor count : "+mc.getCount());
			return mc;

		} else if (selection.equals("*")) { // return all key value pairs from all the live AVDs
			String fileName = null;
			FileInputStream fis;
			MatrixCursor mc = new MatrixCursor(new String[] { "key", "value"});
			StringBuffer fileContent;
			int n=0;
			String fileNamesList[] = getContext().fileList();
			int noOfFiles = fileNamesList.length;
			Log.e(TAG,"Only original * query receiver  comes here : : noOffiles I have : "+noOfFiles);

			for ( int counter = 0 ; counter < noOfFiles ; counter++) {
				fileName = fileNamesList[counter];
				n=0;
				fileContent = new StringBuffer("");

				try {
					fis = getContext().openFileInput(fileName);
					byte[] buffer = new byte[1024];
					while ((n=fis.read(buffer)) != -1)
					{
						fileContent.append(new String(buffer, 0, n));
					}
				} catch (FileNotFoundException e) {
					Log.e(TAG,"File Not Found To Read for iteration : "+counter);
				} catch (IOException e) {
					Log.e(TAG, "Error Reading File for iteration : "+counter);
				}

				//read one file now store its corresponding values as a new row in matrix cursor
				mc.addRow(new String[] {fileName, fileContent.toString() });
				Log.v(counter+ " : In query(Sel) : ",selection);
				Log.v(counter+" : In query...", "Filename : "+fileName+" & File Content : "+fileContent.toString());

			}

			//Now you got ur local files, but wait till u get the files stored in all other alive AVD nodes.
			// Send a request to all other avds except urs and wait for their responses, then concatenate all the responses into a matrix and return

			queryAllMap = new HashMap<String,HashMap<String,String>>();

			for(int i=0;i<EMULATOR_ID.length;i++) {
				if(myNodeId.equals(EMULATOR_ID[i])) {
					continue;
				}
				queryAllMap.put(EMULATOR_ID[i],null);
			}
			MessageObj msgObj = new MessageObj();
			msgObj.setMsgType(QUERY_ALL_MSG);
			msgObj.setSenderNodeID(myNodeId);
			msgObj.setQueryFile("@");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
			Log.e(TAG, "QUERY_ALL_MSG send to Client task with msg" + msgObj.toString());

			synchronized (this ) {
				queryAllCount = 0;
			}

			// check the condition of No Of Replies Expected in the case of failure ( pending)

			while(queryAllCount < NoOfRepliesExpected ){
				// only original sender waits to receive the reply from all the nodes to return all their local file content
				Log.e(TAG, "Query FileName : Waiting Till File Content Not Received ..................... QueryAllCount is : "+queryAllCount);
			}

			Log.e(TAG, "While loop exited : queryAllCount : " + queryAllCount + "NoOfRepliesExpected :" +NoOfRepliesExpected );

			Iterator it = queryAllMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				String emulatorID = (String)pair.getKey();
				HashMap fileMap = (HashMap)pair.getValue();
				if(fileMap!=null) {
					Iterator it2 = fileMap.entrySet().iterator();
					int counter = 0;
					while(it2.hasNext()) {
						Map.Entry fileMapValues = (Map.Entry)it2.next();
						String fileKey = (String)fileMapValues.getKey();
						String fileValue = (String)fileMapValues.getValue();
						mc.addRow(new String[]{fileKey, fileValue});
						Log.v(TAG,counter+ " : In query(All while loop) : "+selection);
						Log.v(TAG,counter+" : In query(All while loop)...Filename : "+fileKey+" & File Content : "+fileValue);

					}
				}
			}
			queryAllMap = null;
			return mc; // returns the final matrixCursor

		} else {
			// when the selection gives the filename directly
			String fileName = selection;
			String fileNameHash = null;
			String senderNodeId = null;

			if(projection!=null) {
				senderNodeId = projection[0];
				FileInputStream fis;
				StringBuffer fileContent = new StringBuffer("");
				int n=0;

				try {
					fis = getContext().openFileInput(fileName);
					byte[] buffer = new byte[1024];
					n = 0;
					while((n = fis.read(buffer)) != -1 ) {
						fileContent.append(new String(buffer, 0, n));
					}
					Log.e(TAG, "File Content when I am the successor to return query request with value : "+fileContent.toString());
				} catch (FileNotFoundException e) {
					Log.e(TAG, "File Not Found To Read File : "+fileName);

				} catch (IOException e) {
					Log.e(TAG, "Error Reading File");
				}
				MatrixCursor mc = new MatrixCursor(new String[] { "key", "value"});
				mc.addRow(new String[]{fileName, fileContent.toString()});
				Log.v("In query...Selection : ",selection);
				Log.v("In query...", "Filename : "+fileName+" & File Content : "+fileContent.toString());
				return mc;
			}

			try {
				fileNameHash = genHash(fileName);

			} catch(NoSuchAlgorithmException e ) {
				Log.e(TAG, "In Insert() : No Such Algorithm exception caught ");
				e.printStackTrace();
			}

			FileInputStream fis;
			StringBuffer fileContent = new StringBuffer("");
			int n=0;



			int indexOfMyNode = ListOfAliveNodes.indexOf(myNodeIdGenHash);
			int indexToStoreKey = 0;
			if(((fileNameHash.compareTo(ListOfAliveNodes.get(4)))>0) ||(fileNameHash.compareTo(ListOfAliveNodes.get(0))<=0)) {
				//boundary cases where the (key,value) should be stored in first Node
				indexToStoreKey = 0;
			} else {
				for (int i =1 ; i<ListOfAliveNodes.size(); i++) {
					if( (fileNameHash.compareTo(ListOfAliveNodes.get(i-1))>0) && (fileNameHash.compareTo(ListOfAliveNodes.get(i))<=0) ) {
						indexToStoreKey = i;
						break;
					}
				}
			}

			String originalNodeToStoreKey = SenderGenHashMap.get(ListOfAliveNodes.get(indexToStoreKey));
			String successor1ToStoreKey = SenderGenHashMap.get(ListOfAliveNodes.get((indexToStoreKey+1)%5));
			String successor2ToStoreKey = SenderGenHashMap.get(ListOfAliveNodes.get((indexToStoreKey+2)%5));
			if(indexOfMyNode == indexToStoreKey ) {
				// means i have the original file so can directly return to the query
				boolean continueLoop= true;
				while(continueLoop) {
					try {
						fis = getContext().openFileInput(fileName);
						byte[] buffer = new byte[1024];
						n = 0;
						while((n = fis.read(buffer)) != -1 || fileContent.toString()=="") {
							fileContent.append(new String(buffer, 0, n));
						}
						Log.e(TAG, "File Content after exiting while loop in query filename() : "+fileContent.toString());
						continueLoop = false;
					} catch (FileNotFoundException e) {
						Log.e(TAG, "File Not Found To Read File : "+fileName);


					} catch (IOException e) {
						Log.e(TAG, "Error Reading File");
					}
				}
			} else {
				// forward the query request for given file to the node where it is stored ; originalNodeToStoreKey & also the successors
				// But wait till the original node returns the result
				queryMap= new HashMap<String, String>();
				MessageObj msgObj = new MessageObj();
				msgObj.setQueryFile(fileName);
				msgObj.setMsgType(QUERY_FILE);
				msgObj.setReceiverForMsg(originalNodeToStoreKey);
				msgObj.setSuccessor1(successor1ToStoreKey);
				msgObj.setSucessor2(successor2ToStoreKey);
				msgObj.setSenderNodeID(myNodeId);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
				Log.e(TAG, "QUERY_FILE send to Client task with msg : " + msgObj.toString());


				while(queryMap.get(fileName)==null){
					// only original sender waits to receive the reply from any node with the file content
					//  Log.e(TAG, "Query FileName : Waiting Till File Content Not Received ........................");
				}

				fileContent.append(queryMap.get(fileName));
			}
			MatrixCursor mc = new MatrixCursor(new String[] { "key", "value"});
			mc.addRow(new String[]{fileName, fileContent.toString()});
			Log.v("In query...Selection : ",selection);
			Log.v("In query...", "Filename : "+fileName+" & File Content : "+fileContent.toString());
			return mc;
		}

	}


	public synchronized void myInsert(ContentValues contentValues) {

			//u received insert request from another avd..ur not the original sender
			//so directly insert the file to ur file system without any further checks
			String key = (String)contentValues.get("key");
			String value = (String)contentValues.get("value");
			FileOutputStream outputStream;
			try {
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE); // check the value of key & Value to be inserted is correct
				outputStream.write(value.getBytes());
				outputStream.flush();
				outputStream.close();


			} catch (Exception e) {
				Log.e(TAG, "File write failed");
			}
			Log.v(TAG, "INSERT() Received from parent AVD : Provider msg inserted : " + contentValues.toString());
	}

	public synchronized void InsertOriginalFiles(HashMap<String,String> fileMap) {

		Iterator it = fileMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			String fileName = (String)pair.getKey();
			String fileContent = (String)pair.getValue();
			FileOutputStream outputStream;
			try {
				outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
				outputStream.write(fileContent.getBytes());
				outputStream.flush();
				outputStream.close();


			} catch (Exception e) {
				Log.e(TAG, "File write failed");
			}
			Log.v(TAG, "InsertOriginalFiles() Inserted file : " + fileName + " with value : "+fileContent);
		}
	}

	public HashMap<String, String> getOriginalFiles(String senderGenHash) {
		HashMap<String, String> fileMap = new HashMap<String, String>();
		String fileName = null;
		FileInputStream fis;
		StringBuffer fileContent = new StringBuffer("");
		int n = 0;
		int indexOfSenderGenHash = ListOfAliveNodes.indexOf(senderGenHash);
		int originalIndexToStoreFile =0;
		String fileNameHash = null;

		String fileNamesList[] = getContext().fileList();
		int noOfFiles = fileNamesList.length;
		for (int counter = 0; counter < noOfFiles; counter++) {
			fileName = fileNamesList[counter];

			try {
				fileNameHash = genHash(fileName);

			} catch(NoSuchAlgorithmException e ) {
				Log.e(TAG, "In Delete() For filename : No Such Algorithm exception caught ");
				e.printStackTrace();
			}

			if(((fileNameHash.compareTo(ListOfAliveNodes.get(4)))>0) ||(fileNameHash.compareTo(ListOfAliveNodes.get(0))<=0)) {
				//boundary cases where the (key,value) should be stored in first Node
				originalIndexToStoreFile = 0;
			} else {
				for (int i =1 ; i<ListOfAliveNodes.size(); i++) {
					if( (fileNameHash.compareTo(ListOfAliveNodes.get(i-1))>0) && (fileNameHash.compareTo(ListOfAliveNodes.get(i))<=0) ) {
						originalIndexToStoreFile = i;
						break;
					}
				}
			}

			// check if the fileNameHash was in the correct range, if yes retrieve the content and put in the hashmap<Filename,Filecontent>
			if(indexOfSenderGenHash == originalIndexToStoreFile) {
				n = 0;
				fileContent = new StringBuffer("");

				try {
					fis = getContext().openFileInput(fileName);
					byte[] buffer = new byte[1024];
					while ((n = fis.read(buffer)) != -1) {
						fileContent.append(new String(buffer, 0, n));
					}
				} catch (FileNotFoundException e) {
					Log.e(TAG, "File Not Found To Read for iteration : " + counter);
				} catch (IOException e) {
					Log.e(TAG, "Error Reading File for iteration : " + counter);
				}

				//read one file now store its corresponding values as a new row in matrix cursor
				fileMap.put(fileName, fileContent.toString());
				Log.v(TAG, counter + " : In getOriginalFiles...Got File : " + fileName + " with Content : " + fileContent.toString());
			}


		}
		return fileMap;

	}

	public HashMap<String, String> getReplicatedFiles(String lastPredecessorHash, String secondLastPredecessorHash) {
		HashMap<String, String> fileMap = new HashMap<String, String>();
		String fileName = null;
		FileInputStream fis;
		StringBuffer fileContent = new StringBuffer("");
		int n = 0;
		int indexOfLastPredecessorHash = ListOfAliveNodes.indexOf(lastPredecessorHash);
		int indexOfSecLastPredecessorHash = ListOfAliveNodes.indexOf(secondLastPredecessorHash);
		int originalIndexToStoreFile =0;
		String fileNameHash = null;

		String fileNamesList[] = getContext().fileList();
		int noOfFiles = fileNamesList.length;
		for (int counter = 0; counter < noOfFiles; counter++) {
			fileName = fileNamesList[counter];

			try {
				fileNameHash = genHash(fileName);

			} catch(NoSuchAlgorithmException e ) {
				Log.e(TAG, "In Delete() For filename : No Such Algorithm exception caught ");
				e.printStackTrace();
			}

			if(((fileNameHash.compareTo(ListOfAliveNodes.get(4)))>0) ||(fileNameHash.compareTo(ListOfAliveNodes.get(0))<=0)) {
				//boundary cases where the (key,value) should be stored in first Node
				originalIndexToStoreFile = 0;
			} else {
				for (int i =1 ; i<ListOfAliveNodes.size(); i++) {
					if( (fileNameHash.compareTo(ListOfAliveNodes.get(i-1))>0) && (fileNameHash.compareTo(ListOfAliveNodes.get(i))<=0) ) {
						originalIndexToStoreFile = i;
						break;
					}
				}
			}

			// check if the fileNameHash was in the correct range, if yes retrieve the content and put in the hashmap<Filename,Filecontent>
			if(indexOfLastPredecessorHash == originalIndexToStoreFile || indexOfSecLastPredecessorHash == originalIndexToStoreFile ) {
				n = 0;
				fileContent = new StringBuffer("");

				try {
					fis = getContext().openFileInput(fileName);
					byte[] buffer = new byte[1024];
					while ((n = fis.read(buffer)) != -1) {
						fileContent.append(new String(buffer, 0, n));
					}
				} catch (FileNotFoundException e) {
					Log.e(TAG, "File Not Found To Read for iteration : " + counter);
				} catch (IOException e) {
					Log.e(TAG, "Error Reading File for iteration : " + counter);
				}

				//read one file now store its corresponding values as a new row in matrix cursor
				fileMap.put(fileName, fileContent.toString());
				Log.v(TAG, counter + " : In getReplicatedFiles...Got File : " + fileName + " with Content : " + fileContent.toString());
			}


		}
		return fileMap;

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	/***
	 * ServerTask is an AsyncTask that should handle incoming messages. It is created by
	 * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
	 * <p/>
	 * Please make sure you understand how AsyncTask works by reading
	 * http://developer.android.com/reference/android/os/AsyncTask.html
	 *
	 * @author stevko
	 */
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			while (true) {
				Socket socket = null;
				ObjectInputStream ois = null;

				MessageObj messageRecvd;
				try {
					socket = serverSocket.accept();
					ois = new ObjectInputStream(socket.getInputStream());
					messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
					//ois.close();


					if (msgRecvdType.equalsIgnoreCase(INSERT_ALL)) {

						ObjectOutputStream oos2 = new ObjectOutputStream((socket.getOutputStream()));
						MessageObj msgObj = new MessageObj();
						msgObj.setMsgType(INSERT_ACK);
						msgObj.setSenderNodeID(myNodeId);
						oos2.writeObject(msgObj);
						oos2.flush();
						//oos2.close();

						String keyToInsert   = (String)messageRecvd.getmContentValues().get(KEY_FIELD);
						String valueToInsert = (String)messageRecvd.getmContentValues().get(VALUE_FIELD);
						ContentValues mContentValues = new ContentValues();
						mContentValues.put(KEY_FIELD, keyToInsert);
						mContentValues.put(VALUE_FIELD, valueToInsert);
						Log.e(TAG, "msg to INSERT_ALL received from original sender : " + messageRecvd.getSenderNodeID());
						myInsert(mContentValues);
						//send acknowledgement of insert back to the sender

					} else if (msgRecvdType.equalsIgnoreCase(INSERT_REPLICATE)) {
						ObjectOutputStream oos2 = new ObjectOutputStream((socket.getOutputStream()));
						MessageObj msgObj = new MessageObj();
						msgObj.setMsgType(INSERT_ACK);
						msgObj.setSenderNodeID(myNodeId);
						oos2.writeObject(msgObj);
						oos2.flush();
						//oos2.close();

						String keyToInsert   = (String)messageRecvd.getmContentValues().get(KEY_FIELD);
						String valueToInsert = (String)messageRecvd.getmContentValues().get(VALUE_FIELD);
						ContentValues mContentValues = new ContentValues();
						mContentValues.put(KEY_FIELD, keyToInsert);
						mContentValues.put(VALUE_FIELD, valueToInsert);
						Log.e(TAG, "msg to INSERT_REPLICATE received from original sender : "+messageRecvd.getSenderNodeID());
						myInsert(mContentValues);
						//send acknowledgement of insert back to the sender

					} else if (msgRecvdType.equalsIgnoreCase(DELETE_FILE_FROM_ALL)) {
						ObjectOutputStream oos2 = new ObjectOutputStream((socket.getOutputStream()));
						MessageObj msgObj = new MessageObj();
						msgObj.setMsgType(DELETE_ACK);
						msgObj.setSenderNodeID(myNodeId);
						oos2.writeObject(msgObj);
						oos2.flush();
						//oos2.close();

						// i got this request means i have the original copy or the replicated copy of the file to be deleted
						String queryFile = messageRecvd.getQueryFile();
						String senderNodeId = messageRecvd.getSenderNodeID();
						delete(providerUri, queryFile, new String[]{DELETE_FILE_FROM_ALL, senderNodeId});
						//send acknowledgement of delete back to the sender

					} else if (msgRecvdType.equalsIgnoreCase(DELETE_FILE_REPLICATED)) {
						ObjectOutputStream oos2 = new ObjectOutputStream((socket.getOutputStream()));
						MessageObj msgObj = new MessageObj();
						msgObj.setMsgType(DELETE_ACK);
						msgObj.setSenderNodeID(myNodeId);
						oos2.writeObject(msgObj);
						oos2.flush();
						//oos2.close();

						// i got this request means i have the the replicated copy of the file to be deleted
						String queryFile = messageRecvd.getQueryFile();
						String senderNodeId = messageRecvd.getSenderNodeID();
						delete(providerUri, queryFile, new String[]{DELETE_FILE_REPLICATED, senderNodeId});
						//send acknowledgement of delete back to the sender

					} else if (msgRecvdType.equalsIgnoreCase(DELETE_ALL_MSG)) {
						ObjectOutputStream oos2 = new ObjectOutputStream((socket.getOutputStream()));
						MessageObj msgObj = new MessageObj();
						msgObj.setMsgType(DELETE_ACK);
						msgObj.setSenderNodeID(myNodeId);
						oos2.writeObject(msgObj);
						oos2.flush();
						//oos2.close();

						// i got this request means i have to delete all my local files
						String queryFile = messageRecvd.getQueryFile();
						String senderNodeId = messageRecvd.getSenderNodeID();
						delete(providerUri, queryFile, new String[]{DELETE_ALL_MSG, senderNodeId});
						//send acknowledgement of delete back to the sender

					} else if (msgRecvdType.equalsIgnoreCase(QUERY_FILE)) {
						// i got this request means i have the file for query, need to return the same to the original sender
						String queryFile = messageRecvd.getQueryFile();
						String senderNodeId = messageRecvd.getSenderNodeID();

						int keyIndex = 0, valueIndex=0;
						Cursor resultCursor = query(providerUri, new String[]{senderNodeId}, queryFile, null, null);
						if (resultCursor == null || resultCursor.getCount()==0) {
							Log.e(TAG, "Result null");

						} else {
							keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
							valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

							if (keyIndex == -1 || valueIndex == -1) {
								Log.e(TAG, "Wrong columns");
								resultCursor.close();
							}

							resultCursor.moveToFirst();

							if (!(resultCursor.isFirst() && resultCursor.isLast())) {
								Log.e(TAG, "Wrong number of rows");
								resultCursor.close();
							}

							String returnFileName = resultCursor.getString(keyIndex);
							String returnFileContent = resultCursor.getString(valueIndex);
							Log.e(TAG, "Server task :: QUERY_FILE : returnFileName"+returnFileName + " returnFileContent: "+returnFileContent);

							if(returnFileContent!=null && returnFileContent!="") {
								String[] queryReplyMsg = new String[4];
								queryReplyMsg[0] = msgRecvdType;
								queryReplyMsg[1] = returnFileName;
								queryReplyMsg[2] = returnFileContent;
								queryReplyMsg[3] = senderNodeId;
								publishProgress(queryReplyMsg);
							}
							Log.e(TAG, "QUERY_FILE block progressUpdate Ends");
						}
					} else if (msgRecvdType.equalsIgnoreCase(QUERY_FILE_REPLY)) {
						// means i have been replied with key,value for my original query
						// put the values in the hashmap to exit from the waiting while loop
						String[] queryReplyMsg = new String[3];
						queryReplyMsg[0] = msgRecvdType;
						queryReplyMsg[1] = (String)messageRecvd.getmContentValues().get(KEY_FIELD);
						queryReplyMsg[2] = (String)messageRecvd.getmContentValues().get(VALUE_FIELD);

						Log.e(TAG, "Executed QUERY_FILE_REPLY NODE (ServerTask)putting value in queryMap : " + queryReplyMsg[2] + " for file : "+queryReplyMsg[1]);


						queryMap.put(queryReplyMsg[1], queryReplyMsg[2]);
					}else if (msgRecvdType.equalsIgnoreCase(QUERY_ALL_MSG)) {
						// i got this request means i have to query all my local files and return to the original sender who got * request
						// 0 - msgType, 1 - senderNodeID , 2- selection Param
						String[] queryAllMsg = new String[3];
						queryAllMsg[0] = msgRecvdType;
						queryAllMsg[1] = messageRecvd.getSenderNodeID();;
						queryAllMsg[2] = messageRecvd.getQueryFile();

						Log.e(TAG, "Executed QUERY_ALL_MSG NODE Block in ServerTask...MsgType : " + queryAllMsg[0] + "Selection Param : "+queryAllMsg[2]);
						Log.e(TAG, "Executed QUERY_ALL_MSG NODE Block in ServerTask...Original Sender : " + queryAllMsg[1]);
						publishProgress(queryAllMsg);

					}else if (msgRecvdType.equalsIgnoreCase(QUERY_ALL_REPLY)) {

						HashMap<String,String> map = (HashMap)messageRecvd.getmContentValues();
						String whoSentReplyAllMsg = messageRecvd.getWhoSentReplyAll();

						if(map==null) {
							//this avd who replied doesnt have any keys stored so don't count it
							// NoOfRepliesExpected--;
							Log.e(TAG, "IF map == null QUERY_ALL_REPLY NODE Block in ServerTask...I am here : " + NoOfRepliesExpected);

						}

						queryAllMap.put(whoSentReplyAllMsg,map);
						queryAllCount++;
						Log.e(TAG, "Executed QUERY_ALL_REPLY NODE Block in ServerTask...MsgType : " + msgRecvdType);
						Log.e(TAG, "Executed QUERY_ALL_REPLY NODE Block in ServerTask...whoSentReplyAllMsg : " + whoSentReplyAllMsg);
						Log.e(TAG, "Executed QUERY_ALL_REPLY NODE Block in ServerTask...map : " + map);
						Log.e(TAG, "Executed QUERY_ALL_REPLY NODE Block in ServerTask...NoOfRepliesExpected : " + NoOfRepliesExpected);

					} else if (msgRecvdType.equalsIgnoreCase(RECOVERY_GET_ORIGINAL)) {
						String senderOfMsg = messageRecvd.getSenderNodeID();
						String receiverOfMsg = messageRecvd.getReceiverForMsg();
						String senderGenHash = null;
						try {
							senderGenHash = genHash(senderOfMsg);
						} catch (NoSuchAlgorithmException e) {
							Log.e(TAG, "On create : No Such Algorithm exception caught ");
							e.printStackTrace();
						}

						String[] getOrigMsg = new String[4];
						getOrigMsg[0] = msgRecvdType;
						getOrigMsg[1] = senderOfMsg;;
						getOrigMsg[2] = receiverOfMsg;
						getOrigMsg[3] = senderGenHash;


						publishProgress(getOrigMsg);

					} else if (msgRecvdType.equalsIgnoreCase(RECOVERY_GET_REPLICATED)) {
						String senderOfMsg = messageRecvd.getSenderNodeID();
						String immediatePredec1 = messageRecvd.getReceiverForMsg(); //sender's immediate predecessor
						String immediatePredec2 = messageRecvd.getSuccessor1();
						String immediatePredec1GenHash = null;
						String immediatePredec2GenHash = null;
						try {
							immediatePredec1GenHash = genHash(immediatePredec1);
							immediatePredec2GenHash = genHash(immediatePredec2);
						} catch (NoSuchAlgorithmException e) {
							Log.e(TAG, "On create : No Such Algorithm exception caught ");
							e.printStackTrace();
						}

						String[] getReplicaMsg = new String[6];
						getReplicaMsg[0] = msgRecvdType;
						getReplicaMsg[1] = senderOfMsg;;
						getReplicaMsg[2] = immediatePredec1;
						getReplicaMsg[3] = immediatePredec2;
						getReplicaMsg[4] = immediatePredec1GenHash;
						getReplicaMsg[5] = immediatePredec2GenHash;

						publishProgress(getReplicaMsg);

					}  else if (msgRecvdType.equalsIgnoreCase(SEND_ORIG_FILE)) {
						// after recovery, i got the original files i need to store
						HashMap fileMap = messageRecvd.getmContentValues();
						// now iterate the Map and store the files & its content in ur filesystem
						InsertOriginalFiles(fileMap);

					}  else if (msgRecvdType.equalsIgnoreCase(SEND_REPLICA_FILE)) {
						// after recovery , i got the replicated files i need to store
						HashMap fileMap = messageRecvd.getmContentValues();
						// now iterate the Map and store the files & its content in ur filesystem
						InsertOriginalFiles(fileMap);
					}
					//socket.close();

				} catch (IOException e) {
					e.printStackTrace();

				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		protected void onProgressUpdate(String... strings) {

			String msgTypeReceived = strings[0];
			if (msgTypeReceived.equalsIgnoreCase(QUERY_FILE)) {
				String senderNodeId = strings[3];
				String fileName = strings[1];
				String fileContent = strings[2];

				MessageObj msgObj = new MessageObj();
				msgObj.setMsgType(QUERY_FILE_REPLY);
				msgObj.setReceiverForMsg(senderNodeId);
				msgObj.setmContentValues(fileName, fileContent);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
				Log.e(TAG, "QUERY_FILE_REPLY send to Client task with msg" + msgObj.toString());

			}else if (msgTypeReceived.equalsIgnoreCase(QUERY_ALL_MSG)) {
				// means i have been forwarded a query request with @ from my predecessor node
				//call query and retrieve all the files stored in ur local avd & return as a HashMap of <File,FileContent> to the original sender
				// 0 - msgType, 1 - senderNodeID , 2- selection Param
				String senderNodeId = strings[1];
				String selectionParam   = strings[2];

				int keyIndex = 0, valueIndex=0;

				Cursor resultCursor = query(providerUri, new String[]{QUERY_ALL_MSG,senderNodeId}, selectionParam, null, null);
				if (resultCursor == null || resultCursor.getCount()<=0) {
					Log.e(TAG, "Result null null null nulll nulll");
					MessageObj msgObj = new MessageObj();
					msgObj.setMsgType(QUERY_ALL_REPLY);
					msgObj.setmContentValues(null);
					msgObj.setReceiverForMsg(senderNodeId); // sending back to the original sender of the query
					msgObj.setWhoSentReplyAll(myNodeId);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
					Log.e(TAG, "QUERY_ALL_REPLY send to Client task with msg"+msgObj.toString());
					Log.e(TAG, "QUERY_ALL_REPLY block progressUpdate Ends");



				} else {
					keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
					valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

					if (keyIndex == -1 || valueIndex == -1) {
						Log.e(TAG, "Wrong columns");
						resultCursor.close();
					}

					resultCursor.moveToFirst();

					String returnFileName = null;
					String returnFileContent = null;
					int i = 0 ;
					HashMap<String,String> fileMap = new HashMap<String,String>();

					Log.e(TAG, "QUERY_ALL_MSG : Iterating Over the result cursor");

					// handle the first row of cursor explicitly , then move to Next rows....
					returnFileName = resultCursor.getString(keyIndex);
					returnFileContent = resultCursor.getString(valueIndex);
					Log.e(TAG, "QUERY_ALL_MSG :Iteration: "+i+" returnFileName"+returnFileName + " returnFileContent: "+returnFileContent);
					if(returnFileContent!=null && returnFileContent!="") {
						fileMap.put(returnFileName,returnFileContent);
					}

					while(resultCursor.moveToNext()) {
						i++;
						returnFileName = resultCursor.getString(keyIndex);
						returnFileContent = resultCursor.getString(valueIndex);
						Log.e(TAG, "QUERY_ALL_MSG :Iteration: "+i+" returnFileName"+returnFileName + " returnFileContent: "+returnFileContent);
						if(returnFileContent!=null && returnFileContent!="") {
							fileMap.put(returnFileName,returnFileContent);
						}
					}
					MessageObj msgObj = new MessageObj();
					msgObj.setMsgType(QUERY_ALL_REPLY);
					msgObj.setmContentValues(fileMap);
					msgObj.setWhoSentReplyAll(myNodeId);
					msgObj.setReceiverForMsg(senderNodeId); // sending back to the original sender of the query
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
					Log.e(TAG, "QUERY_ALL_REPLY send to Client task with msg"+msgObj.toString());
					Log.e(TAG, "QUERY_ALL_REPLY block progressUpdate Ends");
				}

			} else if (msgTypeReceived.equalsIgnoreCase(RECOVERY_GET_ORIGINAL)) {
				String senderNodeId = strings[1];
				String receiverNodeId   = strings[2];
				String senderGenHash = strings[3];

				//Now i need to retrieve all the messages which are originally of sender but replicated in this avd
				HashMap<String,String> originalFilesMap = getOriginalFiles(senderGenHash);
				MessageObj msgObj = new MessageObj();
				msgObj.setMsgType(SEND_ORIG_FILE);
				msgObj.setReceiverForMsg(senderNodeId);
				msgObj.setSenderNodeID(receiverNodeId);
				msgObj.setmContentValues(originalFilesMap);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
				Log.e(TAG, "SEND_ORIG_FILE send to Client task with msg" + msgObj.toString());
				Log.e(TAG, "SEND_ORIG_FILE block progressUpdate Ends");


			} else if (msgTypeReceived.equalsIgnoreCase(RECOVERY_GET_REPLICATED)) {

				String senderNodeId = strings[1];
				String immediatePredec1   = strings[2];
				String immediatePredec2 = strings[3];
				String immediatePredec1GenHash   = strings[4];
				String immediatePredec2GenHash = strings[5];

				//Now i need to retrieve all the messages which are originally of sender but replicated in this avd
				HashMap<String,String> replicatedFilesMap = getReplicatedFiles(immediatePredec1GenHash,immediatePredec2GenHash);
				MessageObj msgObj = new MessageObj();
				msgObj.setMsgType(SEND_REPLICA_FILE);
				msgObj.setReceiverForMsg(senderNodeId);
				msgObj.setSenderNodeID(immediatePredec1);
				msgObj.setmContentValues(replicatedFilesMap);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgObj);
				Log.e(TAG, "SEND_REPLICA_FILE send to Client task with msg" + msgObj.toString());
				Log.e(TAG, "SEND_REPLICA_FILE block progressUpdate Ends");
			}
		}
	}



	/***
	 * ClientTask is an AsyncTask that should send a string over the network.
	 * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
	 * an enter key press event.
	 *
	 * @author stevko
	 */
	private class ClientTask extends AsyncTask<MessageObj, Void, Void> {

		@Override
		protected Void doInBackground(MessageObj... msgs) {

			MessageObj msgObj = msgs[0];
			if (msgObj.getMsgType().equalsIgnoreCase(INSERT_ALL))  {

			try {

				Log.e(TAG, "ClientTask() : Sending INSERT_ALL msg to Original Receiver : " + msgObj.getReceiverForMsg());
				int ReceiverPort1 = Integer.parseInt(msgObj.getReceiverForMsg()) * 2;
				Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						ReceiverPort1);
				ObjectOutputStream oos = new ObjectOutputStream((socket1.getOutputStream()));
				oos.writeObject(msgObj);
				oos.flush();
				//oos.close();

				//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
				socket1.setSoTimeout(1500);

				ObjectInputStream ois = new ObjectInputStream(socket1.getInputStream());
				MessageObj messageRecvd = (MessageObj) ois.readObject();
				String msgRecvdType = messageRecvd.getMsgType();
				Log.e(TAG, "Got acknowledgement for " + msgRecvdType+" msg from"+messageRecvd.getSenderNodeID());
				//ois.close();

				//socket1.close();
			}  catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException ( INSERT_ALL)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL) ");
				// which node has failed...
				String failedNode = msgObj.getReceiverForMsg();
				// increase the queryAllCount by 1 so the query *doesn't wait for the response from failed node!
				// here check if you are waiting for query * responses..if yes..have u got the response from failed avd..then
				//querycount remains the same...else no need to wait for its response..so increment querycount by 1
				if(queryAllMap == null) {
					// means i have no query * request pending...so do nothing
				} else {
					// means i have a pending query * operation which is waiting for responses...
					//now check if u have got the response from the failed avd
					if(queryAllMap.get(failedNode)!=null) {
						// yes i already got the response from failed avd so continue the normal flow
					} else {
						queryAllCount = queryAllCount + 1;
					}
				}

				Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New queryAllCount = "+queryAllCount);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception ( INSERT_ALL)");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			try{


				Log.e(TAG, "ClientTask() : Sending INSERT_ALL msg to Successor 1 : " + msgObj.getSuccessor1());
				int ReceiverPort2 = Integer.parseInt(msgObj.getSuccessor1())*2;
				Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						ReceiverPort2);
				ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
				oos2.writeObject(msgObj);
				oos2.flush();
				//oos2.close();

				//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
				socket2.setSoTimeout(1500);

				ObjectInputStream ois = new ObjectInputStream(socket2.getInputStream());
				MessageObj messageRecvd = (MessageObj) ois.readObject();
				String msgRecvdType = messageRecvd.getMsgType();
				Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
				//ois.close();

				//socket2.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException ( INSERT_ALL)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL) ");
				// which node has failed...
				String failedNode = msgObj.getSuccessor1();
				if(queryAllMap == null) {
					// means i have no query * request pending...so do nothing
				} else {
					// means i have a pending query * operation which is waiting for responses...
					//now check if u have got the response from the failed avd
					if(queryAllMap.get(failedNode)!=null) {
						// yes i already got the response from failed avd so continue the normal flow
					} else {
						queryAllCount = queryAllCount + 1;
					}
				}
				Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New queryAllCount = "+queryAllCount);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception ( INSERT_ALL)");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			try {
					Log.e(TAG, "ClientTask() : Sending INSERT_ALL msg to Successor 2 : " + msgObj.getSucessor2());
					int ReceiverPort3 = Integer.parseInt(msgObj.getSucessor2())*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream oos3 = new ObjectOutputStream((socket3.getOutputStream()));
				oos3.writeObject(msgObj);
					oos3.flush();
					//oos3.close();

					//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
					socket3.setSoTimeout(1500);

					ObjectInputStream ois = new ObjectInputStream(socket3.getInputStream());
					MessageObj messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
				Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
					//ois.close();

					//socket3.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "Client Task UnknownHostException ( INSERT_ALL)");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL) ");
				// which node has failed...
				String failedNode = msgObj.getSucessor2();
				if(queryAllMap == null) {
					// means i have no query * request pending...so do nothing
				} else {
					// means i have a pending query * operation which is waiting for responses...
					//now check if u have got the response from the failed avd
					if(queryAllMap.get(failedNode)!=null) {
						// yes i already got the response from failed avd so continue the normal flow
					} else {
						queryAllCount = queryAllCount + 1;
					}
				}
				Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New queryAllCount = "+queryAllCount);

			} catch (NullPointerException e) {
				Log.e(TAG, "Client task socket Null Pointer Exception ( INSERT_ALL)");
			}
			catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			} else if (msgObj.getMsgType().equalsIgnoreCase(INSERT_REPLICATE))  {

				try {
					Log.e(TAG, "ClientTask() : Sending INSERT_ALL msg to Successor 1 : " + msgObj.getSuccessor1());
					int ReceiverPort2 = Integer.parseInt(msgObj.getSuccessor1())*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();

					//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
					socket2.setSoTimeout(1500);

					ObjectInputStream ois = new ObjectInputStream(socket2.getInputStream());
					MessageObj messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
					Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
					//ois.close();
					//socket2.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException ( INSERT_REPLICATE)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...( INSERT_REPLICATE) ");
					// which node has failed...
					String failedNode = msgObj.getSuccessor1();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New queryAllCount = "+queryAllCount);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception ( INSERT_REPLICATE)");
				}  catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				try {
					Log.e(TAG, "ClientTask() : Sending INSERT_ALL msg to Successor 2 : " + msgObj.getSucessor2());
					int ReceiverPort3 = Integer.parseInt(msgObj.getSucessor2())*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream oos3 = new ObjectOutputStream((socket3.getOutputStream()));
					oos3.writeObject(msgObj);
					oos3.flush();
					//oos3.close();

					//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
					socket3.setSoTimeout(1500);

					ObjectInputStream ois = new ObjectInputStream(socket3.getInputStream());
					MessageObj messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
					Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
					//ois.close();

					//socket3.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException ( INSERT_REPLICATE)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...( INSERT_REPLICATE) ");
					// which node has failed...
					String failedNode = msgObj.getSucessor2();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New queryAllCount = "+queryAllCount);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception ( INSERT_REPLICATE)");
				}  catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			} else if (msgObj.getMsgType().equalsIgnoreCase(DELETE_FILE_FROM_ALL))  {

				try {

					Log.e(TAG, "ClientTask() : Sending DELETE_FROM_ALL msg to Original Receiver : " + msgObj.getReceiverForMsg());
					int ReceiverPort1 = Integer.parseInt(msgObj.getReceiverForMsg())*2;
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort1);
					ObjectOutputStream oos = new ObjectOutputStream((socket1.getOutputStream()));
					oos.writeObject(msgObj);
					oos.flush();
					//oos.close();
					//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
					socket1.setSoTimeout(1500);

					ObjectInputStream ois = new ObjectInputStream(socket1.getInputStream());
					MessageObj messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
					Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
					//ois.close();

					//socket1.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException ( DELETE_FROM_ALL)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...( DELETE_FROM_ALL) ");
					// which node has failed...
					String failedNode = msgObj.getReceiverForMsg();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New queryAllCount = "+queryAllCount);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception ( DELETE_FROM_ALL)");
				}  catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				try {
					Log.e(TAG, "ClientTask() : Sending DELETE_FROM_ALL msg to Successor 1 : " + msgObj.getSuccessor1());
					int ReceiverPort2 = Integer.parseInt(msgObj.getSuccessor1())*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();

					//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
					socket2.setSoTimeout(1500);

					ObjectInputStream ois = new ObjectInputStream(socket2.getInputStream());
					MessageObj messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
					Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
					//ois.close();
					//socket2.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException ( DELETE_FROM_ALL)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...( DELETE_FROM_ALL) ");
					// which node has failed...
					String failedNode = msgObj.getSuccessor1();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New queryAllCount = "+queryAllCount);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception ( DELETE_FROM_ALL)");
				}  catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				try {
					Log.e(TAG, "ClientTask() : Sending DELETE_FROM_ALL msg to Successor 2 : " + msgObj.getSucessor2());
					int ReceiverPort3 = Integer.parseInt(msgObj.getSucessor2())*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream oos3 = new ObjectOutputStream((socket3.getOutputStream()));
					oos3.writeObject(msgObj);
					oos3.flush();
					//oos3.close();

					//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
					socket3.setSoTimeout(1500);

					ObjectInputStream ois = new ObjectInputStream(socket3.getInputStream());
					MessageObj messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
					Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
					//ois.close();

					//socket3.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException ( DELETE_FROM_ALL)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...( DELETE_FROM_ALL) ");
					// which node has failed...
					String failedNode = msgObj.getSucessor2();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception ( DELETE_FROM_ALL)");
				}  catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			} else if (msgObj.getMsgType().equalsIgnoreCase(DELETE_FILE_REPLICATED))  {

				try {
					Log.e(TAG, "ClientTask() : Sending DELETE_REPLICATED msg to Successor 1 : " + msgObj.getSuccessor1());
					int ReceiverPort2 = Integer.parseInt(msgObj.getSuccessor1())*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();

					//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
					socket2.setSoTimeout(1500);

					ObjectInputStream ois = new ObjectInputStream(socket2.getInputStream());
					MessageObj messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
					Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
					//ois.close();

					//socket2.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (DELETE_REPLICATED)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(DELETE_REPLICATED) ");
					// which node has failed...
					String failedNode = msgObj.getSuccessor1();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (DELETE_REPLICATED)");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}


				try {
					Log.e(TAG, "ClientTask() : Sending DELETE_REPLICATED msg to Successor 2 : " + msgObj.getSucessor2());
					int ReceiverPort3 = Integer.parseInt(msgObj.getSucessor2())*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream oos3 = new ObjectOutputStream((socket3.getOutputStream()));
					oos3.writeObject(msgObj);
					oos3.flush();
					//oos3.close();
					//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
					socket3.setSoTimeout(1500);

					ObjectInputStream ois = new ObjectInputStream(socket3.getInputStream());
					MessageObj messageRecvd = (MessageObj) ois.readObject();
					String msgRecvdType = messageRecvd.getMsgType();
					Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
					//ois.close();

				//	socket3.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (DELETE_REPLICATED)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(DELETE_REPLICATED) ");
					// which node has failed...
					String failedNode = msgObj.getSucessor2();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);
				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (DELETE_REPLICATED)");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			} else if (msgObj.getMsgType().equalsIgnoreCase(DELETE_ALL_MSG))  {


					int i = 0;
					for (String REMOTE_PORT : REMOTE_PORTS) {
						try {
							if (REMOTE_PORT.equals(myPort)) {
								continue;
							} else {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(REMOTE_PORT));
								ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
								oos.writeObject(msgObj);
								Log.e(TAG, "DELETE_ALL_MSG SENT TO: " + REMOTE_PORT + "No of NEW MSG Sent :" + (i + 1));
								oos.flush();
								//oos.close();

								//wait for the acknowledgement request here before closing the socket, if not received there will be a timeout meaning node has failed.
								socket.setSoTimeout(1500);

								ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
								MessageObj messageRecvd = (MessageObj) ois.readObject();
								String msgRecvdType = messageRecvd.getMsgType();
								Log.e(TAG, "Got acknowledgement for " + msgRecvdType + " msg from" + messageRecvd.getSenderNodeID());
								//ois.close();
								//socket.close();
								i++;
							}
						} catch (UnknownHostException e) {
							Log.e(TAG, "Client Task UnknownHostException (DELETE_ALL_MSG)");
						} catch (IOException e) {
							Log.e(TAG, "ClientTask socket IOException...(DELETE_ALL_MSG) ");
							// which node has failed...
							String failedNode = (Integer.parseInt(REMOTE_PORT))/2+"";
							if(queryAllMap == null) {
								// means i have no query * request pending...so do nothing
							} else {
								// means i have a pending query * operation which is waiting for responses...
								//now check if u have got the response from the failed avd
								if(queryAllMap.get(failedNode)!=null) {
									// yes i already got the response from failed avd so continue the normal flow
								} else {
									queryAllCount = queryAllCount + 1;
								}
							}
							Log.e(TAG, "ClientTask socket IOException...( INSERT_ALL).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);

						} catch (NullPointerException e) {
							Log.e(TAG, "Client task socket Null Pointer Exception (DELETE_ALL_MSG)");
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
					}
			} else if (msgObj.getMsgType().equalsIgnoreCase(QUERY_FILE))  {

				try {
					Log.e(TAG, "ClientTask() : Sending QUERY_FILE msg to Original Node with File : " + msgObj.getReceiverForMsg());
					int ReceiverPort2 = Integer.parseInt(msgObj.getReceiverForMsg()) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();
					//socket2.close();
				}  catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (QUERY_FILE)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(QUERY_FILE) ");
					// which node has failed...
					String failedNode = msgObj.getReceiverForMsg();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( QUERY_FILE).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (QUERY_FILE)");
				}
				try {
					Log.e(TAG, "ClientTask() : Sending QUERY_FILE msg to Successor 1 : " + msgObj.getSuccessor1());
					int ReceiverPort1 = Integer.parseInt(msgObj.getSuccessor1())*2;
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort1);
					ObjectOutputStream oos1 = new ObjectOutputStream((socket1.getOutputStream()));
					oos1.writeObject(msgObj);
					oos1.flush();
					//oos1.close();
					//socket1.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (QUERY_FILE)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(QUERY_FILE) ");
					// which node has failed...
					String failedNode = msgObj.getSuccessor1();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( QUERY_FILE).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (QUERY_FILE)");
				}
				try {
					Log.e(TAG, "ClientTask() : Sending QUERY_FILE msg to Successor 2 : " + msgObj.getSucessor2());
					int ReceiverPort3 = Integer.parseInt(msgObj.getSucessor2())*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream oos3 = new ObjectOutputStream((socket3.getOutputStream()));
					oos3.writeObject(msgObj);
					oos3.flush();
					//oos3.close();
					//socket3.close();


				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (QUERY_FILE)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(QUERY_FILE) ");
					// which node has failed...
					String failedNode = msgObj.getSucessor2();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( QUERY_FILE).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (QUERY_FILE)");
				}
			} else if (msgObj.getMsgType().equalsIgnoreCase(QUERY_FILE_REPLY))  {

				try {
					Log.e(TAG, "ClientTask() : Sending QUERY_FILE_REPLY msg to Node : " + msgObj.getReceiverForMsg());
					int ReceiverPort2 = Integer.parseInt(msgObj.getReceiverForMsg())*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();
					//socket2.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (QUERY_FILE_REPLY)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(QUERY_FILE_REPLY) ");
					// which node has failed...
					String failedNode = msgObj.getReceiverForMsg();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( QUERY_FILE_REPLY).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (QUERY_FILE_REPLY)");
				}
			} else if (msgObj.getMsgType().equalsIgnoreCase(QUERY_ALL_MSG))  {


				int i = 0;
				for (String REMOTE_PORT : REMOTE_PORTS) {
					try {
						if (REMOTE_PORT.equals(myPort)) {
							continue;
						} else {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(REMOTE_PORT));
							ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
							oos.writeObject(msgObj);
							Log.e(TAG, "QUERY_ALL_MSG SENT TO: " + REMOTE_PORT + "No of NEW MSG Sent :" + (i + 1));
							oos.flush();
							//oos.close();
							//socket.close();
							i++;
						}
					} catch (UnknownHostException e) {
						Log.e(TAG, "Client Task UnknownHostException (QUERY_ALL_MSG)");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException...(QUERY_ALL_MSG) ");
						// which node has failed...
						String failedNode = (Integer.parseInt(REMOTE_PORT)/2)+"";
						if(queryAllMap == null) {
							// means i have no query * request pending...so do nothing
						} else {
							// means i have a pending query * operation which is waiting for responses...
							//now check if u have got the response from the failed avd
							if(queryAllMap.get(failedNode)!=null) {
								// yes i already got the response from failed avd so continue the normal flow
							} else {
								queryAllCount = queryAllCount + 1;
							}
						}
						Log.e(TAG, "ClientTask socket IOException...( QUERY_ALL_MSG).... FailedNode :  "+failedNode + " New NoOfRepliesExpected = "+NoOfRepliesExpected);

					} catch (NullPointerException e) {
						Log.e(TAG, "Client task socket Null Pointer Exception (QUERY_ALL_MSG)");
					}
				}
			}  else if (msgObj.getMsgType().equalsIgnoreCase(QUERY_ALL_REPLY))  {

				try {
					Log.e(TAG, "ClientTask() : Sending QUERY_FILE_REPLY msg to Node : " + msgObj.getReceiverForMsg());
					int ReceiverPort2 = Integer.parseInt(msgObj.getReceiverForMsg())*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();
					//socket2.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (QUERY_ALL_REPLY)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(QUERY_ALL_REPLY) ");
					// which node has failed...
					String failedNode = msgObj.getReceiverForMsg();
					if(queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if(queryAllMap.get(failedNode)!=null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( QUERY_ALL_REPLY).... FailedNode :  " + failedNode + " New NoOfRepliesExpected = " + NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (QUERY_ALL_REPLY)");
				}
			} else if (msgObj.getMsgType().equalsIgnoreCase(RECOVERY_GET_ORIGINAL)) {

				try {
					Log.e(TAG, "ClientTask() : Sending RECOVERY_GET_ORIGINAL msg to Node : " + msgObj.getReceiverForMsg());
					int ReceiverPort2 = Integer.parseInt(msgObj.getReceiverForMsg()) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();
					//socket2.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (RECOVERY_GET_ORIGINAL)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(RECOVERY_GET_ORIGINAL) ");
					// which node has failed...
					String failedNode = msgObj.getReceiverForMsg();
					if (queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if (queryAllMap.get(failedNode) != null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( RECOVERY_GET_ORIGINAL).... FailedNode :  " + failedNode + " New NoOfRepliesExpected = " + NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (RECOVERY_GET_ORIGINAL)");
				}
			} else if (msgObj.getMsgType().equalsIgnoreCase(RECOVERY_GET_REPLICATED)) {

				try {
					Log.e(TAG, "ClientTask() : Sending RECOVERY_GET_REPLICATED msg to Node : " + msgObj.getReceiverForMsg());
					int ReceiverPort2 = Integer.parseInt(msgObj.getReceiverForMsg()) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();
					//socket2.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (RECOVERY_GET_REPLICATED)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(RECOVERY_GET_REPLICATED) ");
					// which node has failed...
					String failedNode = msgObj.getReceiverForMsg();
					if (queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if (queryAllMap.get(failedNode) != null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( RECOVERY_GET_REPLICATED).... FailedNode :  " + failedNode + " New NoOfRepliesExpected = " + NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (RECOVERY_GET_REPLICATED)");
				}
			} else if (msgObj.getMsgType().equalsIgnoreCase(SEND_ORIG_FILE)) {

				try {
					Log.e(TAG, "ClientTask() : Sending SEND_ORIG_FILE msg to Node : " + msgObj.getReceiverForMsg());
					int ReceiverPort2 = Integer.parseInt(msgObj.getReceiverForMsg()) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();
					//socket2.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (SEND_ORIG_FILE)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(SEND_ORIG_FILE) ");
					// which node has failed...
					String failedNode = msgObj.getReceiverForMsg();
					if (queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if (queryAllMap.get(failedNode) != null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( SEND_ORIG_FILE).... FailedNode :  " + failedNode + " New NoOfRepliesExpected = " + NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (SEND_ORIG_FILE)");
				}
			} else if (msgObj.getMsgType().equalsIgnoreCase(SEND_REPLICA_FILE)) {

				try {
					Log.e(TAG, "ClientTask() : Sending SEND_REPLICA_FILE msg to Node : " + msgObj.getReceiverForMsg());
					int ReceiverPort2 = Integer.parseInt(msgObj.getReceiverForMsg()) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream oos2 = new ObjectOutputStream((socket2.getOutputStream()));
					oos2.writeObject(msgObj);
					oos2.flush();
					//oos2.close();
					//socket2.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "Client Task UnknownHostException (SEND_REPLICA_FILE)");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException...(SEND_REPLICA_FILE) ");
					// which node has failed...
					String failedNode = msgObj.getReceiverForMsg();
					if (queryAllMap == null) {
						// means i have no query * request pending...so do nothing
					} else {
						// means i have a pending query * operation which is waiting for responses...
						//now check if u have got the response from the failed avd
						if (queryAllMap.get(failedNode) != null) {
							// yes i already got the response from failed avd so continue the normal flow
						} else {
							queryAllCount = queryAllCount + 1;
						}
					}
					Log.e(TAG, "ClientTask socket IOException...( SEND_REPLICA_FILE).... FailedNode :  " + failedNode + " New NoOfRepliesExpected = " + NoOfRepliesExpected);

				} catch (NullPointerException e) {
					Log.e(TAG, "Client task socket Null Pointer Exception (SEND_REPLICA_FILE)");
				}
			}

			return null;
		}
	}


	private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
