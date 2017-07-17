package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.R.attr.order;
import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	String myport;
	static final int SERVER_PORT = 10000;
	String TAG ="TAG";
	String my_id="";
	List<String> ports = new ArrayList<String>();
	HashMap<String,Integer> map=new HashMap<String,Integer>();
	ArrayList<String> backup = new ArrayList<String>();


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@") || selection.equals("*")){
			String[] files=getContext().fileList();
			for(String file: files){
				getContext().deleteFile(file);
			}
		}
		else{
			getContext().deleteFile(selection);
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key= (String) values.get("key");


		String val= (String) values.get("value");

		String tokens [] = val.split("#");

		Log.i("tokens_length",Integer.toString(tokens.length));

		if(tokens.length==2){
		//	Log.i("inserting_tokens","inserting_tokens");
			String filename =(String) values.get("key");
			val = tokens[1];
			FileOutputStream outputStream;

			try {
				outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
				outputStream.write(val.getBytes());
				outputStream.close();
			} catch (Exception e) {

				Log.e(TAG, "File write failed");
			}
			return uri;
		}

	 else {

			String hash_key="";
			int sz=ports.size();

		try {
			 hash_key =genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if(hash_key.compareTo(ports.get(0))<0 || hash_key.compareTo(ports.get(4))>0){
			String msg="";
			msg+= "insert_0";
			msg+= "#";
			msg+= key;
			msg+="#";
			msg+= (String) values.get("value");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
		}
		else {
			for (int i = 0; i < sz; i++) {
                   if(hash_key.compareTo(ports.get(i))<0){
					   String msg="";
					   msg+= "insert";
					   msg+= "#";
					   msg+=key;
					   msg+="#";
					   msg+= (String) values.get("value");
					   msg+="#";
					   msg+=Integer.toString(i);
					   new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
					   break;
				   }
			}
		}
		}

		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel=(TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myport = String.valueOf((Integer.parseInt(portStr)*2 ));

		try {
			my_id = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		int arr[] = {5554, 5556, 5558, 5560, 5562};

		for(int p=0;p<=4;p++){
			try {
				String hash=genHash(Integer.toString(arr[p]));
				map.put(hash,arr[p]*2);
				ports.add(hash);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(ports);
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
	//	String[] files=getContext().fileList();
	//	if(files.length!=0){
	//		Log.i("failed_on_create","failed on create");

			String msg="";
			msg+="failed_node#1";
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
	//	}
		return false;
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			Socket clientSocket = null;

			while(true) {
				try {
					clientSocket = serverSocket.accept();
					BufferedReader in = null;
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
					in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					String msg = in.readLine();
					if(msg==null){
						out.println("ok");
						clientSocket.close();
					}
					String tokens[]=msg.split("#");

					if(tokens[0].equals("insert")){
					//	Log.i("inserting_2","insert_2");
						Uri uri;
						Uri.Builder uriBuilder = new Uri.Builder();
						uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
						uriBuilder.scheme("content");
						uri = uriBuilder.build();
						ContentResolver cc;
						cc = getContext().getContentResolver();
						ContentValues cv = new ContentValues();
						String str="insert";
						str+="#";
						str+=tokens[2];
						cv.put("key", tokens[1]);
						cv.put("value", str);
						insert(uri,cv);
						out.println("ok");
						out.flush();
						clientSocket.close();
					}
					else if(tokens[0].equals("query")){
						msg="";
						String[] files=getContext().fileList();
						for(String file : files) {
							String str = "";
							FileInputStream inputStream;
							try {
								inputStream = getContext().openFileInput(file);
								int k = inputStream.read();
								while (k != -1) {
									str += String.valueOf((char) k);
									k = inputStream.read();
								}
								inputStream.close();
							} catch (Exception e) {
								Log.e(TAG, "File write failed");
							}
							msg+=file;
							msg+="#";
							msg+=str;
							msg+="#";
						}
						out.println(msg);
						out.flush();
						String str =in.readLine();
						if(str!=null && str.equals("ok"))clientSocket.close();
					}
					else if(tokens[0].equals("query_1")){
						String str="";
						FileInputStream inputStream;
						inputStream = getContext().openFileInput(tokens[1]);
						int k = inputStream.read();
						while (k != -1) {
							str += String.valueOf((char) k);
							k = inputStream.read();
						}
						inputStream.close();
						 msg="";
						msg+=str;
						out.println(msg);
						out.flush();
						String temp=in.readLine();
						if(temp!=null&& temp.equals("ok"))clientSocket.close();
					}
					else if(tokens[0].equals("failed_node")){
                    //      Log.i("failed_node","I'm a failed node");
						msg="";
						  for(int i=0;i<backup.size();i++){
							  msg+=backup.get(i);
							  msg+="#";
						  }
						backup.clear();
						Log.i("backup_msg",msg);
						out.println(msg);
						if(in.readLine().equals("ok"))clientSocket.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	private class ClientTask extends AsyncTask<String, Void, String> {
		@Override
		protected String doInBackground(String... msgs) {

			String msg = msgs[0];
			String temp_1=msg;

			String tokens[] = msg.split("#");

			try {
				if(tokens[0].equals("failed_node")){
				//	Log.i("failed_node_1","I'm .....a...failed.....node");
					for(int i=0;i<=4;i++) {
						if (ports.get(i).equals(my_id)) continue;
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								map.get(ports.get(i)));
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						out.println("failed_node#1");
						String str=in.readLine();
						out.println("ok");
                        if(str==null || str.equals(""))continue;
						Log.i("token_str_1",str);
						String temp=str;
						String tokens1[] = str.split("#");
						Log.i("token_str",Integer.toString(tokens1.length));

						for(int k=0;k<tokens1.length;k++){
							if(k%2==0)continue;
							Uri uri;
							Uri.Builder uriBuilder = new Uri.Builder();
							uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
							uriBuilder.scheme("content");
							uri = uriBuilder.build();
							ContentResolver cc;
							cc = getContext().getContentResolver();
							ContentValues cv = new ContentValues();
							String str1="insert";
							str1+="#";
							str1+=tokens1[k];
						//	Log.i("tokens_tokens_1",str1);
						//	Log.i("tokens_tokens_2",tokens[k-1]);
							cv.put("key", tokens1[k-1]);
							cv.put("value", str1);
							insert(uri,cv);
						}
					}
				}
				else if (tokens[0].equals("insert_0")) {
				//	Log.i("in_zero","woooh.......I'm......here");
					for (int k = 0; k <= 2; k++) {
						int port_1 = map.get(ports.get(k));
						Log.i("port_1", Integer.toString(port_1));
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								port_1);
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						msg = "";
						msg += "insert";
						msg += "#";
						msg += tokens[1];
						msg += "#";
						msg += tokens[2];
						out.println(msg);
						out.flush();
						String temp = in.readLine();
						if (temp != null && temp.equals("ok")) socket.close();
						else {
							String str = "";
							str += tokens[1];
							str += "#";
							str += tokens[2];
							backup.add(str);
						}
					}
				} else if (tokens[0].equals("insert")) {
				//	Log.i("in_insert","woooohhhh.......I'm.....here");
					Log.i("tokens_tokens",temp_1);
					int i = Integer.parseInt(tokens[3]);
					for (int k = 0; k <= 2; k++) {
						int port_1 = map.get(ports.get(i % 5));
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								port_1);
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						msg = "";
						msg += "insert";
						msg += "#";
						msg += tokens[1];
						msg += "#";
						msg += tokens[2];
						out.println(msg);
						out.flush();
						String temp = in.readLine();
						if (temp != null && temp.equals("ok")) socket.close();
						else {
							String str = "";
							str += tokens[1];
							str += "#";
							str += tokens[2];
							backup.add(str);
						}
						i++;
					}
				}
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}

			return null;
			}
		}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		MatrixCursor cursor = new MatrixCursor(new String[] { "key", "value"});

		if(selection.equals("@")){
		//	Log.i("here_10","woohhh I'm here");
			String[] files = getContext().fileList();
			for (String file : files) {
				String str = "";
				FileInputStream inputStream;

				try {
					inputStream = getContext().openFileInput(file);
					int k = inputStream.read();
					while (k != -1) {
						str += String.valueOf((char) k);
						k = inputStream.read();
					}
					inputStream.close();
				} catch (Exception e) {
					Log.e(TAG, "File write failed");
				}
				cursor.addRow(new Object[]{file, str});
				Log.v("query", selection);
			}
			return cursor;
		}
		else if(selection.equals("*")){
			String msg="";
			msg+="query";
			msg+="#";
			msg+=myport;
			msg+="#";
			String[] files=getContext().fileList();
			for(String file : files) {
				String str = "";
				FileInputStream inputStream;

				try {
					inputStream = getContext().openFileInput(file);
					int k = inputStream.read();
					while (k != -1) {
						str += String.valueOf((char) k);
						k = inputStream.read();
					}
					inputStream.close();
				} catch (Exception e) {
					Log.e(TAG, "File write failed");
				}

				cursor.addRow(new Object[]{file, str});
			}
			try{
			for(int i=0;i<=4;i++){
                  if(ports.get(i).equals(my_id))continue;
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						map.get(ports.get(i)));
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				msg="";
				msg+="query";
				msg+="#";
				msg+="*";
				out.println(msg);
				out.flush();
				String str;
				str=in.readLine();
				if(str==null)continue;
				out.println("ok");
				String tokens[]=str.split("#");
				for(int k=0;k<tokens.length;k++){
					if(k%2!=0){
						cursor.addRow(new Object[]{tokens[k-1], tokens[k]});
					}
				}

			}
				return cursor;
			}catch(UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			}catch(IOException e){
				Log.e(TAG, "ClientTask socket IOException");
			}
		}
		else{

			String hash =selection;

			try {
				hash=genHash(hash);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			try {
				if (hash.compareTo(ports.get(0)) < 0 || hash.compareTo(ports.get(ports.size() - 1)) > 0) {
					Log.i("ports_size", Integer.toString(ports.size()));
					int i=2;
			for(;i>=0;i--){
						int port_no_1 = map.get(ports.get(i));
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								port_no_1);
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						String msg = "";
						msg += "query_1";
						msg += "#";
						msg += selection;
						//	msg += "\n";
						out.println(msg);
						String ret = in.readLine();
						if(ret==null){
							continue;
						}
						out.println("ok");
						String key_1 = selection;
						String value_1 = ret;
						MatrixCursor.RowBuilder builder = cursor.newRow();
						builder.add("key", key_1);
						builder.add("value", value_1);
						return cursor;
					}
				}
				else{
					for(int i=0;i<ports.size();i++) {
						if (hash.compareTo(ports.get(i)) < 0) {
							int k=2;
					for(;k>=0;k--){
								int port_no_1 = map.get(ports.get((i + k) % 5));
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										port_no_1);
								BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
								PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
								String msg = "";
								msg += "query_1";
								msg += "#";
								msg += selection;
								//msg += "\n";
								out.println(msg);
								out.flush();
								String ret = in.readLine();
						      if(ret==null)continue;
								out.println("ok");
								String key_1 = selection;
								String value_1 = ret;
								MatrixCursor.RowBuilder builder = cursor.newRow();
								builder.add("key", key_1);
								builder.add("value", value_1);
								return cursor;
							}
						}
					}

				}
			}catch(UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			}catch(IOException e){
				Log.e(TAG, "ClientTask socket IOException");
			}

			}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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
