package com.grj.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class ShellUtil {
	public static void main(String[] args) throws IOException {
		String result = ShellUtil.execute("192.168.6.250","root","123456","cd /root/test;./sshshelll.sh");
		System.out.println(result);
	}
	private static Logger logger=LoggerFactory.getLogger(ShellUtil.class);
	public static String execute(String ip, String username, String password, String cmd) throws IOException {
		
		Connection conn = new Connection(ip);  
        conn.connect();//连接  
        boolean flag=conn.authenticateWithPassword(username, password);//认证 
        if(flag==true){
        	Session session = conn.openSession();
        	session.execCommand(cmd);
        	InputStream stdout = session.getStdout();
        	String result = processStdout(stdout, "utf-8");
        	return result;
        	
        }else{
        	logger.error("connection failed");
        	return null;
        }
	}
	private static String processStdout(InputStream in, String charset){  
        InputStream    stdout = new StreamGobbler(in);  
        StringBuffer buffer = new StringBuffer();;  
        try {  
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout,charset));  
            String line=null;  
            while((line=br.readLine()) != null){  
                buffer.append(line+"\n");  
            }  
        } catch (UnsupportedEncodingException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return buffer.toString();  
    } 
}