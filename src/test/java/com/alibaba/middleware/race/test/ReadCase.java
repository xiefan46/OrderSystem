package com.alibaba.middleware.race.test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class ReadCase {
	@org.junit.Test
	public void read() throws Exception
	{
		String path = "../case/case.0";
		File f = new File(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)),4096);
		String line = br.readLine();
		for(int i=0;i<1000;i++){
			System.out.println(line);
			line = br.readLine();
		}
		br.close();
	}
}
