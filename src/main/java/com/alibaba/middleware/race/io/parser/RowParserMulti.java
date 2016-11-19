package com.alibaba.middleware.race.io.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.alibaba.middleware.race.index.model.LightRow;


public class RowParserMulti {
	
    private RowParser[] readers;
    
	private TreeMap<Long, Integer> tmap = new TreeMap<Long, Integer>();
	
	private long[] sumsz;
	
	private int readerPos;
	
	private Collection<String> files;
	
	@Override
	public RowParserMulti clone() {
		try {
			return new RowParserMulti(files);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void seek(long pos) {
		Entry<Long, Integer> t = tmap.floorEntry(pos);
		readerPos = t.getValue();
		readers[readerPos].seek(pos - t.getKey());
	}
	
	public LightRow readRow() {
		if (readerPos >= readers.length) {
			return null;
		}
		LightRow r = readers[readerPos].readRow();
		if (readers[readerPos].getpos() == readers[readerPos].size) {
			readerPos++;
			if (readerPos < readers.length) {
				readers[readerPos].seek(0);
			}
		}
		return r;
	}
	
	public long getPos() {
		if (readerPos >= readers.length) {
			return -1;
		}
		return sumsz[readerPos] + readers[readerPos].getpos();
	}
	
	public RowParserMulti(Collection<String> files) throws IOException {
		readerPos = 0;
		readers = new RowParser[files.size()];
		sumsz = new long[files.size()];
		int pos = 0;
		long posSum = 0;
		for (String file: files) {
			readers[pos] = new RowParser(file);
			sumsz[pos] = posSum;
			tmap.put(posSum, pos);
			posSum += readers[pos].size;
			pos++;
		}
		this.files = new ArrayList<String>(files);
    }
	
	
}
