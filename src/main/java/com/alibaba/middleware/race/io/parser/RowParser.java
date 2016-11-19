package com.alibaba.middleware.race.io.parser;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.alibaba.middleware.race.index.model.LightRow;


public class RowParser {
	
	public long size;
	
    private static final long SEGMENT_SIZE = 1024 * 1024 * 20;
    
    private static final long FILESZEXT = 1024 * 1024;
    
    MappedByteBuffer[] fileSegments;
    
    MappedByteBuffer curSegment;
    
    int curSegmentId;
    
    void seek(long pos) {
    	int p = (int) (pos % SEGMENT_SIZE);
    	curSegmentId = (int) (pos / SEGMENT_SIZE);
    	curSegment = fileSegments[curSegmentId];
    	fileSegments[curSegmentId].position(p);
    }
    long getpos() {
    	return curSegmentId * SEGMENT_SIZE + fileSegments[curSegmentId].position();
    	
    }
    
    byte readByte() {
    	return curSegment.get();
    }
    
    char getChar() {
    	byte ch = curSegment.get();
    	if ((ch & 0x80) == 0) {  //一个byte中最高为0表示ascii码
    		return (char)ch;
    	} else if ((ch & 0xE0) == 0xC0 && (ch & 0x1E) != 0) {
    		byte ch1 = curSegment.get();
    		return (char) (((ch << 6) & 0x07C0) | (ch1 & 0x003F));
    	} else if ((ch & 0xF0) == 0xE0) {
    		byte ch1=curSegment.get();
    		byte ch2=curSegment.get();
            return (char) (((ch << 12) & 0xF000) | ((ch1 << 6) & 0x0FC0) |
                    (ch2 & 0x003F));
    	} else if ((ch & 0xF8) == 0xF0) {
    		byte ch1=curSegment.get();
    		byte ch2=curSegment.get();
    		byte ch3=curSegment.get();
    	}
        return (char) (-1);
    }
    
    String ReadKey() {
    	StringBuilder sb = new StringBuilder();
    	char b;
    	while((b = getChar())!=':') {
    		sb.append(b);
    	}
    	return sb.toString();
    }
    
    String ReadValue() {
    	StringBuilder sb = new StringBuilder();
    	char b;
    	while(true) {
    		b = getChar();
    		if (b=='\t' || b=='\n') {
    			break;
    		}
    		sb.append(b);
    	}
    	return sb.toString();
    }
   
    LightRow readRow() {
    	try {
    	char b;
    	LightRow row = new LightRow();
    	boolean lineend = false;
    	while(!lineend) {
        	StringBuilder sb = new StringBuilder();
        	while((b = getChar())!=':') {
        		sb.append((char)b);
        	}
        	String key = sb.toString();
        	//System.out.println(key);
        	sb.setLength(0);
        	while(true) {
        		b = getChar();
        		if (b=='\t' || b=='\n') {
        			if (b=='\n') {
        				lineend = true;
        			}
        			break;
        		}
        		sb.append((char)b);
        	}
        	String value = sb.toString();
        	sb.setLength(0);
        	row.put(key, value);
    	}
    	int pos = fileSegments[curSegmentId].position();
    	if (pos >= SEGMENT_SIZE) {
    		pos -= SEGMENT_SIZE;
    		curSegmentId++;
    		curSegment = fileSegments[curSegmentId];
    		fileSegments[curSegmentId].position(pos);
    	}
    	return row;
    	} catch (Exception e) {
    		return null;
    	}
    }
    
    public RowParser(String filename) throws IOException {

    	RandomAccessFile in = new RandomAccessFile(filename, "r");
    	size = in.length();
    	int sz = (int) ((in.length() + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
    	fileSegments = new MappedByteBuffer[sz];
    	for (int i = 0; i < sz; i++) {
    		if (i < sz - 1) {
    			fileSegments[i] = in.getChannel().map(FileChannel.MapMode.READ_ONLY, SEGMENT_SIZE * i, Math.min(SEGMENT_SIZE * (i + 1) + FILESZEXT, size) - SEGMENT_SIZE * i);
    		} else {
    			fileSegments[i] = in.getChannel().map(FileChannel.MapMode.READ_ONLY, SEGMENT_SIZE * i, in.length() % SEGMENT_SIZE);
    		}
    	}
    	curSegmentId = 0;
    	curSegment = fileSegments[0];
    	
	}
    

}
