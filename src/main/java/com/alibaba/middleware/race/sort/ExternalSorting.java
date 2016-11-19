package com.alibaba.middleware.race.sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.model.ComparableKeys;
import com.alibaba.middleware.race.model.Row;


public class ExternalSorting 
{   
	
	private static InternalSortMethod<String> sortMethod= new JdkQuickSort<String>();
	
	/*
	 * 外排序接口，将指定目录下的文件按orderingKeys排好序并且归并为一个大文件，保存在outputDir目录下
	 */
	public static int externalSort(Collection<File> files,List<String> orderingKeys,
			String outputDir,String outputFileName) throws Exception
	{
		File dir = new File(outputDir);
		if(!dir.exists() || !dir.isDirectory()){
			RaceUtil.mkDir(dir);
		}
		for(File f: dir.listFiles()){
			f.delete();
		}
		//System.out.println("outputdir:"+outputDir);
		long start = System.currentTimeMillis();
		internalSortAndSaveAllFiles(files, orderingKeys, outputDir);
		long sortEnd = System.currentTimeMillis();
		System.out.println("internal sort time:"+(sortEnd-start)+" ms");
		List<File> sortedFiles = Arrays.asList(new File(outputDir).listFiles());
		//System.out.println("output file name:"+outputFileName);
		//System.out.println("..:"+outputDir+outputFileName);
		int size = mergeSortedFiles(sortedFiles, orderingKeys, new File(outputDir+outputFileName));
		System.out.println("merge time:"+(System.currentTimeMillis()-sortEnd)+" ms");
		return size;
	}
	
	public static int externalSort(String inputFilesDir,
			List<String> orderingKeys,String outputDir,String outputFileName) throws Exception
	{
		//System.out.println("outputdir:"+outputDir);
		long start = System.currentTimeMillis();
		internalSortAndSaveAllFiles(inputFilesDir, orderingKeys, outputDir);
		long sortEnd = System.currentTimeMillis();
		System.out.println("internal sort time:"+(sortEnd-start)+" ms");
		List<File> sortedFiles = Arrays.asList(new File(outputDir).listFiles());
		File dir = new File(outputDir);
		if(!dir.exists() || !dir.isDirectory()){
			RaceUtil.mkDir(dir);
		}
		//System.out.println("output file name:"+outputFileName);
		//System.out.println("..:"+outputDir+outputFileName);
		int size = mergeSortedFiles(sortedFiles, orderingKeys, new File(outputDir+outputFileName));
		System.out.println("merge time:"+(System.currentTimeMillis()-sortEnd)+" ms");
		return size;
	}
	
	
	
	/*
	 * 将list中所有文件排序，并且将排序好的文件保存到指定目录
	 * 排序好的文件命名规则  {originalFileName}.tmp
	 */
	
	private static void internalSortAndSaveAllFiles(String inputFilesDir,
			List<String> orderingKeys,String outputDir) throws Exception
	{
		File dir = new File(inputFilesDir);
		if(!dir.isDirectory()){
			throw new Exception("inputFilesDir不存在或不是目录");
		}
		internalSortAndSaveAllFiles(Arrays.asList(dir.listFiles()), orderingKeys, outputDir);
	}
	
	private static void internalSortAndSaveAllFiles(Collection<File> files,
			List<String> orderingKeys,String outputDir) throws Exception
	{
		//System.out.println("output dir"+outputDir);
		File dir = new File(outputDir);
		if(!dir.exists() || !dir.isDirectory()){
			RaceUtil.mkDir(dir);
		}
		for(File f : files)
		{
			String tmpFileName = f.getName() + ".tmp";
			internalSortAndSaveFile(f, orderingKeys, outputDir+tmpFileName);
		}
	}
	
	/*
	 * 单线程内排序
	 * 特别注意，这个函数没有检查内存溢出的逻辑，需要确保被排序文件能装得下内存
	 * TODO：内排序方法暂时先使用jdk Collections类自带的方法，后面可换多线程快排
	 */
	private static File internalSortAndSaveFile(File inputFile,List<String> orderingKeys,
			String outputFilePath) throws Exception
	{
		//System.out.println(outputFilePath);
		File output = new File(outputFilePath);
		if(!output.exists()){
			output.createNewFile();
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(inputFile)));
		List<String> rowStrs = new ArrayList<String>();
		String line = br.readLine();
		while(line != null)
		{
			rowStrs.add(line);
			//System.out.println(line);
			line = br.readLine();
		}
		/*int size = rowStrs.size();
		ComparableKeys[] cks = new ComparableKeys[size];
		for(int i=0;i<size;i++)
		{
			cks[i] = new ComparableKeys(orderingKeys, FileUtil.createKVMapFromLine(rowStrs.get(i)));
		}*/
		//sortMethod.sort(strs, new RowComparator(orderingKeys));
		//System.out.println("before sort");
		Collections.sort(rowStrs, new RowComparator(orderingKeys));
		//System.out.println("after sort");
		//Sorter.quicksort(cks);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(output)));
		for(String rowStr : rowStrs)
		{
			//System.out.println(rowStr);
			bw.write(rowStr);
			bw.newLine();
		}
		br.close();
		bw.close();
		return output;
	}
	
	/*
	 * 第一个参数是输入文件列表
	 * 第二个是需要排序的key列表
	 * 第三个是输出文件
	 */
	private static int mergeSortedFiles(List<File> files,List<String> orderingKeys
			,File outputfile) throws Exception
	{
		ArrayList<BinaryFileBuffer> bfbs = new ArrayList<BinaryFileBuffer>();
		Comparator<String> rowCmp = new RowComparator(orderingKeys);
        for (File f : files) {
                final int BUFFERSIZE = 2048;
                InputStream in = new FileInputStream(f);
                BufferedReader br;
                br = new BufferedReader(new InputStreamReader(in));
                BinaryFileBuffer bfb = new BinaryFileBuffer(br);
                bfbs.add(bfb);
        }
        if(!outputfile.exists()){
        	outputfile.createNewFile();
        }
        BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(outputfile, false)));
        int rowcounter = mergeSortedFiles(fbw, rowCmp, bfbs);
        for (File f : files)
                f.delete();
        return rowcounter;
	}
	
	
	private static int mergeSortedFiles(BufferedWriter fbw,final Comparator<String> cmp,
            List<BinaryFileBuffer> buffers) throws IOException 
	{       
		
            PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(
                    11, new Comparator<BinaryFileBuffer>() {
                            @Override
                            public int compare(BinaryFileBuffer i,
                                    BinaryFileBuffer j) {
                                    return cmp.compare(i.peek(), j.peek());
                            }
                    });
            
            for (BinaryFileBuffer bfb : buffers)
                    if (!bfb.empty())
                            pq.add(bfb);
            int rowcounter = 0;
            try {                        
                while (pq.size() > 0) 
                {
                	BinaryFileBuffer bfb = pq.poll();
                	String r = bfb.pop();
                    fbw.write(r);
                    fbw.newLine();
                    ++rowcounter;
                    if (bfb.empty()) {
                      bfb.fbr.close();
                     } else {
                      pq.add(bfb); // add it back
                     }
                 }
          
            } finally {
                    fbw.close();
                    for (BinaryFileBuffer bfb : pq)
                            bfb.close();
            }
            return rowcounter;
    }
    
	public static void main(String[] args) throws Exception
	{
		String orderPath = RaceConfig.OrderInfoPath;
		File dir = new File(orderPath);
		RaceUtil.mkDir(new File(RaceConfig.OrderIndexPath));
		File outputFile = new File(RaceConfig.OrderIndexPath+"index");
		List<String> orderingKeys = new ArrayList<String>();
		orderingKeys.add("orderid");
		mergeSortedFiles(Arrays.asList(dir.listFiles()), orderingKeys, outputFile);
		System.out.println("finish");
	}
}



final class BinaryFileBuffer {
    public BinaryFileBuffer(BufferedReader r) throws IOException {
            this.fbr = r;
            reload();
    }
    public void close() throws IOException {
            this.fbr.close();
    }

    public boolean empty() {
            return this.cache == null;
    }

    public String peek() {
            return this.cache;
    }

    public String pop() throws IOException {
            String answer = peek().toString();// make a copy
            reload();
            return answer;
    }

    private void reload() throws IOException {
            this.cache = this.fbr.readLine();
    }

    public BufferedReader fbr;

    private String cache;
    
    
}

