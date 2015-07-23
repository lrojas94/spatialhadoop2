/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.delaunay;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;

/**
 * Computes the Delaunay triangulation for a set of points.
 * @author Ahmed Eldawy
 * 
 * TODO use a pointer array of integers to refer to points to save memory
 *
 */
public class DelaunayTriangulation {
  
  static final Log LOG = LogFactory.getLog(DelaunayTriangulation.class);
  
  public static <P extends Point> Triangulation delaunayInMemory(P[] points) {
    Site[] sites = new Site[points.length];
    for (int i = 0; i < points.length; i++)
      sites[i] = new Site(points[i].x, points[i].y);
    Triangulation[] triangulations = new Triangulation[sites.length / 3 + (sites.length % 3 == 0 ? 0 : 1)];
    // Sort all points by X
    Arrays.sort(sites, new Comparator<Site>() {
      @Override
      public int compare(Site s1, Site s2) {
        if (s1.x < s2.x)
          return -1;
        if (s1.x > s2.x)
          return 1;
        if (s1.y < s2.y)
          return -1;
        if (s1.y > s2.y)
          return 1;
        return 0;
      }
    });
    
    // Compute the trivial Delaunay triangles of every three consecutive points
    int i, t=0;
    for (i = 0; i < sites.length - 4; i += 3) {
      // Compute Delaunay triangulation for three points
      triangulations[t++] =  new Triangulation(sites[i], sites[i+1], sites[i+2]);
    }
    if (points.length - i == 4) {
      // Compute Delaunay triangulation for every two points
       triangulations[t++] = new Triangulation(sites[i], sites[i+1]);
       triangulations[t++] = new Triangulation(sites[i+2], sites[i+3]);
    } else if (points.length - i == 3) {
      // Compute for three points
      triangulations[t++] = new Triangulation(sites[i], sites[i+1], sites[i+2]);
    } else if (points.length - i == 2) {
      // Two points, connect with a line
      triangulations[t++] = new Triangulation(sites[i], sites[i+1]);
    } else {
      throw new RuntimeException("Cannot happen");
    }
    
    // Start the merge process
    while (triangulations.length > 1) {
      // Merge every pair of Deluanay triangulations
      Triangulation[] newTriangulations = new Triangulation[triangulations.length / 2 + (triangulations.length & 1)];
      int t2 = 0;
      int t1;
      for (t1 = 0; t1 < triangulations.length - 1; t1 += 2) {
        Triangulation dt1 = triangulations[t1];
        Triangulation dt2 = triangulations[t1+1];
        newTriangulations[t2++] = new Triangulation(dt1, dt2);
      }
      if (t1 < triangulations.length)
        newTriangulations[t2++] = triangulations[t1];
      triangulations = newTriangulations;
    }
    return triangulations[0];
  }
  
  
  /**
   * Compute the Deluanay triangulation in the local machine
   * @param inPath
   * @param outPath
   * @param params
   * @throws IOException
   * @throws InterruptedException
   */
  public static <P extends Point> void delaunayLocal(Path inPath, Path outPath,
      OperationsParams params) throws IOException, InterruptedException {
    // 1- Split the input path/file to get splits that can be processed
    // independently
    final SpatialInputFormat3<Rectangle, P> inputFormat =
        new SpatialInputFormat3<Rectangle, P>();
    Job job = Job.getInstance(params);
    SpatialInputFormat3.setInputPaths(job, inPath);
    final List<InputSplit> splits = inputFormat.getSplits(job);
    
    // 2- Read all input points in memory
    List<P> points = new Vector<P>();
    for (InputSplit split : splits) {
      FileSplit fsplit = (FileSplit) split;
      final RecordReader<Rectangle, Iterable<P>> reader =
          inputFormat.createRecordReader(fsplit, null);
      if (reader instanceof SpatialRecordReader3) {
        ((SpatialRecordReader3)reader).initialize(fsplit, params);
      } else if (reader instanceof RTreeRecordReader3) {
        ((RTreeRecordReader3)reader).initialize(fsplit, params);
      } else if (reader instanceof HDFRecordReader) {
        ((HDFRecordReader)reader).initialize(fsplit, params);
      } else {
        throw new RuntimeException("Unknown record reader");
      }
      while (reader.nextKeyValue()) {
        Iterable<P> pts = reader.getCurrentValue();
        for (P p : pts) {
          points.add((P) p.clone());
        }
      }
      reader.close();
    }
    
    if (params.getBoolean("dup", true)) {
      // Remove duplicates to ensure correctness
      final float threshold = params.getFloat("threshold", 1E-5f);
      Collections.sort(points, new Comparator<P>() {
        @Override
        public int compare(P p1, P p2) {
          double dx = p1.x - p2.x;
          if (dx < 0)
            return -1;
          if (dx > 0)
            return 1;
          double dy = p1.y - p2.y;
          if (dy < 0)
            return -1;
          if (dy > 0)
            return 1;
          return 0;
        }
      });
      
      int i = 1;
      while (i < points.size()) {
        P p1 = points.get(i-1);
        P p2 = points.get(i);
        double dx = Math.abs(p1.x - p2.x);
        double dy = Math.abs(p1.y - p2.y);
        if (dx < threshold && dy < threshold)
          points.remove(i);
        else
          i++;
      }
    }
    
    LOG.info("Read "+points.size()+" points and computing DT");
    Triangulation dt = delaunayInMemory(points.toArray(
        (P[]) Array.newInstance(points.get(0).getClass(), points.size())));
    dt.draw();
  }

  private static void printUsage() {
    // TODO Auto-generated method stub
    
  }

  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    GenericOptionsParser parser = new GenericOptionsParser(args);
    OperationsParams params = new OperationsParams(parser);
    
    Path[] paths = params.getPaths();
    if (paths.length == 0)
    {
      printUsage();
      System.exit(1);
    }
    Path inFile = paths[0];
    Path outFile = paths.length > 1 ? paths[1] : null;
    
    long t1 = System.currentTimeMillis();
    if (OperationsParams.isLocal(params, inFile)) {
      delaunayLocal(inFile, outFile, params);
    } else {
      //voronoiMapReduce(inFile, outFile, params);
    }
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: " + (t2 - t1) + " millis");
  }
}
