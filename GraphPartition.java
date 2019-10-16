import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth

    /* ... */
    public Vertex() {
        this.id = 0;
        this.adjacent = new Vector<Long>();
        this.centroid = 0;
        this.depth = (short) 0;
    }

    public Vertex(long id, Vector<Long> adjacent, long centroid, short depth) {
        this.id =id;
        this.adjacent=adjacent;
        this.centroid=centroid;
        this.depth=depth;
    }

    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        int size = in.readInt();
        this.adjacent.clear();
        for(int i=0; i<size; i++) {
            this.adjacent.add(in.readLong());
        }
        this.centroid=in.readLong();
        this.depth=in.readShort();

    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        // write the vector out too?
        out.writeInt(this.adjacent.size());
        for(int i = 0; i < adjacent.size(); i++) {
            out.writeLong(adjacent.get(i));
        }
        out.writeLong(this.centroid);
        out.writeShort(this.depth);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.id);
        sb.append(",");
        sb.append(this.adjacent.size());
        sb.append(",");
        for (int i = 0; i < this.adjacent.size(); i++) {
            sb.append(this.adjacent.get(i));
            sb.append(",");
        }
        sb.append(this.centroid);
        sb.append(",");
        sb.append(this.depth);
        return sb.toString();
    }
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;

    /*
      map ( key, line ) =
      parse the line to get the vertex id and the adjacent vector
      // take the first 10 vertices of each split to be the centroids
      for the first 10 vertices, centroid = id; for all the others, centroid = -1
      emit( id, new Vertex(id,adjacent,centroid,0) )
    */
    public static class ReadMapper extends Mapper<Object,Text,LongWritable,Vertex>{
        static int count = 1;

        @Override
        public void map(Object key,Text line,Context context) throws IOException, InterruptedException{
            Scanner s = new Scanner(line.toString()).useDelimiter(",");
            long vertexid = s.nextLong();

            Vector<Long> adjacent = new Vector<Long>();
            while(s.hasNext()){
                adjacent.add(s.nextLong());
            }

            long centroid = vertexid;
            if(count<=10) {
                centroid= vertexid;
                System.out.println("Choosing vertex as centroid: " + vertexid);
            } else {
                centroid=-1;
                System.out.println("No centroid for vertex: " + vertexid);
            }
	        count++;
            context.write(new LongWritable(vertexid), new Vertex(vertexid,adjacent,centroid,(short)0));
        }
    }

    /*
      map ( key, vertex ) =
      emit( vertex.id, vertex )   // pass the graph topology
      if (vertex.centroid > 0)
      for n in vertex.adjacent:     // send the centroid to the adjacent vertices
      emit( n, new Vertex(n,[],vertex.centroid,BFS_depth) )
    */
    public static class MapperForBFS extends Mapper<Object,Vertex,LongWritable,Vertex>{
        @Override
        public void map(Object key,Vertex vertex,Context context) throws IOException, InterruptedException{
            context.write(new LongWritable(vertex.id), vertex);
            if(vertex.centroid>0){
                for(long n:vertex.adjacent){
                    System.out.println("Adding " + BFS_depth + " for vertex " + n + " which is a neighbor of " + vertex.id);
                    context.write(new LongWritable(n), new Vertex(n,new Vector<Long>(),vertex.centroid,BFS_depth));
                }
            }
        }
    }
    /*
      reduce ( id, values ) =
      min_depth = 1000
      m = new Vertex(id,[],-1,0)
      for v in values:
      if (v.adjacent is not empty)
      m.adjacent = v.adjacent
      if (v.centroid > 0 && v.depth < min_depth)
      min_depth = v.depth
      m.centroid = v.centroid
      m.depth = min_depth
      emit( id, m )
    */

    public static class ReducerForBFS extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{
        @Override
        public void reduce(LongWritable id,Iterable<Vertex> values,Context context) throws IOException, InterruptedException{
            short min_depth = 1000;
            Vertex m = new Vertex(id.get(),new Vector<Long>(),-1,(short)0);
            for(Vertex v:values){
                if(!v.adjacent.isEmpty()){
                    m.adjacent = v.adjacent;
                }
                if(v.centroid > 0 && v.depth < min_depth){
                    min_depth = v.depth;
                    m.centroid = v.centroid;
                }
            }
            m.depth = min_depth;
            context.write(id,m);
        }
    }
    /*map ( id, value ) =
      emit(value.centroid,1)

      reduce ( centroid, values ) =
      m = 0
      for v in values:
      m = m+v
      emit(centroid,m)
    */
    public static class SizeMapper extends Mapper<Object,Vertex,LongWritable,IntWritable>{
        @Override
        public void map(Object id,Vertex value,Context context) throws IOException,InterruptedException{
            context.write(new LongWritable(value.centroid),new IntWritable(1));
        }
    }

    public static class SizeReducer extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable>{
        @Override
        public void reduce(LongWritable centroid,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int m = 0;
            for(IntWritable v:values){
                m = m+v.get();
            }
            context.write(centroid,new IntWritable(m));
        }

    }


    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(GraphPartition.class);
        job.setJobName("MyJob");
        /* ... First Map-Reduce job to read the graph */
        job.setMapperClass(GraphPartition.ReadMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/i0"));
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        if (job.isSuccessful()) {
            System.out.println("Job was successful! :)");
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful. :(");
            System.exit(returnValue);
        }

        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            job.setJarByClass(GraphPartition.class);
            job.setMapperClass(GraphPartition.MapperForBFS.class);
            job.setReducerClass(GraphPartition.ReducerForBFS.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            FileInputFormat.addInputPath(job, new Path(args[1] + "/i" + i));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/i" + (i+1)));
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            /* ... Second Map-Reduce job to do BFS */
            returnValue = job.waitForCompletion(true) ? 0 : 1;
            if (job.isSuccessful()) {
                System.out.println("Job was successful! :)");
            } else if (!job.isSuccessful()) {
                System.out.println("Job was not successful. :(");
                System.exit(returnValue);
            }
        }
        job = Job.getInstance();
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(GraphPartition.SizeMapper.class);
        job.setReducerClass(GraphPartition.SizeReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1] + "/i8"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* ... Final Map-Reduce job to calculate the cluster sizes */
        returnValue = job.waitForCompletion(true) ? 0 : 1;
        if (job.isSuccessful()) {
            System.out.println("Job was successful! :)");
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful. :(");
        }

        System.exit(returnValue);
    }
}
