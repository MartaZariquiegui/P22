/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.estadisticas_estudiantes;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author alumno
 */
public class Estadisticas_Estudiantes {
    
    //Estadisticas por colegio de ratio de asistencias y ratio de expulsados, colegio que ha habido mas asitencias y menos
    //Estadisticas por mes de lo mismo, el mes que ha habdio mas y que ha habido menos, y la media total
    //Estadisticas --> media
    
    private static class ReductorColegios extends Reducer<Text, Text, Text, Text> { 
        private ArrayList<String> listaColegios = new ArrayList<>();
        
        @Override
        protected ArrayList<String> reduce(Text key, Iterable<Text> values, Context context) {

            for (Text val : values) {
                String[] str = val.toString().split(",", -1);
                String colegio = str[0];
                if (!listaColegios.contains(colegio)){
                    listaColegios.add(colegio);
                }
            }
            return listaColegios;
        }
    }
    
    private static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) {
            try {
                String[] str = value.toString().split(",", -1);
                String idColegio = str[0];
                System.out.println("Clave del map: " + idColegio);
                System.out.println("Valor del map: " + value);

                //Descartar la primera linea
                if (!("School DBN".equals(idColegio))) {
                    context.write(new Text(idColegio), new Text(value)); //No se puede poner string geenro en la key proque el tipo escribible en java es Text. Siempres ahi se pone Text
                }

            } catch (IOException | InterruptedException e) {
                System.err.println("Excepcion: ");
                System.err.println("Mensaje: " + e.getMessage());
                e.printStackTrace(System.err);
            } //Si ponemos el throws no pondemos ver el fallo pq saldra en todos los nodos y a nosotros solo nos saldra false
        }
        
    }
    
    private static class ReducerClass extends Reducer<Text, Text, Text, Text> { //le llegan el geenro y la linea y va a sacar el genero y el salario 
    //private static class ReducerClass extends Reducer<Text, Text, Text, IntWritable>    
        private int dias = 0;
        private long sumatorioRatio = 0L; //para la media
        private String idColegio = null;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try {

                for (Text val : values) {
                    String[] str = val.toString().split(",", -1);
                    int alumnosTotal = Integer.parseInt(str[2]); 
                    int alumnosPresentes = Integer.parseInt(str[3]);
                    int alumnosAusentes = Integer.parseInt(str[4]);                    
                    double ratioAsistencia = alumnosPresentes/alumnosTotal;
                    
                    //Actualizamos estadisticas
                    dias++;
                    sumatorioRatio+=ratioAsistencia;
                    
                }
                long mediaColegio = sumatorioRatio / dias;
                context.write(new Text(key), new Text(idColegio + "\t" + mediaColegio));


            } catch (IOException | InterruptedException e) {
                System.err.println("Excepcion: ");
                System.err.println("Mensaje: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }
    
    private static class PartitionerClass(ArrayList<String> listaColegios) extends Partitioner<Text, Text> {
        
        @Override
        public int getPartition(Text key, Text value, int i) {
            String[] str = value.toString().split(",",-1);
            int colegio = Integer.parseInt(str[2]);
            int numColegios = listaColegios.size();
            
            for(int i=0; i<numColegios; i++){
               if(colegio.equals(listaColegios.get(i))){
                   return i;
               }
            }
        }
        
    }


    
    public static void main(String[] args) {
        
        UserGroupInformation ugi
                = UserGroupInformation.createRemoteUser("a_83029");
        try{
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://192.168.10.1:9000");
                    Job job = Job.getInstance(conf, "CustomMinMaxTuple_A_83029");
                    job.setJarByClass(Estadisticas_Estudiantes.class);
                    job.setMapperClass(MapClass.class);
                    job.setPartitionerClass(PartitionerClass.class);
                    job.setReducerClass(Reducer.class);
                    job.setNumReduceTasks(3);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job,
                            new Path("/PCD2024/a_83029/particionadoHadoop"));
                    FileOutputFormat.setOutputPath(job,
                            new Path("/PCD2024/a_83029/mapreduce_particionadoHaoop"));

                    boolean finalizado = job.waitForCompletion(true);
                    System.out.println("Finalizado: " + finalizado);
                    return null;
                }
            });
        }catch(Exception e){
            System.err.println("Excepcion: ");
                System.err.println("Mensaje: " + e.getMessage());
                e.printStackTrace(System.err);
        }
    }
}
