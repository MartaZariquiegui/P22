/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.estadisticas_estudiantes;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.text.ParseException;
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
    
    private static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) {
            try {
                String[] str = value.toString().split("\t", -1);
                String genero = str[3];
                System.out.println("Clave del map: " + genero);
                System.out.println("Valor del map: " + value);

                //Descartar la primera linea
                if (!("Género".equals(genero))) {
                    context.write(new Text(genero), new Text(value)); //No se puede poner string geenro en la key proque el tipo escribible en java es Text. Siempres ahi se pone Text
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
        private int max = -1;
        private String nombre = null;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer.Context context) {
            try {
                max = -1; //cada vez que entramos inicializamos el valor

                for (Text val : values) {
                    String[] str = val.toString().split("\t", -1);
                    int number = NumberFormat.getNumberInstance(java.util.Locale.US).parse(str[4]).intValue(); //el campo 4 es el dinero
                    if (number > max) {
                        max = number;
                        nombre = str[1];
                    }
                }
                context.write(new Text(key), new Text(nombre + "\t" + max));//new IntWritable(max)


            } catch (IOException | InterruptedException | ParseException e) {
                System.err.println("Excepcion: ");
                System.err.println("Mensaje: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }

    }
    
    private static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int i) {
            String[] str = value.toString().split("\t",-1);
            int edad = Integer.parseInt(str[2]);
            
            if(edad <= 20){
                return 0;
            }
            else if (edad > 20 && edad <=30){
                return 1;
            } 
            else{
                return 2;
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
                    job.setReducerClass(Reducer.class);
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
