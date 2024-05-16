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
    public static ArrayList<String> listaColegios = new ArrayList<>();
    //Estadisticas por colegio de ratio de asistencias y ratio de expulsados, colegio que ha habido mas asitencias y menos
    //Estadisticas por mes de lo mismo, el mes que ha habdio mas y que ha habido menos, y la media total
    //Estadisticas --> media
    
    private static class ReductorColegios extends Reducer<Text, Text, Text, Text> { 
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] str = val.toString().split(",", -1);
                String colegio = str[0];
                if (!listaColegios.contains(colegio)){
                    listaColegios.add(colegio);
                }
            }
            System.out.println(listaColegios.size());       
        }
        
    }
    
    private static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            try {
                String[] str = value.toString().split(",", -1);
                String idColegio = str[0];
                //System.out.println("Clave del map: " + idColegio);
                //System.out.println("Valor del map: " + value);

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
                    double ratioAsistencia = (double) alumnosPresentes/alumnosTotal;
                    
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
    
    private static class PartitionerClass  extends Partitioner<Text, Text> {        
        
        @Override
        public int getPartition(Text key, Text value, int i) {
            //String[] str = value.toString().split(",",-1);
            //String colegio = str[0];
            int numColegios = listaColegios.size();
            
            for(int j=0; i<numColegios; j++){
               //if(colegio.equals(listaColegios.get(i))){
                    return j;
               //}
            }
            return -1;
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
                    Job job = Job.getInstance(conf, "Estadisticas_Estudiantes_A_83029");
                    
                    job.setReducerClass(ReductorColegios.class);
                    
                    //ReductorColegios reductorColegios = new ReductorColegios();
                    //reductorColegios.meterColegios(key, values, context);
                    System.out.println(listaColegios.size());
                    
                    //PartitionerClass partitioner = new PartitionerClass();
                    //partitioner.setListaColegios(listaColegios);

                    job.setJarByClass(Estadisticas_Estudiantes.class);
                    job.setMapperClass(MapClass.class);
                    job.setPartitionerClass(PartitionerClass.class);
                    job.setReducerClass(ReducerClass.class);
                    job.setNumReduceTasks(listaColegios.size());
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);

                    FileInputFormat.addInputPath(job,
                            new Path("/PCD2024/a_83029/P22"));
                    FileOutputFormat.setOutputPath(job,
                            new Path("/PCD2024/a_83029/mapreduce_EstadisticasEstudiantes_4"));

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
