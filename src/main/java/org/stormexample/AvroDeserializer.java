//package org.stormexample;
//
//import org.apache.avro.file.DataFileReader;
//import org.apache.avro.io.DatumReader;
//import org.apache.avro.specific.SpecificDatumReader;
//
//import java.io.File;
//import java.io.IOException;
//
//public class AvroDeserializer {
//    public static void main(String[] args) throws IOException {
//        /*args[0] should be the avro file name
//        * args[1] should be the avro class name
//        * */
//        if (args.length!=2){
//            System.out.println("The number of arguments is not correct. Program will exit");
//            System.exit(-1);
//        }
//        File file = new File(args[0]);
//        /* To be a bit generic
//        To do so, a generic function like the follwoing should be created.
//        switch(args[1]){
//
//            case "Employee":
//               //deserializeFunc
//                // code block
//                break;
//                //it should be populated withh all necessary objects!
////            case y:
////                // code block
////                break;
//            default:
//        }*/
//
//        DatumReader<Employee> userDatumReader = new SpecificDatumReader<Employee>();
//        DataFileReader<Employee> dataFileReader = new DataFileReader<Employee>(file, userDatumReader);
//        Employee emp = null;
//        while (dataFileReader.hasNext())
//        {
//            emp = dataFileReader.next();
//            System.out.println(emp);
//        }
//    }
//
////    public static <T> void deserialize(File file) throws IOException {
////        DatumReader<T> userDatumReader = new SpecificDatumReader<T>(T.class);
////        DataFileReader<T> dataFileReader = new DataFileReader<T>(file, userDatumReader);
////        T emp = null;
////        while (dataFileReader.hasNext())
////        {
////            emp = dataFileReader.next();
////            System.out.println(emp);
////        }
////    }
//}
