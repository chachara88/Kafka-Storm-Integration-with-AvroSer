package org.stormexample;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;
// an custom VaroSerializer with templates so as to b eused for different avro objects.
public class CustomAvroDeserializer<T> {

    public void avroDeserializer(File file) throws IOException {
        DatumReader<T> userDatumReader = new SpecificDatumReader<T>();
        DataFileReader<T> dataFileReader = new DataFileReader<T>(file, userDatumReader);
        T emp = null;
        while (dataFileReader.hasNext())
        {
            emp = dataFileReader.next();
            System.out.println(emp);
        }
    }
}
