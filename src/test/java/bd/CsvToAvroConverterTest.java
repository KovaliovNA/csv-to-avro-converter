package bd;

import static org.junit.Assert.assertTrue;

import bd.avro.CsvToAvroConverter;
import bd.avro.CsvToAvroConverterImpl;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;

public class CsvToAvroConverterTest {

  @InjectMocks
  private CsvToAvroConverter converter;

  private final static String RELATIVE_CSV_FILE_PATH = "src/test/resources/test.csv";
  private final static String RELATIVE_AVRO_FILE_PATH = "src/test/resources/test.avro";

  @Before
  public void setUp() {
    converter = new CsvToAvroConverterImpl();
  }

  /**
   * Test for base case of converting csv file to avro file.
   */
  @Test
  public void convertCsvToAvro_baseCase() throws IOException {
    File csvFile = new File(RELATIVE_CSV_FILE_PATH);

    converter.convertCsvToAvro(csvFile.getAbsolutePath());

    File avroFile = new File(RELATIVE_AVRO_FILE_PATH).getAbsoluteFile();

    assertTrue(avroFile.exists());
    assertTrue(isContentIdentical(avroFile, csvFile));
    
    avroFile.delete();
  }

  private boolean isContentIdentical(File avroFile, File csvFile) throws IOException {
    try (InputStream isCsv = new FileInputStream(csvFile);
        BufferedReader csvReader = new BufferedReader(new InputStreamReader(isCsv))) {
      String csvHeader = csvReader.readLine();

      Schema schema = converter.generateSchemaFrom(csvHeader, "test.avro");
      DatumReader<Record> datumReader = new GenericDatumReader<>(schema);
      try (DataFileReader<Record> dataFileReader = new DataFileReader<>(avroFile, datumReader)) {
        return compareAvroContentWithCsv(dataFileReader, csvReader);
      }
    }
  }

  private boolean compareAvroContentWithCsv(DataFileReader<Record> dataFileReader, BufferedReader csvReader)
      throws IOException {

    while (dataFileReader.hasNext()) {
      Record avroRecrod = dataFileReader.next();
      String[] csvRecord = csvReader.readLine().split(",");

      for (int i = 0; i < csvRecord.length; i++) {
        if (!avroRecrod.get(i).toString().equals(csvRecord[i])) {
          return false;
        }
      }
    }

    return true;
  }
}