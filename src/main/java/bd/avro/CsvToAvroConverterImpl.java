package bd.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CsvToAvroConverterImpl implements CsvToAvroConverter {

  private static final String CSV_DELIMITER = ",";
  private static final String SLASH = "/";
  private static final String REVERSE_SLASH = "\\";
  private static final String DOT = ".";
  private static final String AVRO_EXTENSION = ".avro";

  @Override
  public void convertCsvToAvro(String path) throws IOException {
    Configuration conf = createConfigurationWith();
    FileSystem fs = FileSystem.get(conf);

    try (FSDataInputStream is = fs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

      String csvHeader = reader.readLine();

      String recordName = getRecordName(path);
      Schema schema = generateSchemaFrom(csvHeader, recordName);

      convertRemainingLinesToAvro(reader, schema, path);
    }
  }

  @Override
  public Schema generateSchemaFrom(String csvHeader, String recordName) {
    FieldAssembler<Schema> builder = SchemaBuilder.record(recordName)
        .fields();

    for (String column : csvHeader.split(CSV_DELIMITER)) {
      builder
          .name(column)
          .type()
          .stringType()
          .stringDefault("");
    }

    return builder.endRecord();
  }

  private static Configuration createConfigurationWith() {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/core-site.xml"));
    conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));

    return conf;
  }

  private String getAvroFilePathBasedOn(String path) {
    String fileWithoutExtension = path.substring(0, path.lastIndexOf(DOT));

    return fileWithoutExtension + AVRO_EXTENSION;
  }

  private void convertRemainingLinesToAvro(BufferedReader reader, Schema schema, String path)
      throws IOException {

    List<Field> fields = schema.getFields();
    DatumWriter<Record> datumWriter = new GenericDatumWriter<>(schema);
    File avroFile = new File(getAvroFilePathBasedOn(path));

    try (DataFileWriter<Record> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schema, avroFile);

      for (String line : reader.lines().collect(Collectors.toList())) {
        String[] values = line.split(CSV_DELIMITER);
        Record avroRecord = new Record(schema);

        for (int i = 0; i < values.length; i++) {
          avroRecord.put(fields.get(i).name(), values[i]);
        }

        dataFileWriter.append(avroRecord);
      }
    }
  }

  private String getRecordName(String path) {
    int indexOfStartFileName = getIndexOfStartFileName(path);

    return path.substring(indexOfStartFileName + 1);
  }

  private int getIndexOfStartFileName(String path) {
    int indexOfStartFileName = path.lastIndexOf(SLASH);

    if (indexOfStartFileName == -1) {
      indexOfStartFileName = path.lastIndexOf(REVERSE_SLASH);
    }

    return indexOfStartFileName;
  }
}
