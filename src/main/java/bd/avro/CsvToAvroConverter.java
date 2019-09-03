package bd.avro;

import java.io.IOException;
import org.apache.avro.Schema;

/**
 * Created for converting of csv file to avro.
 *
 * @author Kovalev_Nikita1@epam.com
 */
public interface CsvToAvroConverter {

  /**
   * Convert csv file to avro and save them in the same package as the csv file.
   *
   * @param path absolute path to the csv file
   */
  void convertCsvToAvro(String path) throws IOException;

  /**
   * Generate {@link Schema} from csv header.
   * 
   * @param csvHeader csv header of file.
   * @param recordName some name for schema record.
   * @return {@link Schema} based on csv header.
   */
  Schema generateSchemaFrom(String csvHeader, String recordName);
}
