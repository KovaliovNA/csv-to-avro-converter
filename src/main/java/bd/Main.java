package bd;

import bd.avro.CsvToAvroConverter;
import bd.avro.CsvToAvroConverterImpl;
import bd.paths_content.PathsToContent;
import bd.paths_content.PathsToContentParser;
import java.io.IOException;

/**
 * @author Kovalev_Nikita1@epam.com
 */
public class Main {

  public static void main(String[] args) throws IOException {
    PathsToContentParser parser = new PathsToContentParser();
    PathsToContent paths = parser.parse();
    
    CsvToAvroConverter converter = new CsvToAvroConverterImpl();

    for (String path : paths.getContentPath()) {
      converter.convertCsvToAvro(path);
    }
  }
}
