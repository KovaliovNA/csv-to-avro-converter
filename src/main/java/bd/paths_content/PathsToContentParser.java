package bd.paths_content;

import bd.Main;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.yaml.snakeyaml.Yaml;

/**
 * @author Kovalev_Nikita1@epam.com
 */
public class PathsToContentParser {

  private static final String FILE_NAME = "pathsToConvert.yml";

  /**
   * Parses file .yml to {@link PathsToContent} form file {@link PathsToContentParser#FILE_NAME}.
   *
   * @return data from file {@link PathsToContentParser#FILE_NAME}.
   */
  public PathsToContent parse() throws FileNotFoundException {
    Yaml yaml = new Yaml();

    File ymlFile = getConfigFile();

    try (InputStream is = new FileInputStream(ymlFile)) {
      return yaml.loadAs(is, PathsToContent.class);
    } catch (IOException e) {

      e.printStackTrace();
    }

    return null;
  }

  private File getConfigFile() throws FileNotFoundException {
    File jarFile = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath());
    File ymlFile = new File(jarFile.getParent(), FILE_NAME);

    if (!ymlFile.exists()) {
      System.out.print("Please create file pathsToConvert.yml in the same package as jar file");
      throw new FileNotFoundException();
    }

    return ymlFile;
  }
}
