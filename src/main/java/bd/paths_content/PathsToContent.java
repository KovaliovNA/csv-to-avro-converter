package bd.paths_content;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Kovalev_Nikita1@epam.com
 */
@Getter
@Setter
public class PathsToContent {

    /**
     * Paths to files in hadoop home lib e.g. "/train"
     */
    private List<String> contentPath;
}
