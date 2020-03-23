package card_constraint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Application data structure mapping to the DB's
 */
public class LocalCardinalityConstraint {

    public long _id;

    public String relType;

    public String nodeLabel;

    public  Map subgraph;

    public  Number minKCard;

    public String maxKCard;

    public  Number k;

    public Map params;

    public  List<LocalCardinalityConstraint> constraints = new ArrayList<>();

    public LocalCardinalityConstraint() {
    }


    public LocalCardinalityConstraint(long id, String relType, String nodeLabel, Map subgraph, Number minKCard, String maxKCard, Number k, Map params) {
        this._id = id;
        this.relType = relType;
        this.nodeLabel = nodeLabel;
        this.subgraph = subgraph;
        this.minKCard = minKCard;
        this.maxKCard = maxKCard;
        this.k = k;
        this.params = params;
    }

}
