package card_constraint;

import com.google.gson.Gson;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


public class Main {

    @Context
    public GraphDatabaseService db;

    //////////////////////////////////////////////////////////////////////////////////
    @Procedure(value = "card_constraint.create_relationship", mode = Mode.WRITE)
    @Description("Create new relationship with checking cardinality constraints")
    public Stream<Output> createRelationship(@Name("query") String query) {
        String message = "";

        PatternMatcher matcher = new PatternMatcher();

        String logText = ""; // variable for printing CSV-like execution times of each procedure milestone
        /**
         * Parse input string and get nodes and relationships
         */
        long startTimeParse = System.nanoTime();
        matcher.parseNodesRelationships(query);
        long endTimeParse = System.nanoTime();

        logText += "ParseNodesRelationships," + startTimeParse + "," + endTimeParse + "\n";

        String inputPattern = ""; // variable for input pattern with only node and relationship types (e.g. (n1:CreditCard)-[r1:ISSUED_FOR]->(n2:Account))
        long startTimeMatches = System.nanoTime();
        List<String> matches = matcher.getMatches(); // retrieve all node and relationship components of the input query
        long endTimeMatches = System.nanoTime();

        logText += "GetMatches," + startTimeMatches + "," + endTimeMatches + "\n";

        String dbQueryCard = "MATCH "; // start building the DB query for retrieving the current number of relationships R

        long startTimeInputPattern = System.nanoTime();
        for(int i = 0; i<matches.size(); i++){

            if(matches.size() > i+1){
                dbQueryCard += "(" + matches.get(i) + "), ";
                inputPattern += "(" + matches.get(i) + ")-[" + matches.get(++i) + "]->";

            } else{
                dbQueryCard += "(" + matches.get(i) + ")";
                inputPattern += "(" + matches.get(i) + ")";
            }
        }
        matcher.setInputPattern(inputPattern);
        long endTimeInputPattern = System.nanoTime();

        logText += "BuildInputPattern," + startTimeInputPattern + "," + endTimeInputPattern + "\n";

        System.out.println("Input pattern: " + inputPattern);

        boolean isRuleViolated = false; // flag variable to indicate if a given constraint rule is violated
        String firstNodeConditionPropertyName = "", firstNodeConditionPropertyValue = ""; // variables used to identify the first node, for which the number of relationships R is checked

        /**
         * Remove conditions to compare input and constraint query patterns
         */
        long startTimeInputMap = System.nanoTime();
        Pattern conditionsPattern = Pattern.compile("\\{(.*?)\\}");
        Matcher conditionsMatcher = conditionsPattern.matcher(query);

        while(conditionsMatcher.find()){
            if (conditionsMatcher.group(1).contains(",")) {
                String[] singleConditions = conditionsMatcher.group(1).split(",");
                firstNodeConditionPropertyName = singleConditions[0].split(":")[0].trim();
                firstNodeConditionPropertyValue = singleConditions[0].split(":")[1].trim();

                for (int i = 0; i < singleConditions.length; i++)
                    matcher.inputPatternMap.put(singleConditions[i].split(":")[0].trim(), singleConditions[i].split(":")[1].replaceAll("'", "").trim()); // key-value pairs for individual conditions are added to map for input query
            }
            else { // only one condition is present in input query
                firstNodeConditionPropertyName = conditionsMatcher.group(1).split(":")[0].trim();
                firstNodeConditionPropertyValue = conditionsMatcher.group(1).split(":")[1].replaceAll("'", "").trim();
                matcher.inputPatternMap.put(conditionsMatcher.group(1).split(":")[0].trim(), conditionsMatcher.group(1).split(":")[1].replaceAll("'", "").trim());
            }
        }

        matcher.removeConditionsFromPattern();
        matcher.buildMapFromInputPattern(); // add node and relationship types to map
        long endTimeInputMap = System.nanoTime();

        logText += "BuildInputMap," + startTimeInputMap + "," + endTimeInputMap + "\n";

        long startTimeNodeTypes = System.nanoTime();
        List<String> nodeTypes = matcher.getNodeTypes(); // retrieve node types from the input query to check min cardinality
        long endTimeNodeTypes = System.nanoTime();

        logText += "GetNodeTypes," + startTimeNodeTypes + "," + endTimeNodeTypes + "\n";

        long startTimeConstraints = System.nanoTime();
        List<LocalCardinalityConstraint> constraints = retrieveConstraints(null); // retrieve CardinalityConstraint nodes from DB
        long endTimeConstraints = System.nanoTime();

        logText += "RetrieveConstraints," + startTimeConstraints + "," + endTimeConstraints + "\n";
        boolean relExistsDB, relExistsPattern = true; // flag variables to denote if a given relationship R between elements E and S already exists in the DB/is listed in current input query

        int numRels = 0;
        /**
         * Analyze each cardinality constraint and check if new relationship will violate it
         */
        for (LocalCardinalityConstraint constraint:
             constraints) {

            long startTimeConstraintMap = System.nanoTime();
            TreeMap constraintMap = new TreeMap(); // build map of constraint elements
            constraintMap.put("E", constraint.nodeLabel);
            constraintMap.put("R", constraint.relType);
            constraintMap.put("S", constraint.subgraph);

            Structure structure = new Structure();
            Map recurConstraintMap = buildMapStructure(1, constraintMap, structure.structureMap, constraint.params); // iterate through nested subgraphs and params to add elements to constraint map
            structure.structureMap.putAll(recurConstraintMap); // merge structure maps after recursion
            long endTimeConstraintMap = System.nanoTime();

            logText += "BuildConstraintMap," + startTimeConstraintMap + "," + endTimeConstraintMap + "\n";

            long startTimeConstraintPattern = System.nanoTime();
            String constraintPattern = "(n1:" + constraint.nodeLabel + ")-[r1:" + constraint.relType + "]->";

            constraintPattern = buildConstraintPattern(2, constraintPattern, constraint.subgraph, constraint.params); // build constraint pattern to compare with input pattern elements
            long endTimeConstraintPattern = System.nanoTime();

            logText += "BuildConstraintPattern," + startTimeConstraintPattern + "," + endTimeConstraintPattern + "\n";

            if(constraint.minKCard.intValue() == 1){

                for (String inputNode:
                     nodeTypes) {
                    String nodeType = inputNode.split(":")[1].substring(0, inputNode.split(":")[1].indexOf("{"));

                    if(nodeType.trim().toLowerCase().equals(constraint.nodeLabel.toLowerCase())){ // for each node type in the input query, which corresponds to E, first check if the relationship R already exists in DB

                        relExistsDB = checkRelationshipExistence(inputNode, constraintPattern);
                        System.out.println("EXISTS in DB: " + relExistsDB);

                        if(relExistsDB == false){ // if R does not exist in DB, check if it will be created with the input query
                            relExistsPattern = checkRelationshipInputPattern(matcher, constraint);
                            System.out.println("EXISTS in input pattern: " + relExistsPattern);

                            if(relExistsPattern == false){ // if not, then the query with pattern starting at node type violates this cardinality constraint
                                isRuleViolated = true;
                                message += Output.MESSAGE_TYPE.MIN_VIOLATION.text; // prepare output stream message
                                message += " ( Node label: " + constraint.nodeLabel;
                                message += ", Relationship type:  " + constraint.relType;
                                message += ", Subgraph: " + constraint.subgraph;
                                message += ", Min: " + constraint.minKCard;
                                message += ", Max: " + constraint.maxKCard;

                                if (constraint.params != null)
                                    message += ", Params: " + constraint.params;
                                message += ")";
                            }
                        }
                    }
                }
            }

                // check for max

            if (!constraint.maxKCard.equals("*")) {

                String constraintPatternNoCond = constraintPattern.replaceAll("\\{.*?\\}", ""); // remove conditions from constraint pattern for comparison
                constraintPatternNoCond = constraintPatternNoCond.replaceAll("\\,", "");
                constraintPatternNoCond = constraintPatternNoCond.replaceAll("\\ ", "");
                System.out.println("Constraint structure: " + constraintPatternNoCond);

                if (matcher.getPatternWithoutConditions().equals(constraintPatternNoCond)) { // if input query pattern matches the constraint pattern, e.g. (n1:CreditCard)-[r1:ISSUED_FOR]->(n2:Account)
                    if (matcher.inputPatternMap.entrySet().containsAll(structure.structureMap.entrySet())) { // if all constraint definition elements are present in the input pattern map
                        long startTimeNumberofRels = System.nanoTime();
                        numRels = getCurrentNumberOfRels(inputPattern, firstNodeConditionPropertyName, firstNodeConditionPropertyValue, constraintPattern); // retrieve current number of relationships R in DB
                        long endTimeNumberOfRels = System.nanoTime();

                        logText += "GetNumberOfRels," + startTimeNumberofRels + "," + endTimeNumberOfRels + "\n";

                        if (numRels >= Integer.parseInt(constraint.maxKCard)) {
                            isRuleViolated = true;  // constraint rule would be violated by new relationship
                            message += Output.MESSAGE_TYPE.MAX_VIOLATION.text;
                            message += " ( Node label: " + constraint.nodeLabel;
                            message += ", Relationship type:  " + constraint.relType;
                            message += ", Subgraph: " + constraint.subgraph;
                            message += ", Min: " + constraint.minKCard;
                            message += ", Max: " + constraint.maxKCard;

                            if (constraint.params != null)
                                message += ", Params: " + constraint.params;
                            message += ")";
                        }
                    }
                }
            }

        }

        if (isRuleViolated == false) {
            long startTimeFinalParse = System.nanoTime();

            String secondNode = matcher.getNodeTypes().stream().skip(1).findFirst().orElse(null).split(":")[0]; // get the variable name of the second node in the input pattern

            int secondNodeIndex = matcher.getPatternNoNodeConditions().indexOf(secondNode) + secondNode.length() + 1;

            String createSubPattern = matcher.getPatternNoNodeConditions().substring(0, secondNodeIndex); // build a subpattern to separately execute CREATE and MERGE statements in case of higher order constraints (between 3 or more nodes)

            long endTimeFinalParse = System.nanoTime();

            logText += "FinalParse," + startTimeFinalParse + "," + endTimeFinalParse + "\n";

            dbQueryCard += " CREATE " + createSubPattern; // e.g. CREATE (n1)-[r1:ISSUED_FOR]->(n2)
            if(matcher.getPatternNoNodeConditions().substring(secondNodeIndex) != null)
                dbQueryCard += " MERGE (" + secondNode + ")" + matcher.getPatternNoNodeConditions().substring(secondNodeIndex);  // append MERGE statement of the rest of the input pattern (in case of higher order constraints)
            System.out.println("[CREATE] DB query: " + dbQueryCard);

            long startTimeCreate = System.nanoTime();
            db.execute(dbQueryCard); // execute DB statement
            long endTimeCreate = System.nanoTime();
            logText += "CreateRel," + startTimeCreate + "," + endTimeCreate + "\n";

            message += Output.MESSAGE_TYPE.SUCCESS.text;
        }
        System.out.println(logText);
        return Stream.of(new Output(message));
    }

    private String buildConstraintPattern(int recursionLevel, String constraintPattern, Map<String, Object> subgraphMap,
                                          Map<String, Object> params){
        TreeMap sortedMap = new TreeMap();
        sortedMap.putAll(subgraphMap);

        Iterator it = null, localIt = null;

        for(Object entry : sortedMap.entrySet()){
            Map.Entry property = (Map.Entry)entry;

            if(params != null) {
                it = params.entrySet().iterator();
            }

            switch(property.getKey().toString()){
                case "E":
                    constraintPattern += "(n" + recursionLevel + ":" + property.getValue();

                    if(params != null){
                        while(it.hasNext()){
                            Map.Entry paramEntry = (Map.Entry) it.next();

                            int paramEntryLevel = Integer.valueOf(paramEntry.getKey().toString().substring(1, 2));

                            if (paramEntryLevel == recursionLevel) {
                                Map localParams = (Map)paramEntry.getValue();

                                localIt = localParams.entrySet().iterator();
                                while(localIt.hasNext()){
                                    Map.Entry localEntry = (Map.Entry)localIt.next();
                                    if(localEntry.getKey().toString().equals("E")){
                                        List<Map> paramsList = (List<Map>)localEntry.getValue();
                                        for (Map paramsMap:
                                                paramsList) {
                                            if(paramsList.indexOf(paramsMap) == paramsList.size() - 1)
                                                constraintPattern += " {" + paramsMap.get("prop") + ":'" + paramsMap.get("value") + "'}";
                                            else
                                                constraintPattern += " {" + paramsMap.get("prop") + ":'" + paramsMap.get("value") + "'}, ";
                                        }
                                    }
                                }
                            }
                        }
                    }

                    constraintPattern +=  ")";
                    break;
                case "R":
                    constraintPattern += "-[r" + recursionLevel + ":" + property.getValue();

                    if(params != null){
                        while(it.hasNext()){

                            Map.Entry paramEntry = (Map.Entry) it.next();
                            int paramEntryLevel = Integer.valueOf(paramEntry.getKey().toString().substring(1, 2));

                            if (paramEntryLevel == recursionLevel) {
                                Map localParams = (Map)paramEntry.getValue();

                                localIt = localParams.entrySet().iterator();
                                while(localIt.hasNext()){
                                    Map.Entry localEntry = (Map.Entry)localIt.next();

                                    if(localEntry.getKey().toString().equals("R")){
                                        List<Map> paramsList = (List<Map>)  localEntry.getValue();
                                        for (Map paramsMap:
                                                paramsList) {
                                            if(paramsList.indexOf(paramsMap) == paramsList.size() - 1)
                                                constraintPattern += " {" + paramsMap.get("prop") + ":'" + paramsMap.get("value") + "'}";
                                            else
                                                constraintPattern += " {" + paramsMap.get("prop") + ":'" + paramsMap.get("value") + "'}, ";
                                        }
                                    }
                                }
                            }
                        }
                    }

                    constraintPattern += "]->";

                    break;
                case "S":
                    Map sMap = (Map) property.getValue();

                    constraintPattern = buildConstraintPattern((recursionLevel+1), constraintPattern, sMap, params);
                    break;
                default:
                    return constraintPattern;
            }
        }
        System.out.println("[STATUS] BuildConstraintPattern - " + recursionLevel);

        return constraintPattern;
    }

    private Map buildMapStructure(int recursionLevel, Map subgraphMap, Map structureMap, Map params){
        TreeMap sortedMap = new TreeMap();
        sortedMap.putAll(subgraphMap); // copy the nested subgraph structure to sorted map

        Iterator it = null, localIt = null;

        for(Object entry : sortedMap.entrySet()){
            Map.Entry property = (Map.Entry)entry;

            if(params != null) { // if the constraint has no parameters defined
                it = params.entrySet().iterator();
            }

            switch(property.getKey().toString()){ // add nested E/R elements of the subgraph S to the map
                case "E":
                    structureMap.put("n" + recursionLevel, property.getValue().toString()); // e.g. n2 : Account

                    if(params != null){ // if the constraint has parameters, map them to appropriate E/R level
                        while(it.hasNext()){
                            Map.Entry paramEntry = (Map.Entry) it.next();
                            int paramEntryLevel = Integer.valueOf(paramEntry.getKey().toString().substring(1, 2)); // e.g. param level 1 means that this parameter is defined for node type E on the level 1 of the constraint (e.g. CreditCard)

                            if (paramEntryLevel == recursionLevel) {
                                Map localParams = (Map)paramEntry.getValue(); // retrieve which component is the parameter for (E or R)

                                localIt = localParams.entrySet().iterator();
                                while(localIt.hasNext()){
                                    Map.Entry localEntry = (Map.Entry)localIt.next();
                                    if(localEntry.getKey().toString().equals("E")){ // parameter is defined exactly for node type E on a given level of constraint
                                        List<Map> paramsList = (List<Map>)localEntry.getValue();
                                        for (Map paramsMap:
                                                paramsList) {
                                            structureMap.put(paramsMap.get("prop").toString(), paramsMap.get("value").toString()); // e.g. type: debit
                                        }
                                    }
                                }
                            }
                        }
                    }

                    break;
                case "R": // the same parameter mapping logic as with node types E
                    structureMap.put("r"+ recursionLevel, property.getValue().toString());

                    if(params != null){
                        while(it.hasNext()){
                            Map.Entry paramEntry = (Map.Entry) it.next();

                            int paramEntryLevel = Integer.valueOf(paramEntry.getKey().toString().substring(1, 2));
                            if (paramEntryLevel == recursionLevel) {
                                Map localParams = (Map)paramEntry.getValue();

                                localIt = localParams.entrySet().iterator();
                                while(localIt.hasNext()){
                                    Map.Entry localEntry = (Map.Entry)localIt.next();
                                    if(localEntry.getKey().toString().equals("R")){
                                        List<Map> paramsList = (List<Map>)localEntry.getValue();
                                        for (Map paramsMap:
                                                paramsList) {
                                            structureMap.put(paramsMap.get("prop").toString(), paramsMap.get("value").toString());
                                        }
                                    }
                                }
                            }
                        }
                    }
                  //  System.err.println("Structure Map " +  recursionLevel + ": " + structureMap.values());

                    break;
                case "S": // go to another recursion level and forward current structure and subgraph maps
                    Map sMap = (Map) property.getValue();
                    recursionLevel = recursionLevel + 1;
                    structureMap = buildMapStructure(recursionLevel, sMap, structureMap, params);
                    break;
                default:
                    return structureMap;
            }
        }
        return structureMap;
    }

    private String buildDBCountQuery(String inputPattern, String propertyCondName, String propertyCondValue, String constraintPattern){

        String strippedInput = inputPattern.replaceAll("\\{.*?\\}", "");

        Pattern relLabelPattern = Pattern.compile("\\[(.*?)\\]");
        Matcher relLabelMatcher = relLabelPattern.matcher(strippedInput);
        List<String> matches = new ArrayList<>();

        while(relLabelMatcher.find()){
            matches.add(relLabelMatcher.group(1).split(":")[0]);
        }

        String relationshipLabel = matches.get(0); // get relationship R variable name

        String countQuery = "MATCH " + constraintPattern + " WHERE n1." + propertyCondName + "='" + propertyCondValue + "' RETURN COUNT(" + relationshipLabel + ") as number";

        return countQuery;
    }

    public int getCurrentNumberOfRels(String inputPattern, String propertyCondName, String propertyCondValue, String constraintPattern) {
        long numRels = 0;

        String countQuery = buildDBCountQuery(inputPattern, propertyCondName, propertyCondValue, constraintPattern);

        System.out.println("Count query: " + countQuery);
        Result countResult = db.execute(countQuery);

        if (countResult.hasNext()) {
            while (countResult.hasNext()) {
                numRels = (long) countResult.next().get("number");

                System.out.println("[CREATE] Numrels: " + numRels);
            }
        }

        return (int)numRels;
    }

    public boolean checkRelationshipExistence(String startNode, String pattern) {
        boolean existsRel = false;

        pattern = pattern.substring(pattern.indexOf("-"),pattern.length());
        String dbQuery = "MATCH p=(" + startNode + ")" + pattern + " RETURN p";
        Result dbResults = db.execute(dbQuery);

        if (dbResults.hasNext())
            existsRel = true;
        return existsRel;
    }

    public boolean checkRelationshipInputPattern(PatternMatcher matcher, LocalCardinalityConstraint constraint){
        boolean existsRel = false;

        TreeMap constraintMap = new TreeMap();
        constraintMap.put("E", constraint.nodeLabel);
        constraintMap.put("R", constraint.relType);
        constraintMap.put("S", constraint.subgraph);

        Structure structure = new Structure();
        Map recurConstraintMap = buildMapStructure(1, constraintMap, structure.structureMap, constraint.params);
        structure.structureMap.putAll(recurConstraintMap);

        //build structure map for constraint pattern query

        if(matcher.inputPatternMap.entrySet().containsAll(structure.structureMap.entrySet())){
            existsRel = true;
        }

        return existsRel;
    }

    public List<LocalCardinalityConstraint> retrieveConstraints(String condition) {
        List<LocalCardinalityConstraint> constraints = new ArrayList<>();
        Gson gson = new Gson();
        Map<String, Object> map = new HashMap<>();

        Result resultConstraints = null;
        if (condition != null)
            resultConstraints = db.execute("MATCH (c:CardinalityConstraint) WHERE c.E = '" + condition + "' RETURN c");
        else
            resultConstraints = db.execute("MATCH (c:CardinalityConstraint) RETURN c");

        while (resultConstraints.hasNext()) {
            Map<String, Object> row = resultConstraints.next();
            Node n = (Node) row.get("c");
            long id = n.getId();
            String relType = n.getProperty("R").toString();
            String nodeLabel = n.getProperty("E").toString();
            map = gson.fromJson(n.getProperty("S").toString(), map.getClass());
            Number min = (long) n.getProperty("min");
            String max = n.getProperty("max").toString();
            Number k = (long) n.getProperty("k");

            Map params = null;
            if(n.hasProperty("params"))
               params = gson.fromJson(n.getProperty("params").toString(), map.getClass());
            LocalCardinalityConstraint constraint = new LocalCardinalityConstraint(id, relType, nodeLabel, map, min, max, k, params);

            constraints.add(constraint);
        }
        return constraints;
    }
}

