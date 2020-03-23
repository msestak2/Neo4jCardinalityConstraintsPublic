package card_constraint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Set of methods for parsing input query pattern
 */
public class PatternMatcher {

    private String patternWithoutConditions;
    private String patternNoNodeConditions;
    private String inputPattern;
    private String patternVariables;
    private List<String> matches;
    private List<String> inputArray;
    private List<String> nodeTypes;
    public Map inputPatternMap;

    public PatternMatcher() {
        this.matches = new ArrayList<>();
        this.patternWithoutConditions = "";
        this.patternNoNodeConditions = "";
        this.inputArray = new ArrayList<>();
        this.nodeTypes = new ArrayList<>();
        this.inputPatternMap = new TreeMap();
        this.patternVariables = "";
    }

    public void parseNodesRelationships(String inputPath){
        // extract nodes and relationship components of the input query with regex
        Pattern nodesRelsPattern = Pattern.compile("\\((.*?)\\)|\\[(.*?)\\]");
        Matcher nodesRelsMatcher = nodesRelsPattern.matcher(inputPath);

        this.patternWithoutConditions = "";
        while(nodesRelsMatcher.find()){
            if (nodesRelsMatcher.group(1) != null){ // node
                this.matches.add(nodesRelsMatcher.group(1));
                this.patternWithoutConditions += "(" + nodesRelsMatcher.group(1).replaceAll("\\{.*?\\}", "").trim() + ")-"; // add node without conditions to pattern
                this.patternNoNodeConditions += "(" + nodesRelsMatcher.group(1).replaceAll("\\{.*?\\}", "").split(":")[0].trim() + ")-"; // add only the node variable to pattern for MERGE query purpose
                this.nodeTypes.add(nodesRelsMatcher.group(1)); // add entire node component to the list of nodes
            } else if(nodesRelsMatcher.group(2) != null) { // relationship
                this.matches.add(nodesRelsMatcher.group(2));
                this.patternWithoutConditions += "[" + nodesRelsMatcher.group(2).replaceAll("\\{.*?\\}", "").trim() + "]->"; // add relationship without conditions to pattern
                this.patternNoNodeConditions += "[" + nodesRelsMatcher.group(2).trim() + "]->"; // add entire relationship component to the MERGE query pattern
            }

        }

        // remove last "-" character from patterns
        this.patternWithoutConditions = this.patternWithoutConditions.substring(0, this.patternWithoutConditions.length()-1);
        this.patternNoNodeConditions = this.patternNoNodeConditions.substring(0, this.patternNoNodeConditions.length()-1);
    }

    public void removeConditionsFromPattern(){
        this.inputPattern = this.inputPattern.replaceAll("\\{.*?\\}", "");
        this.inputPattern = this.inputPattern.replaceAll("\\s", "");

        Pattern varPattern = Pattern.compile("\\((.*?)\\)|\\[(.*?)\\]");
        Matcher varMatcher = varPattern.matcher(inputPattern);

        List<String> variableMatches = new ArrayList<>();

        while(varMatcher.find()){
            if (varMatcher.group(1) != null) {
                variableMatches.add(varMatcher.group(1).split(":")[0]);
                this.patternVariables += "(" + varMatcher.group(1).split(":")[0] + ")";
                this.inputArray.add(varMatcher.group(1).split(":")[1]);
            }
            else if(varMatcher.group(2) != null) {
                variableMatches.add(varMatcher.group(2).split(":")[1]);
                this.patternVariables += "-[" + varMatcher.group(2) + "]->";
                this.inputArray.add(varMatcher.group(2).split(":")[1]);;
            }
        }

    }

    public void buildMapFromInputPattern(){
        Pattern varPattern = Pattern.compile("\\((.*?)\\)|\\[(.*?)\\]");
        Matcher varMatcher = varPattern.matcher(this.inputPattern);

        while(varMatcher.find()){
            if (varMatcher.group(1) != null)
                inputPatternMap.put(varMatcher.group(1).split(":")[0], varMatcher.group(1).split(":")[1].replaceAll("'", "")); // add node type to map
            else if(varMatcher.group(2) != null)
                inputPatternMap.put(varMatcher.group(2).split(":")[0], varMatcher.group(2).split(":")[1].replaceAll("'", "")); // add relationship type to map
        }

    }

    public String getPatternWithoutConditions() {
        return patternWithoutConditions;
    }

    public List<String> getMatches() {
        return matches;
    }

    public String getInputPattern() {
        return inputPattern;
    }

    public void setInputPattern(String inputPattern) {
        this.inputPattern = inputPattern;
    }

    public List<String> getInputArray() {
        return inputArray;
    }

    public List<String> getNodeTypes() {
        return nodeTypes;
    }

    public Map getInputPatternMap() {
        return inputPatternMap;
    }

    public String getPatternVariables() {
        return patternVariables;
    }

    public void setPatternVariables(String patternVariables) {
        this.patternVariables = patternVariables;
    }

    public String getPatternNoNodeConditions() {
        return patternNoNodeConditions;
    }
}
