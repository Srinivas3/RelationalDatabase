package aggregators;

public class AggregateFactory {
    public static Aggregator getAggregator(String functionName) {
        if (functionName.equalsIgnoreCase("SUM"))
            return new SumAggregator();
        else if(functionName.equalsIgnoreCase("COUNT"))
            return new CountAggregator();
        else if(functionName.equalsIgnoreCase("MIN"))
            return new MinAggregator();
        else if(functionName.equalsIgnoreCase("MAX"))
            return new MaxAggregator();
        else if(functionName.equalsIgnoreCase("AVG"))
            return new AverageAggregator();
        else
            return null;
    }

}
