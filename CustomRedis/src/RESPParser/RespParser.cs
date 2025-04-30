namespace CustomRedis.RESPParser;

public class RespParser
{
    public static object Parse(string input)
    {
        // Split input by lines
        List<string> lines = input.Split(new [] {"\r\n", "\n"}, StringSplitOptions.RemoveEmptyEntries).ToList();

        // Determine the type based on the first character
        char type = lines[0][0];

        switch (type)
        {
            case '+':
                return lines[0].Substring(1); // Simple string
            case '-':
                return new Exception(lines[0].Substring(1)); // Error
            case ':':
                return int.Parse(lines[0].Substring(1)); // Integer
            case '$':
                return ParseBulkString(lines);
            case '*':
                return ParseArray(lines);
            default:
                throw new InvalidOperationException("Unknown RESP type.");
        }
    }

    private static string ParseBulkString(List<string> lines)
    {
        int length = int.Parse(lines[0].Substring(1)); // Get length of bulk string

        // Return the bulk string
        return lines[1]; // The next line contains the actual string
    }

    private static List<object> ParseArray(List<string> possiblyBadLines)
    {
        //possiblyBadLines = ["*2", "$4", "ECHO", "$3", "hey"]

        List<string> lines = HackyFixForBulkStrings(possiblyBadLines);
        // lines = ["*2", "$4"\nECHO", "$3\nhey"]
        
        int count = int.Parse(lines[0].Substring(1)); // Get the number of elements
        var results = new List<object>();

        
        for(int i=1; i<=count; i++)
        {
            results.Add(Parse(lines[i])); // Recursively parse each element
        }

        return results;
    }

    private static List<string> HackyFixForBulkStrings(List<string> possiblyBadLines)
    {
        List<string> lines = new();
        for (int j = 0; j < possiblyBadLines.Count; j++)
        {
            if (possiblyBadLines[j].StartsWith("$") && j+1<possiblyBadLines.Count)
            {
                lines.Add(possiblyBadLines[j]+"\n"+possiblyBadLines[j+1]);
                j++;
            }
            else
                lines.Add(possiblyBadLines[j]);
        }
        return lines;
    }
}