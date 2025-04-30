namespace CustomRedis.Utils;

public class BulkCommandsParser
{
    public static List<string> RetrieveIndivCommandsFromBulk(string s)
    {
        int j = 0;
        List<string> parts = new List<string>();
        List<string> tokens = s.Split("\r\n", StringSplitOptions.RemoveEmptyEntries).ToList();
        while (j<tokens.Count)
        {
            string token = tokens[j];
            if (tokens[j].StartsWith('*') && token.Length > 1)
            {
                int arrSize = int.Parse(tokens[j][1..]);
                parts.Add(string.Join("\r\n", tokens[j..(j + arrSize*2 + 1)]));
                j += arrSize * 2 + 1;
            }
        }
        return parts;
    }
}