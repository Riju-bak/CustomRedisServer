using System.Collections.Concurrent;

namespace CustomRedis.Utils;

public static class KeyValueStore
{
    private static ConcurrentDictionary<string, (string, DateTime?)> _expStorage = new();   //Concurrent dictionary is thread-safe
    
    public static void Set(string key, string value, string? expiresAfterMillis = null)
    {
        DateTime? expiry = null;
        if (expiresAfterMillis != null)
        {
            int expInMillis = int.Parse(expiresAfterMillis);
            expiry = DateTime.Now.AddMilliseconds(expInMillis).ToLocalTime();
        }
        if (!_expStorage.TryAdd(key, (value, expiry)))
        {
            _expStorage[key] = (value, expiry);
        }
    }

    //This method will only be called by RdbParser to add a key read from an rdb file to the key-val store
    public static void Set(string key, string value, ulong? expTimeMsUnix)
    {
        DateTime? expiry = expTimeMsUnix == null
            ? null
            : new DateTime(1970, 1, 1, 0, 0, 0).AddMilliseconds((double)expTimeMsUnix).ToLocalTime();
        _expStorage[key] = (value, expiry);
    }
    

    public static (string, DateTime?) Get(string key)
    {
        _expStorage.TryGetValue(key, out var valueTuple);
        return valueTuple;
    }

    public static void Remove(string key)
    {
        if (_expStorage.ContainsKey(key)) 
            _expStorage.Remove(key, out _);
    }

    public static bool Expired(string key)
    {
        (string, DateTime?) valueTuple = _expStorage[key];
        var expiry = valueTuple.Item2;
        if (expiry is null  || expiry > DateTime.Now)
        {
            return false;
        }
        return true;
    }

    public static bool ContainsKey(string key)
    {
        return _expStorage.ContainsKey(key);
    }

    public static ICollection<string> Keys()
    {
        //Returns a list of all keys
        return _expStorage.Keys;
    }
    
    public static string Increment(string key)
    {
        string response;
        if (!_expStorage.ContainsKey(key))
        {
            Set(key, "1");
            response = ":1\r\n";
        }
        else
        {
            if (int.TryParse(_expStorage[key].Item1, out int val))
            {
                val++;
                Set(key, val.ToString());
                response = $":{val}\r\n";
            }
            else
                response = "-ERR value is not an integer or out of range\r\n";
        }
        return response;
    }
}