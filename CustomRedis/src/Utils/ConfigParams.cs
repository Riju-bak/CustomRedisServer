namespace CustomRedis.Utils;

public static class ConfigParams
{
    public enum Param
    {
        Dir,
        DbFilename,
        Port,           //This is the port open on server (either master or replica) serving client requests
        Role,
        ReplicaOf,
        ReplId,         //Both master and replica have a replId, this helps to keep track of sync 
        ReplOffset,     
        MasterHost,     //Address of master of a replica
        MasterPort      //Port open on master for communication with replica.
    }
    
    private static Dictionary<Param, string> _parameters = new();

    public static void Set(Param k, string value)
    {
        if (!_parameters.TryAdd(k, value))
        {
            _parameters[k] = value;
        }
    }

    public static string Get(Param k)
    {
        return _parameters[k];
    }

    public static Param DecodeParam(string key)
    {
        Param k = new();
        if (key.Equals("dir", StringComparison.OrdinalIgnoreCase)) k = Param.Dir;
        else if (key.Equals("dbfilename", StringComparison.OrdinalIgnoreCase)) k = Param.DbFilename;
        else if (key.Equals("port", StringComparison.OrdinalIgnoreCase)) k = Param.Port;
        else if (key.Equals("role", StringComparison.OrdinalIgnoreCase)) k = Param.Role;
        else if (key.Equals("replicaof", StringComparison.OrdinalIgnoreCase)) k = Param.ReplicaOf;
        else if (key.Equals("master_replid", StringComparison.OrdinalIgnoreCase)) k = Param.ReplId;
        else if (key.Equals("master_repl_offset", StringComparison.OrdinalIgnoreCase)) k = Param.ReplOffset;
        return k;
    }

    public static bool ContainsKey(Param k)
    {
        return _parameters.ContainsKey(k);
    }
}