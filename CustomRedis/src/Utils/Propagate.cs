using System.Text;

namespace CustomRedis.Utils;

/* This class contains methods that the master will use to propagate write commands to its replicas*/
public static class Propagate
{

    public static async Task Set(string key, string value, string? expiresInMillis = null)
    {
        if (Server.Replicas.Count == 0)
            return;
        
        String msg = String.Empty;
        msg += String.Join("\r\n", [
            "$3",
            "SET",
            $"${key.Length}",
            key,
            $"${value.Length}",
            value,
        ]);
        if (expiresInMillis != null)
        {
            string exp = String.Join("\r\n", [
                "$2",
                "PX",
                
                $"${expiresInMillis.Length}",
                expiresInMillis
            ]);
            msg = String.Join("\r\n",
            [
                "*5",
                msg,
                exp
            ]);
        }
        else
        {
            msg = String.Join("\r\n",
            [
                "*3",
                msg
            ]);
        }
        msg = $"{msg}\r\n";

        Interlocked.Add(ref Server.MasterReplOffset, Encoding.UTF8.GetByteCount(msg));  //Thread-safe atomic increment
        // Server.MasterReplOffset += Encoding.UTF8.GetByteCount(msg);

        foreach (var replica in Server.Replicas.Values)
        {
            Console.WriteLine($"Propagating {msg} from master to replica");
            await replica.SendAsync(Encoding.UTF8.GetBytes(msg)); 
        }
    }
    
}