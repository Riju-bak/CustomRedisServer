using System.Net.Sockets;
using System.Text;
using CustomRedis.Utils;

namespace CustomRedis;

public class CommandExecutor(Socket clientSocket)
{
    public async Task<string> ExecCommandAsync(List<object>? tokenizedCommand)
    {
        string response = "";
        string command = (tokenizedCommand![0] as string)!;
        if (command.Equals("PING", StringComparison.OrdinalIgnoreCase)) response = "+PONG\r\n";
        else if (command.Equals("ECHO", StringComparison.OrdinalIgnoreCase))
        {
            string msg = (tokenizedCommand[1] as string)!;
            response = $"${msg.Length}\r\n{msg}\r\n";
        }
        else if (command.Equals("SET", StringComparison.OrdinalIgnoreCase))
        {
            string? key = (tokenizedCommand[1] as string)!;
            string? value = (tokenizedCommand[2] as string)!;
            string? expiresInMillis = null;
            if (tokenizedCommand.Contains("PX")) expiresInMillis = (tokenizedCommand[4] as string)!;
            KeyValueStore.Set(key, value, expiresInMillis);
            await Propagate.Set(key, value, expiresInMillis);
            response = "+OK\r\n";
        }
        else if (command.Equals("GET", StringComparison.OrdinalIgnoreCase))
        {
            string key = (tokenizedCommand[1] as string)!;
            if (KeyValueStore.ContainsKey(key))
            {
                if (!KeyValueStore.Expired(key))
                {
                    (string, DateTime?) valueTuple = KeyValueStore.Get(key);
                    var value = valueTuple.Item1;
                    response = $"${value.Length}\r\n{value}\r\n";
                }
                else
                {
                    KeyValueStore.Remove(key);
                    response = "$-1\r\n";
                }
            }
            else response = "$-1\r\n";

        }
        else if (command.Equals("CONFIG", StringComparison.OrdinalIgnoreCase))
        {
            string configCmd = (tokenizedCommand[1] as string)!;
            if (configCmd.Equals("SET", StringComparison.OrdinalIgnoreCase))
            {
                List<Tuple<string, string>> paramsAndValues = new();
                for (int i=1; i < tokenizedCommand.Count; )
                {
                    var param = tokenizedCommand[i++] as string;
                    var value = tokenizedCommand[i++] as string;
                    paramsAndValues.Add(new Tuple<string, string>(param!, value!));
                }
                foreach (var (param, value) in paramsAndValues)
                {
                    ConfigParams.Set(ConfigParams.DecodeParam(param), value);
                }
                response = "+OK\r\n";
            }
            else if (configCmd.Equals("GET", StringComparison.OrdinalIgnoreCase))
            {
                List<string> parameters = new();
                for (int i = 1; i < tokenizedCommand.Count; i++)
                {
                    var param = tokenizedCommand[i] as string;
                    parameters.Add(param!);
                }
                response = ConfigGet(parameters);
            }
        }
        else if(command.Equals("KEYS", StringComparison.OrdinalIgnoreCase))
        {
            string pattern = (tokenizedCommand[1] as string)!;
            response = Keys(pattern);
        }
        else if (command.Equals("INCR", StringComparison.OrdinalIgnoreCase))
        {
            string key = (tokenizedCommand[1] as string)!;
            response = KeyValueStore.Increment(key);
        }
        return response;
    }

    private string Keys(string pattern)
    {
        //NOTE: This assumes that only pattern "*" i.e all keys are requested from this method.
        // A full implementation must do glob pattern matching
        var keys = KeyValueStore.Keys();
        List<string> validKeys = new List<string>();
        
        foreach (var key in keys)
            if (true) validKeys.Add(key); //if(true) must be replace with if(GlobMatch(key, pattern)) for a full implementation
        
        var respL = new List<string> { $"*{validKeys.Count}"};
        respL.AddRange(validKeys.Select(key => $"${key.Length}\r\n{key}"));
        string msg = $"{string.Join("\r\n", respL)}\r\n";
        return msg;
    }

    private string ConfigGet(List<string> parameters)
    {
        int numParams = parameters.Count;
        string msg = $"*{numParams*2}\r\n";
        for (int i=0; i<parameters.Count; i++)
        {
            string parameter = parameters[i];
            var key = ConfigParams.DecodeParam(parameter);
            if (ConfigParams.ContainsKey(key))
            {
                string value = ConfigParams.Get(key);
                msg += $"${parameter.Length}\r\n{parameter}\r\n${value.Length}\r\n{value}\r\n";
            }
            else
            {
                msg += $"$-1\r\n";
            }
        }
        return msg;
    }

    public async Task SendToClient(string msg)
    {
        await clientSocket.SendAsync(Encoding.UTF8.GetBytes(msg));
    }
}