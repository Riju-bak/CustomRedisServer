using System.Net.Sockets;
using System.Text;
using CustomRedis.Replication;
using CustomRedis.Utils;
using static CustomRedis.Utils.ConfigParams.Param;

namespace CustomRedis;

public class CommandRunner
{
    private readonly Socket _clientSocket;
    private static Mutex _replOffsetMutex = new Mutex();
    
    public CommandRunner(Socket clientSocket)
    {
        _clientSocket = clientSocket;
    }
    
    public async Task RunCommandAsync(List<object>? tokenizedCommand)
    {
        string command = (tokenizedCommand![0] as string)!;
        if (command.Equals("EXEC", StringComparison.OrdinalIgnoreCase))
        {
            await Exec();
        }
        else if(command.Equals("DISCARD", StringComparison.OrdinalIgnoreCase))
        {
            await Discard();
        }
        if (!command.Equals("EXEC", StringComparison.OrdinalIgnoreCase) &&
            Server.TransactionQueue.ContainsKey(_clientSocket.Handle))
        {
            //This means MULTI has been called, and subsequent commands must be queued
            Server.TransactionQueue[_clientSocket.Handle].Enqueue(tokenizedCommand);
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("+QUEUED\r\n"));
        }
        else if (command.Equals("PING", StringComparison.OrdinalIgnoreCase))
        {
            // TCP is a byte stream protocol. To send string we convert it to byte arrays.
            // Essentially we send a stream of bytes
            //  that TCP divides into packets.
            byte[] response = Encoding.UTF8.GetBytes("+PONG\r\n");
            await _clientSocket.SendAsync(response);
        }
        else if (command.Equals("ECHO", StringComparison.OrdinalIgnoreCase))
        {
            string? msg = tokenizedCommand[1] as string;
            await Echo(msg);
        }
        else if(command.Equals("SET", StringComparison.OrdinalIgnoreCase))
        {
            string? key = (tokenizedCommand[1] as string)!;
            string? value = (tokenizedCommand[2] as string)!;
            string? expiresInMillis = null;
            // if (tokenizedCommand.Contains("PX")) expiresInMillis = (tokenizedCommand[4] as string)!;
            if (3 < tokenizedCommand.Count && (tokenizedCommand[3] as string)!.Equals("PX", StringComparison.OrdinalIgnoreCase))
            {
                expiresInMillis = tokenizedCommand[4] as string;
            }
            KeyValueStore.Set(key, value, expiresInMillis);
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
            await Propagate.Set(key, value, expiresInMillis);
        }
        else if (command.Equals("GET", StringComparison.OrdinalIgnoreCase))
        {
            await Get(key: tokenizedCommand[1] as string);
        }
        else if (command.Equals("CONFIG", StringComparison.OrdinalIgnoreCase))
        {
            string? configCommand = tokenizedCommand[1] as string;
            if (configCommand!.Equals("SET", StringComparison.OrdinalIgnoreCase))
            {
                List<Tuple<string, string>> paramsAndValues = new();
                for (int i=1; i < tokenizedCommand.Count; )
                {
                    var param = tokenizedCommand[i++] as string;
                    var value = tokenizedCommand[i++] as string;
                    paramsAndValues.Add(new Tuple<string, string>(param!, value!));
                }
                await ConfigSet(paramsAndValues);
            }
            else if (configCommand.Equals("GET", StringComparison.OrdinalIgnoreCase))
            {
                List<string> parameters = new();
                for (int i=2; i < tokenizedCommand.Count; i++)
                {
                    var param = tokenizedCommand[i] as string;
                    parameters.Add(param!);
                }
                await ConfigGet(parameters);
            }
        }
        else if (command.Equals("INFO", StringComparison.OrdinalIgnoreCase))
        {
            string? section = tokenizedCommand[1] as string;
            if(section != null)
                await DisplayInfo(section);
            else
                await DisplayInfo();
        }
        else if(command.Equals("REPLCONF", StringComparison.OrdinalIgnoreCase))
        {
            if ((tokenizedCommand[1] as string)!.Equals("ACK", StringComparison.OrdinalIgnoreCase))
            {
                //REPLCONF ACK <offset> req will come only from a replica. So we know the _clientSocket reprsents a replica conn.
                int replSlaveOffset = int.Parse((tokenizedCommand[2] as string)!);
                            
                _replOffsetMutex.WaitOne();
                Console.WriteLine($"Updating replica's offset to {replSlaveOffset}");
                Replica repl = Server.Replicas[_clientSocket.Handle];
                repl.Offset = replSlaveOffset;
                repl.SentAnAckToMaster = true;
                repl.MasterPropagatedWriteCmdToMe = true;
                _replOffsetMutex.ReleaseMutex();
            }
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
        }
        else if (command.Equals("PSYNC", StringComparison.OrdinalIgnoreCase))
        {
            /*NOTE: Below are the replId and replOffset that the replica wants to sync with.
                It may not be the actual replId of the master */ 
            string reqReplId = (tokenizedCommand[1] as string)!;  
            string reqReplOffset = (tokenizedCommand[2] as string)!;

            await Psync(reqReplId, reqReplOffset);
        }
        else if(command.Equals("KEYS", StringComparison.OrdinalIgnoreCase))
        {
            string pattern = (tokenizedCommand[1] as string)!;
            await Keys(pattern);
        }
        else if(command.Equals("WAIT", StringComparison.OrdinalIgnoreCase))
        {
            await Wait(minReqReplicasAckStr:
                (tokenizedCommand[1] as string), timeoutStr: (tokenizedCommand[2] as string));
        }
        else if(command.Equals("INCR", StringComparison.OrdinalIgnoreCase))
        {
            await Incr(key: (tokenizedCommand[1] as string)!);
        }
        else if (command.Equals("MULTI", StringComparison.OrdinalIgnoreCase))
        {
            await Multi();
        }
        else if (command.Equals("COMMAND", StringComparison.OrdinalIgnoreCase) &&
                 ((tokenizedCommand[1] as string)!).Equals("DOCS"))
        {
            //This is to support interactive mode
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("*0\r\n"));
        }
                    
        // WARNING: Somehow uncommenting this causes the codecrafters tests to fail - MUST INVESTIGATE
        // else
        // {
        //     string errMsg = $"(error) ERR unknown command '{command}'";
        //     string respErrMsg = $"${errMsg.Length}\r\n{errMsg}\r\n";
        //     clientSocket.Send(Encoding.UTF8.GetBytes(respErrMsg)); 
        // }
    }

    private async Task Discard()
    {
        if (!Server.TransactionQueue.TryGetValue(_clientSocket.Handle, out var q))
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("-ERR DISCARD without MULTI\r\n"));
        else
        {
            Server.TransactionQueue.Remove(_clientSocket.Handle, out _);
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
        }
    }

    private async Task Exec()
    {
        Server.TransactionMre.Reset();      //Block all client requests while a transaction is being executed
        if (!Server.TransactionQueue.TryGetValue(_clientSocket.Handle, out var q))
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("-ERR EXEC without MULTI\r\n"));
        else
        {
            string msg = $"*{q.Count}\r\n";
            CommandExecutor ce = new CommandExecutor(_clientSocket);
            while(q.Count > 0)
            {

                List<object>? tokenizedCommand = q.Dequeue();
                string response = await ce.ExecCommandAsync(tokenizedCommand);
                msg += response;
            }
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes(msg));
            Server.TransactionQueue.Remove(_clientSocket.Handle, out _);
        }
        Server.TransactionMre.Set();    //Unblock the client threads to handle their requests
    }

    private async Task Multi()
    {
        if(!Server.TransactionQueue.ContainsKey(_clientSocket.Handle))
        {
            Server.TransactionQueue.TryAdd(_clientSocket.Handle, new Queue<List<object>?>());
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
        }
    }    
    private async Task Incr(string key)
    {
        string response = KeyValueStore.Increment(key);
        await _clientSocket.SendAsync(Encoding.UTF8.GetBytes(response));
    }

    private async Task Wait(string? minReqReplicasAckStr, string? timeoutStr)
    {
        int minReqReplicasAck = int.Parse(minReqReplicasAckStr ?? "0");
        int timeout = int.Parse(timeoutStr ?? "1");

        string getAckReq = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        int count = 0;

        // CancellationTokenSource for the timeout
        var cts = new CancellationTokenSource();
        var timeoutTask = Task.Delay(timeout, cts.Token);  // This task will complete after timeout ms

        foreach (var replica in Server.Replicas.Values)
        {
            if (timeoutTask.IsCompleted)
            {
                break;
            }
        
            await replica.SendAsync(Encoding.UTF8.GetBytes(getAckReq));
        
            if (replica.SentAnAckToMaster)
            {
                Interlocked.Increment(ref count);
            }
        }

        // Console.WriteLine($"GOT OUT OF DA LOOP, count is now {count}");
        await _clientSocket.SendAsync(Encoding.UTF8.GetBytes($":{count}\r\n"));
    }

    private async Task Keys(string pattern)
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
        await _clientSocket.SendAsync(Encoding.UTF8.GetBytes(msg));
    }

    private async Task Psync(string reqReplId, string reqReplOffset)
    {
        if (reqReplId.Equals("?") && reqReplOffset.Equals("-1"))
        {
            //PSYNC ? -1 is only sent once during the initial connection (handhshake)
            //The master does not expect to receive this command again unless the replica has lost its replication state.
            await Server.SendFullResyncResponse(_clientSocket);

            //Once, handshake is complete, we know _clientSocket represents the connection to a replica.
            var replica = new Replica(_clientSocket);
            if(!Server.Replicas.TryAdd(_clientSocket.Handle, replica))
                Server.Replicas[_clientSocket.Handle] = replica;
        }
    }

    private async Task DisplayInfo()
    {
        //WARNING: This is incomplete. Low priority. Might need to implement later
        List<string> supportedSections = new List<string>()
            { "server", "clients", "memory", "persistance", "stats", "replication", "cpu" };
        foreach (string section in supportedSections) 
            await DisplayInfo(section);
    }

    private async Task DisplayInfo(string section)
    {
        if (section.Equals("REPLICATION", StringComparison.OrdinalIgnoreCase))
        {
            string info = string.Join("\r\n", [
                $"role:{ConfigParams.Get(Role)}",
                $"master_replid:{ConfigParams.Get(ReplId)}",
                $"master_repl_offset:{ConfigParams.Get(ReplOffset)}"
            ]);

            string msg = $"${info.Length}\r\n{info}\r\n";
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes(msg));
        }
    }

    private async Task ConfigSet(List<Tuple<string, string>> paramsAndValues)
    {
        foreach (var (param, value) in paramsAndValues)
        {
            ConfigParams.Set(ConfigParams.DecodeParam(param), value);
        }
        await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
    }

    private async Task ConfigGet(List<string> parameters)
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
        await _clientSocket.SendAsync(Encoding.UTF8.GetBytes(msg));
    }

    private async Task Get(string? key)
    {
        if (key != null)
        {
            if (KeyValueStore.ContainsKey(key))
            {
                if (!KeyValueStore.Expired(key))
                {
                    (string, DateTime?) valueTuple = KeyValueStore.Get(key);
                    var value = valueTuple.Item1;
                    string msg = $"${value.Length}\r\n{value}\r\n";
                    _clientSocket.Send(Encoding.UTF8.GetBytes(msg));
                }
                else
                {
                    //key has expired and must be removed
                    KeyValueStore.Remove(key);
                    await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("$-1\r\n"));
                }
            }
            else
            {
                // Key doesn't exist
                await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("$-1\r\n"));
            }
        }
    }

    private async Task Set(string? key, string? value, string? expiresInMillis = null)
    {
        if (key != null && value != null)
        {
            KeyValueStore.Set(key, value, expiresInMillis!);
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes("+OK\r\n"));
        }
    }

    private async Task Echo(string? msg)
    {
        if (msg != null)
        {
            string respMsg = $"${msg.Length}\r\n{msg}\r\n";
            await _clientSocket.SendAsync(Encoding.UTF8.GetBytes(respMsg));
        }
    }
}