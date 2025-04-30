using System.Net.Sockets;
using System.Text;
using CustomRedis.RESPParser;
using CustomRedis.Utils;
using static CustomRedis.Utils.ConfigParams.Param;

namespace CustomRedis.Replication;

public class ReplicaHandler
{
    private static TcpClient? _client;
    private static NetworkStream? _masterStream;
    private static int _handshakeDone;
    private static int _totalBytesProcessedPostHandShake;

    //See: https://www.youtube.com/watch?v=DVnk5fn3gZw for a good explanation of ManualResetEvent
    public static readonly ManualResetEvent SyncWithMasterMre = new ManualResetEvent(true); //This will help to block client read requests while syncing with master
    
    
    public static async Task HandshakeAsync()
    {
        string masterHost = ConfigParams.Get(MasterHost);
        int masterPort = int.Parse(ConfigParams.Get(MasterPort));
        _client = new TcpClient(masterHost, masterPort);
        _masterStream = _client.GetStream();      //Open a network _masterStream for replica to initiate handshake with master. Although a replica is technically a server in this case the master treats the replica as another client whose request is being served.
        
        var response = await HandShakePingAsync();
        if (!response.Equals("+PONG\r\n")) return;
        Console.WriteLine($"Master responded with: {response}");

        response = await HandShakeReplConfAsync();
        if (!response.Equals("+OK\r\n")) return;
        Console.WriteLine($"Master responded with: {response}");

        response = await HandShakePsyncAsync();
        Console.WriteLine($"Master responded with {response}");

        // _handshakeDone = true;
        Interlocked.Exchange(ref _handshakeDone, 1);
        
        if (response.IndexOf('*')>0)
        {
            await ProcessInputFromMaster(respInput: response.Substring(response.IndexOf('*')));
        }
    }

    private static async Task<string> HandShakeReplConfAsync()
    {
        int replListeningPort = Constant.DefaultPort.ReplListening;
        Console.WriteLine($"Replica sending handshake REPLCONF listening-port {replListeningPort}");
        await _masterStream!.WriteAsync(
            Encoding.UTF8.GetBytes($"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{replListeningPort}\r\n"));
        
        string response = await GetServerResponse();
        if (!response.Equals("+OK\r\n")) return "";
        Console.WriteLine($"Master responded with: {response}");
        
        Console.WriteLine($"Replica sending handshake REPLCONF capa psync2");
        await _masterStream!.WriteAsync(
            Encoding.UTF8.GetBytes("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"));
        response = await GetServerResponse();
        return response;
    }

    private static async Task<string> HandShakePsyncAsync()
    {
        Console.WriteLine($"Replica sending handshake PSYNC ? -1");
        await _masterStream!.WriteAsync(Encoding.UTF8.GetBytes("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"));
        string response = await GetServerResponse();
        return response;
    }

    private static async Task<string> HandShakePingAsync()
    {
        Console.WriteLine("Replica sending handshake PING.");
        string request = "*1\r\n$4\r\nPING\r\n";
        await _masterStream!.WriteAsync(Encoding.UTF8.GetBytes(request));
        
        var response = await GetServerResponse();
        return response;
    }

    private static async Task<string> GetServerResponse()
    {
        byte[] buffer = new byte[256];
        int bytesRead = await _masterStream!.ReadAsync(buffer);
        string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        if (_handshakeDone == 1)
        {
            // _totalBytesProcessedPostHandShake += bytesRead;
            Interlocked.Add(ref _totalBytesProcessedPostHandShake, bytesRead);
        }
        return response;
    }

    public static async Task HandleMasterCommAsync()
    {
        while (_masterStream!.Socket.Connected)
        {
            string respInput = await GetServerResponse(); //receive the propagated write command from master

            if (!String.IsNullOrEmpty(respInput) && respInput.StartsWith('*'))
            {
                await ProcessInputFromMaster(respInput);
            }
        }
    }

    private static async Task ProcessInputFromMaster(string respInput)
    {
        SyncWithMasterMre.Reset();  //Block all client threads' read requests
        Console.WriteLine($"\r\nMaster propagated write command: {respInput}\r\n");
        List<string> commandsList = BulkCommandsParser.RetrieveIndivCommandsFromBulk(respInput);
        foreach (var cmd in commandsList)
        {
            object tokenizedCommand = RespParser.Parse(cmd);
            await RunCommand(tokenizedCommand as List<object>);
        }
        SyncWithMasterMre.Set();    //Unblock the client threads to handle their read requests
    }

    private static async Task RunCommand(List<object>? commandTokens)
    {
        if (commandTokens != null)
        {
            for (int i = 0; i<commandTokens.Count; i++)
            {
                string? command = commandTokens[i] as string;
                if (command!.Equals("SET", StringComparison.OrdinalIgnoreCase))
                {
                    if (i+3 < commandTokens.Count
                        && (commandTokens[i + 3] as string)!.Equals("PX", StringComparison.OrdinalIgnoreCase)
                        && i+4 < commandTokens.Count)
                    {
                        Set(key: commandTokens[i + 1] as string, value: commandTokens[i + 2] as string,
                            expiresInMillis: commandTokens[i + 4] as string);
                    }
                    else
                    {
                        Set(key: commandTokens[i + 1] as string,  value: commandTokens[i+2] as string);
                    }
                }
                else if (command!.Equals("REPLCONF"))
                {
                    //This assumes that the master sends only "REPLCONF GETACK *"  to the replica if the command starts with "REPLICA"
                    int totalBytesProcessedPostHandshakeDigitLen = _totalBytesProcessedPostHandShake.ToString().Length; //Example: DigitLen of 501 => 3, 20045 => 5 etc.
                    await _masterStream!.WriteAsync(Encoding.UTF8.GetBytes($"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${totalBytesProcessedPostHandshakeDigitLen}\r\n{_totalBytesProcessedPostHandShake}\r\n"));
                }
            }
        }
        
        void Set(string? key, string? value, string? expiresInMillis = null)
        {
            if (key != null && value != null) 
                KeyValueStore.Set(key, value, expiresInMillis!);
        }
    }
    

}