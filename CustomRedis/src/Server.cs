using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using CustomRedis.Replication;
using CustomRedis.RESPParser;
using CustomRedis.Utils;
using static CustomRedis.Utils.ConfigParams.Param;

namespace CustomRedis;

public class Server
{
    public static ConcurrentDictionary<IntPtr, Replica> Replicas = new (); //A master server can have 1 or more replicas. This list will be empty if the server is itself a replica
    public static int MasterReplOffset = 0;     //Tracks how many bytes were sent by master to replica

    public static async Task Main(String[] args)
    {
        StoreConfigParams(args);
        if (ServerIsReplica())
        {
            /* a replica (or slave) should not accept client commands or communicate with clients before the handshake
             and full resynchronization (FULLRESYNC) with the master are complete. */
            await ReplicaHandler.HandshakeAsync();
            
            //Offload sync and other comms with master to a thread on thread pool
            _ = Task.Run(() => ReplicaHandler.HandleMasterCommAsync());    //no await, we don't want replica to get blocked while it's listening for comm with master
        }

        if (ConfigParams.ContainsKey(Dir) && ConfigParams.ContainsKey(DbFilename))
        {
            new RdbParser.RdbParser().Parse();
        }
        await StartAsync();
    }

    private static bool ServerIsReplica()
    {
        return ConfigParams.Get(Role) == Constant.Role.Slave;
    }

    private static void StoreConfigParams(string[] args)
    {
        ConfigParams.Set(Role, Constant.Role.Master); //By default every server is a master
        
        //Every redis master has a replicationID. This is set when master is booted.
        //every time a master instance restarts from scratch, it's replID is reset.
        ConfigParams.Set(ReplId, "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");   
        
        // Each master also maintains a "replication offset" corresponding to how many bytes of commands have been added to the replication stream.
        ConfigParams.Set(ReplOffset,"0");       //the value starts from 0 when a master is booted and no replicas have connected yet.

        for (var i = 0; i < args.Length; i++)
        {
            if (args[i].StartsWith("--"))
            {
                string key = args[i].Substring(2);
                
                // Check if there's a next argument for its value
                if (i+1 < args.Length && !args[i+1].StartsWith("--"))
                {
                    if (key.Equals("replicaof", StringComparison.OrdinalIgnoreCase))
                    {
                        ConfigParams.Set(Role, Constant.Role.Slave);
                        string[] masterHostPortPair = args[i + 1].Split();
                        string masterHost = masterHostPortPair[0];
                        string masterPort = masterHostPortPair[1];
                        ConfigParams.Set(MasterHost, masterHost);
                        ConfigParams.Set(MasterPort, masterPort);
                    }
                    else
                    {
                        ConfigParams.Set(ConfigParams.DecodeParam(key), args[i+1]);
                    }
                    i++; //skip the value for the next iteration
                }
            }
        }
    }

    private static async Task StartAsync()
    {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        Console.WriteLine("Logs from your program will appear here!");

        int port = SetPortNumber();
        TcpListener server = new TcpListener(IPAddress.Any, port);
        server.Start();
        Console.WriteLine($"Started server on port {port} ... ");


        while (true)
        {
            Socket clientSocket = await server.AcceptSocketAsync();     //wait for client
            
            //Use Task.Run to offload the client handling to a thread pool thread.
            _ = Task.Run(() => HandleClientAsync(clientSocket)); //no awaits, execution happens in background and support concurrent clients       
        }
    }

    private static int SetPortNumber()
    {
        int port = Constant.DefaultPort.ClientFacing;
        if (ConfigParams.ContainsKey(Port))
        {
            var configPort = ConfigParams.Get(Port);
            if (int.TryParse(configPort, out var p) && (p >= 0 && p < 65535))
                port = p;
            else
            {
                Console.WriteLine($"Invalid port {configPort}: argument must be between 0 and 65535 inclusive");
                Environment.Exit(1);
            }
        }
        return port;
    }

    private static async Task HandleClientAsync(Socket clientSocket)
    {
        while(clientSocket.Connected)
        {
            ReplicaHandler.SyncWithMasterMre.WaitOne();    //Clients of replica must wait until sync with master is complete
            //Store incoming data into buffer
            byte[] buffer = new byte[1024];
            int bytesRead = await clientSocket.ReceiveAsync(buffer);
            if (bytesRead == 0)
            {
                clientSocket.Close();
            }
            string respInput = Encoding.UTF8.GetString(buffer, 0, bytesRead);

            object tokenizedCommand = RespParser.Parse(respInput);
            
            CommandRunner commandRunner = new CommandRunner(clientSocket);
            
            await commandRunner.RunCommandAsync(tokenizedCommand as List<object>);
        }    
    }

    public static async Task SendFullResyncResponse(Socket clientSocket)
    {
        /*This assumes the command is `PSYNC ? -1` and not something like `PSYNC ? 400`
        indicating that the replica is attempting to sync with master for the first time*/
        string masterReplId = ConfigParams.Get(ReplId);
        await clientSocket.SendAsync(Encoding.UTF8.GetBytes($"+FULLRESYNC {masterReplId} 0\r\n"));
        
        // After sending the FULLRESYNC response, the master will then send a RDB file of its current state to the replica.
        // The replica is expected to load the file into memory, replacing its current state.
        /* This assumes that the masters Rdb is always empty. */
        byte[] emptyRdbFileBinaryData = Convert.FromBase64String(
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
        byte[] rdbResyncFileMsg = Encoding.UTF8.GetBytes($"${emptyRdbFileBinaryData.Length}\r\n")
            .Concat(emptyRdbFileBinaryData).ToArray();
        await clientSocket.SendAsync(rdbResyncFileMsg);
    }
}