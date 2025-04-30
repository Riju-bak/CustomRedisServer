using System.Net.Sockets;
using System.Text;

namespace CustomRedis.Replication;

public class Replica
{
    public Socket? Socket { get; set; }

    public int Offset { get; set; } = 0;

    public bool MasterPropagatedWriteCmdToMe { get; set; } = false; //A flag to check on TCP level if master actually propagated the write command

    public bool SentAnAckToMaster { get; set; } = false;

    public Replica(Socket socket) => Socket = socket;

    public async Task SendAsync(byte[] msg)
    {
        MasterPropagatedWriteCmdToMe = false; //In-case this was set to true after a previous propagation
        await Socket!.SendAsync(msg);
        bool ackReceived = Socket.Poll(5000, SelectMode.SelectWrite);
        if (ackReceived)
        {
            MasterPropagatedWriteCmdToMe = true;
        }
    }
    
}