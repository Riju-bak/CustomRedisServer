namespace CustomRedis.Utils;

public static class Constant
{
    public static class Opcode
    {
        public const byte Auxilary = 0xFA;
        public const byte Resizedb = 0xFB;
        public const byte ExpiretimeMs = 0xFC;
        public const byte Expiretime = 0xFD;    //expire time in s
        public const byte Selectdb = 0xFE;
        public const byte Eof = 0xFF;
    }

    public static class Role
    {
        public const string Master = "master";
        public const string Slave = "slave";
    }

    public static class DefaultPort
    {
        public const int ClientFacing = 6379;       //port open for client sv comm (sv can be either master or replica)
        public const int ReplListening = 6380;      //Port open on replica for master-replica comm
    }

    public static class ValueType
    {
        public const byte Str = 0;
    }
}