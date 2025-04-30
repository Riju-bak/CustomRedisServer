using CustomRedis.Utils;

namespace CustomRedis.RdbParser;

public class RdbParser
{
    public void Parse()
    {
        string dbfile = $"{ConfigParams.Get(ConfigParams.Param.Dir)}/{ConfigParams.Get(ConfigParams.Param.DbFilename)}";

        try
        {
            using (FileStream fs = new FileStream(dbfile, FileMode.Open, FileAccess.Read))
            {
                using (BinaryReader br = new BinaryReader(fs))
                {
                    Console.WriteLine($"Reading {dbfile} ... ");
                    string magicString = new string(br.ReadChars(5));
                    if (!magicString.Equals("REDIS"))
                        throw new Exception("Invalid RDB file format.");
                    Console.WriteLine($"Magic string: {magicString}");

                    string version = new string(br.ReadChars(4));
                    if (int.TryParse(version, out var v))
                    {
                        if (v < 1 || v > 12)
                            throw new Exception($"Invalid RDB version number {v}");
                    }
                    Console.WriteLine($"version: {version}");

                    LengthEncodingHandler lengthEncodingHandler = new LengthEncodingHandler(br);
                    StringEncodingHandler stringEncodingHandler = new StringEncodingHandler(br);
                    while (true)
                    {
                        byte opcode = br.ReadByte();
                        if (opcode.Equals(Constant.Opcode.Auxilary))
                        {
                            // The op code is followed by two Redis Strings, representing the key and value of a setting. Unknown fields should be ignored by a parser.
                            //read byte
                            byte sz = br.ReadByte();    //e.g 0x0F => This sz byte can be decode to get the length
                            int auxKeyLen = lengthEncodingHandler.Decode(sz);
                            string auxKey = stringEncodingHandler.Decode(numBytesToRead: auxKeyLen);

                            int auxValueLen = lengthEncodingHandler.Decode(br.ReadByte());
                            string auxValue = stringEncodingHandler.Decode(numBytesToRead: auxValueLen);
                        
                            Console.WriteLine($"{auxKey}: {auxValue}");
                        }
                        if (opcode.Equals(Constant.Opcode.Selectdb))
                        {
                            int dbIndex = lengthEncodingHandler.Decode(br.ReadByte());
                            Console.WriteLine($"Db selector: {dbIndex}");
                        }
                        if (opcode.Equals(Constant.Opcode.Resizedb))
                        {
                            int dbHashTableSize = lengthEncodingHandler.Decode(br.ReadByte());
                            int expHashTableSize = lengthEncodingHandler.Decode(br.ReadByte());
                            Console.WriteLine($"Database hash table size: {dbHashTableSize}");
                            Console.WriteLine($"Expiry hash table size: {expHashTableSize}");
                            opcode = ReadDbSection(dbHashTableSize, opcode, br, fs);
                        }
                        if (opcode.Equals(Constant.Opcode.Eof))
                        {
                            ulong checkSum = br.ReadUInt64();
                            Console.WriteLine($"CR64 Checksum: {checkSum}");
                            break;
                        }
                    }
                }
            }
        }
        catch (FileNotFoundException)
        {
            Console.WriteLine($"Error: The file{dbfile} was not found");
        }
    }

    private static byte ReadDbSection(int dbHashTableSize, byte opcode, BinaryReader br, FileStream fs)
    {
        while (dbHashTableSize > 0)
        {
            opcode = br.ReadByte();
            byte valueType;
            if (opcode.Equals(Constant.Opcode.Expiretime))
            {
                uint expTimeUnix = br.ReadUInt32(); //Reads next 4 bytes in little endian (r->l)
                valueType = br.ReadByte();
                if (valueType.Equals(Constant.ValueType.Str))
                {
                    new StringEncodingHandler(br).ReadKeyValPair(expTimeUnix*1000);
                    dbHashTableSize--; if(dbHashTableSize == 0) break;
                }
            }
                            
            else if (opcode.Equals(Constant.Opcode.ExpiretimeMs))
            {
                ulong expTimeMsUnix = br.ReadUInt64();  //Reads next 8 bytes in little endian (r -> l)
                valueType = br.ReadByte();
                if (valueType.Equals(Constant.ValueType.Str))
                {
                    new StringEncodingHandler(br).ReadKeyValPair(expTimeMsUnix);
                    dbHashTableSize--; if(dbHashTableSize == 0) break;
                }
            }

            else
            {
                fs.Position--;
                valueType = br.ReadByte();
                if (valueType.Equals(Constant.ValueType.Str))
                {
                    new StringEncodingHandler(br).ReadKeyValPair();
                    dbHashTableSize--; if(dbHashTableSize == 0) break;
                }
            }
        }
        return opcode;
    }
}