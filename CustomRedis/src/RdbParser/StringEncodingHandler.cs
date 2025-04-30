using CustomRedis.Utils;

namespace CustomRedis.RdbParser;

public class StringEncodingHandler
{
    private readonly BinaryReader _br;

    public StringEncodingHandler(BinaryReader br) => _br = br;
    public string Decode(int numBytesToRead)
    {
        string value = "";
        if (numBytesToRead == 0xC0)
        {
            //indicates that an 8 bit integer follows => read the next byte
            value = _br.ReadByte().ToString();
        }
        else if (numBytesToRead == 0xC1)
        {
            //indicates that a 16-bit integer follows => read next 2 bytes in little endian (r -> l)
            //e.g C1 39 30
            // In this example, the string is "12345"  because 0x3039 = 12345 in decimal. So string "12345"
            value = ReadNextBytesInLittleEndian(count: 2);
            
        }
        else if (numBytesToRead == 0xC2)
        {
            //indicates that a 32-bit integer follows => read next 4 bytes in little endian (r -> l)
            value = ReadNextBytesInLittleEndian(count: 4);
        }
        else
        {
            //Length prefixed string => Easy case
            value = new string(_br.ReadChars(numBytesToRead));
        }
        return value;
    }

    private string ReadNextBytesInLittleEndian(int count)
    {
        // 0x39 30
        byte[] bytes = _br.ReadBytes(count);
        int val = 0;
        for (int i = 0; i < bytes.Length; i++)
        {
            val += bytes[i] << (8*i);
        }
        return val.ToString();    
    }
    
    public void ReadKeyValPair(ulong? expTimeMsUnix = null)
    {
        LengthEncodingHandler lh = new LengthEncodingHandler(_br);
        StringEncodingHandler sh = this;
        int keyLen = lh.Decode(_br.ReadByte());
        string key = sh.Decode(numBytesToRead: keyLen);
                            
        int valueLen = lh.Decode(_br.ReadByte());
        string value = sh.Decode(numBytesToRead: valueLen);
        
        Console.WriteLine($"{key}: {value} {expTimeMsUnix}");
        KeyValueStore.Set(key, value, expTimeMsUnix);
    }
}