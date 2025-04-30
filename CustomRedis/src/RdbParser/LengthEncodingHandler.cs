namespace CustomRedis.RdbParser;

public class LengthEncodingHandler
{
    private readonly BinaryReader _br;

    public LengthEncodingHandler(BinaryReader br) => _br = br;
    
    public int Decode(int sz)
    {
        //check the 2 MSBs of the byte

        //00 => The next 6 bits represent the length
        //01 => The next 6 bits + 1 byte = 14 bits represent the length
        //10 => Discard the remaining 6 bits. The next 4 bytes represent the length
        //11 => 8, 16 or 32 bit int string 
        int auxKeyLen = 0;
        if (sz <= 0x3F)
        {
            //2 MSBs = 00 => The next 6 bits represent the length
            auxKeyLen = sz;
        }
        else if (sz <= 0x7F)
        {
            //2 MSBs = 01 => Read one additional byte. The combined 14 bits represent the length
            auxKeyLen = (sz & (~0x40)) + _br.ReadByte();
        }
        else if (sz <= 0xBF)
        {
            //2 MSBs = 10 => Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            for (int i = 4; i>0; i--) 
                auxKeyLen += (_br.ReadByte() << (i-1)*8);
        }
       
        // We've got integers as strings e.g "1234" , "123456"
        else if (sz == 0xC0)
        {
            //indicates that an 8 bit integer follows => read the next byte
            auxKeyLen = sz;

        }
        else if (sz == 0xC1)
        {
            //indicates that a 16-bit integer follows => read next 2 bytes in little endian (right -> left)
            auxKeyLen = sz;
        }
        else if (sz == 0xC2)
        {
            //indicates that a 32-bit integer follows => read next 4 bytes in little endian (r -> l)
            auxKeyLen = sz;
        }

        return auxKeyLen;
    }
}