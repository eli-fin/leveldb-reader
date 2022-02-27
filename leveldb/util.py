''' Some helper methods '''


def read_varint(stream, max_bytes):
    '''
    Read a variable sized integer from a stream
    see format here: https://developers.google.com/protocol-buffers/docs/encoding#varints
    (basically, msb marks if there's another byte)
    '''
    
    MASK = 0b10000000
    
    is_last_byte = False
    byte_index = 0
    retval = 0
    
    while not is_last_byte and byte_index < max_bytes:
        byte = stream.read(1)[0]
        
        is_last_byte = (byte & 0b10000000) == 0
        byte &= ~MASK
        
        retval += byte << (7*byte_index)
        byte_index += 1
    
    return retval


def read_varint32(stream):
    return read_varint(stream, 5) # int32 can take up to 5 bytes


def read_varint64(stream):
    return read_varint(stream, 10) # int64 can take up to 10 bytes


def read_sint32(stream):
    '''
    Read zigzag encoded varint
    see here: https://developers.google.com/protocol-buffers/docs/encoding#signed-ints
    '''
    n = read_varint32(stream)
    decoded = ((n & 0xFFFFFFFF) >> 1) ^ -(n & 1)
    return decoded


def read_safe(stream, n):
    ''' Read from stream and assert read size '''
    ret = stream.read(n)
    assert len(ret) == n, 'Unexpected EOF'
    return ret


def unmask_leveldb_crc32c(masked):
    '''
    Unmask leveldb crc32c, stored in masked format, see
    https://github.com/google/leveldb/blob/4fb146810cd265ffefa7f9905c016ae965ad36c8/util/crc32c.h#L35
    '''
    
    kMaskDelta = 0xA282EAD8
    UINT_MAX = (2**32)-1
    val = masked-kMaskDelta
    val = (val + UINT_MAX + 1) & UINT_MAX # simulate integer overflow
    val = ((val >> 17) | (val << 15)) & UINT_MAX
    return val


def bytes_left(byte_stream):
    ''' Get number of remaining bytes in io.BytesIO stream '''
    return len(byte_stream.getvalue()) - byte_stream.tell()


def int64_to_bytes_compact(val):
    ''' Int to bytes, but trim trailing zeroes '''
    buf = int.to_bytes(val, 8, 'little')
    trailing_zero_count = 0
    for i in range(8-1, 0, -1):
        if buf[i] == 0:
            trailing_zero_count += 1
        else:
            break
    ret = buf[:-trailing_zero_count]
    return ret
