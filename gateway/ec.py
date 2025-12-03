import zfec
from typing import List
import math

# Configuration for EC
# 4 data shards + 2 parity shards = 6 total shards
# We can tolerate loss of any 2 shards.
K = 4  # Minimum required shards
M = 6  # Total shards (K + parity)

def encode_data(data: bytes) -> List[bytes]:
    """
    Encodes data into M shards (K data + parity).
    Returns a list of M bytes objects, where each is a shard.
    """
    # zfec requires us to split data into exactly K chunks of equal size
    # Pad if necessary
    chunk_size = math.ceil(len(data) / K)
    padded_size = chunk_size * K
    
    # Pad data to be evenly divisible by K
    padded_data = data + b'\x00' * (padded_size - len(data))
    
    # Split into K blocks
    blocks = []
    for i in range(K):
        start = i * chunk_size
        end = start + chunk_size
        blocks.append(padded_data[start:end])
    
    # Encode
    encoder = zfec.Encoder(K, M)
    shards = encoder.encode(blocks)
    
    return shards

def decode_data(shards: List[bytes], shard_nums: List[int], original_size: int) -> bytes:
    """
    Decodes data from a subset of shards.
    shards: list of shard data (bytes)
    shard_nums: list of indices corresponding to the shards (e.g., [0, 2, 4, 5])
    original_size: needed to truncate padding
    """
    if len(shards) < K:
        raise ValueError(f"Need at least {K} shards to recover data")
    
    decoder = zfec.Decoder(K, M)
    recovered_blocks = decoder.decode(shards[:K], shard_nums[:K])
    
    # Join blocks and truncate to original size
    full_data = b"".join(recovered_blocks)
    return full_data[:original_size]
