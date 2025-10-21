import struct
import sys


class LocalBufferFile:
    def __init__(self, db_file):
        self.db_file = db_file
        self.magic_number = 0
        self.file_id = 0
        self.version = 0
        self.block_size = 0
        self.first_free_block = 0
        self.user_param_count = 0
        self.user_params = {}
        self._read_header()
        self._read_user_params()

# //
# 	// The first block is reserved for use by the BlockFile header.
# 	// The format of this header is determined by the Field #3.
# 	//
# 	// FILE FORMAT - VERSION 1
# 	//
# 	// A file header is contained within the first buffer at offset 0 within the file.
# 	// This header contains the following fields:
# 	//
# 	// 1. Magic number (8,long)
# 	// 2. File ID (8,long)
# 	// 3. Block file header format version (4,int) = 1
# 	// 5. Block size (4,int)
# 	// 6. First free block index (4,int)
# 	// 7. User-defined parameter count (4,int)
# 	// 8. User-defined parameters stored as:
# 	//       * Parm name length (4,int)
# 	//       * Parm name (?,char[])
# 	//       * Parm value (4,int)
# 	//    Parameter space is limited by buffer size.
# 	//
# 	// In Version 1 - the first available user buffer immediately follows the file
# 	// header block (i.e., buffer index 0 corresponds to block index 1).
# 	//
# 	// Each user block has the following prefix data:
# 	// 1. Flags (1,byte)
# 	//   * Bit 0: 1=empty block, 0=not empty
# 	// 2. DataBuffer ID (4,int) if not empty, or next empty buffer index if empty
# 	//        (-1 indicates last empty buffer)
# 	//
    def _read_header(self):
        with open(self.db_file, 'rb') as f:
            header = f.read(32)  # Read the first 32 bytes for the header
            if len(header) < 32:
                raise ValueError(
                    "File is too short to contain a valid header.")
            # Unpack the header fields (assuming they are in the correct format)
            header = struct.unpack('>qqiiii', header[:32])
            self.magic_number = header[0]
            self.file_id = header[1]
            self.version = header[2]
            self.block_size = header[3]
            self.first_free_block = header[4]
            self.user_param_count = header[5]

    def _read_user_params(self):
        with open(self.db_file, 'rb') as f:
            f.seek(32)  # Move to the position after the header
            for _ in range(self.user_param_count):
                name_length_data = f.read(4)
                if len(name_length_data) < 4:
                    raise ValueError(
                        "Unexpected end of file while reading parameter name length.")
                name_length = struct.unpack('>i', name_length_data)[0]

                name_data = f.read(name_length)
                if len(name_data) < name_length:
                    raise ValueError(
                        "Unexpected end of file while reading parameter name.")
                name = name_data.decode('utf-8')

                value_data = f.read(4)
                if len(value_data) < 4:
                    raise ValueError(
                        "Unexpected end of file while reading parameter value.")
                value = struct.unpack('>i', value_data)[0]
                self.user_params[name] = value

    def info(self):
        print(f"Magic Number: {self.magic_number:x}")
        print(f"File ID: {self.file_id}")
        print(f"Version: {self.version}")
        print(f"Block Size: {self.block_size}")
        print(f"First Free Block: {self.first_free_block}")
        print(f"User Parameter Count: {self.user_param_count}")
        if self.user_param_count > 0:
            print("User Parameters:")
            for name, value in self.user_params.items():
                print(f"  {name}: {value}")
        flags, block_data, block_id = self.get_buffer_block(0)
        while True:
            block_id += 1
            try:
                flags, block_data, block_id = self.get_buffer_block(block_id)
            except ValueError:
                break
        print("Number of blocks read:", block_id)

    def get_buffer_block(self, block_index):
        with open(self.db_file, 'rb') as f:
            offset = (block_index + 1) * self.block_size  # +1 for header block
            f.seek(offset)
            flags = f.read(1)
            if flags == b'':
                raise ValueError("Block index out of range.")
            block_id = struct.unpack(">i", f.read(4))[0]
            # Remaining data in the block
            block_data = f.read(self.block_size-5)
            if len(block_data) < self.block_size - 5:
                raise ValueError("Block index out of range.")
            return flags, block_data, block_id


if __name__ == "__main__":
    db_file = sys.argv[1] if len(sys.argv) > 1 else None
    if not db_file:
        print("Usage: python jiddra.py <db_file>")
        sys.exit(1)
    lbf = LocalBufferFile(db_file)
    lbf.info()
