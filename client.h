#ifndef GFS_CLIENT_H
#define GFS_CLIENT_H

#include "commons.h"
#include "user.hpp"
#include <string>
#include "service.hpp"
#include <map>
//#include <io.h>  
//#include <direct.h>
#include <string>
#include <fstream>
#include <ctime>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <unistd.h>  
#include <dirent.h>  
#include <sys/stat.h>
#include <sys/types.h> 　
#include "commons.h"
#include <string>
#include "service.hpp"
#include <map>
#include "master.h"
#include <iostream>       
#include <sys/stat.h> 　
#include <sys/types.h> 　
#include <cstdio>  
#include <cstdio> 
#include <fstream>  
#include <sstream>  
#include <cstdlib>
class Client
{
private:
	LightDS::User &serve;
public:
	Client(LightDS::User &srv);

public:
	// Create creates a new file on the specific path on GFS.
	GFSError 
		Create(const std::string &path);

	// Mkdir creates a new directory on GFS.
	GFSError 
		Mkdir(const std::string &path);

	// List lists files and directories in specific directory on GFS.
	std::tuple<GFSError, std::vector<std::string> /*filenames*/>
		List(const std::string &path);

	// Read reads the file at specific offset.
	// It reads up to data.size() bytes form the File.
	// It return the number of bytes, and an error if any.
	std::tuple<GFSError, size_t /*byteOfRead*/>
		Read(const std::string &path, std::uint64_t offset, std::vector<char> &data);

	// Write writes data to the file at specific offset.
	GFSError 
		Write(const std::string &path, std::uint64_t offset, const std::vector<char> &data);

	// Append appends data to the file. Offset of the beginning of appended data is returned.
	std::tuple<GFSError, std::uint64_t /*offset*/>
		Append(const std::string &path, const std::vector<char> &data);

public:
	// GetChunkHandle returns the chunk handle of (path, index).
	// If the chunk doesn't exist, create one.
	std::tuple<GFSError, ChunkHandle>
		GetChunkHandle(const std::string &path, std::uint64_t index);

	// ReadChunk reads data from the chunk at specific offset.
	// data.size()+offset  should be within chunk size.
	std::tuple<GFSError, size_t /*byteOfRead*/>
		ReadChunk(ChunkHandle handle, std::uint64_t offset, std::vector<char> &data);

	// WriteChunk writes data to the chunk at specific offset.
	// data.size()+offset should be within chunk size.
	GFSError
		WriteChunk(ChunkHandle handle, std::uint64_t offset, const std::vector<char> &data);

	// AppendChunk appends data to a chunk.
	// Chunk offset of the start of data will be returned if success.
	// data.size() should be within max append size.
	std::tuple<GFSError, std::uint64_t /*offset*/>
		AppendChunk(ChunkHandle handle, const std::vector<char> &data);


};

#endif
