#ifndef GFS_CHUNKSERVER_H
#define GFS_CHUNKSERVER_H

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

class ChunkServer
{
private:
	template<typename Ret, typename ...Args> 
	void bind(std::string rpcName, Ret(ChunkServer::*func)(Args...)); 
	
	std::map<std::uint64_t,std::string> datamap;
	bool isrun;
	std::vector<ChunkHandle> chunkid;
	//std::map<ChunkHandle,std::string> chunkroot; 
	std::map<ChunkHandle,int> iscrun;//is chunk run
	std::map<ChunkHandle,std::uint64_t> serial;
	std::map<ChunkHandle,ChunkVersion> chunkversion;
	std::map<ChunkHandle,uint64_t> primarytime;
	std::map<ChunkHandle,bool> isprimary;
	std::vector<ChunkHandle> primaries;
	std::string root;
	LightDS::Service &serve;
public:
	enum MutationType : std::uint32_t
	{
		MutationWrite,
		MutationAppend,
		MutationPad
	};
public:
	ChunkServer(LightDS::Service &srv, const std::string &rootDir);
	void Start();
	void Shutdown();

public:
	void Heartbeat();

	// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
	GFSError
		RPCCreateChunk(ChunkHandle handle);

	// RPCReadChunk is called by client, read chunk data and return
	std::tuple<GFSError, std::string /*Data*/>
		RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length);

	// RPCWriteChunk is called by client
	// applies chunk write to itself (primary) and asks secondaries to do the same.
	GFSError
		RPCWriteChunk(ChunkHandle handle, std::uint64_t dataID, std::uint64_t offset, std::vector<std::string> secondaries);

	// RPCAppendChunk is called by client to apply atomic record append.
	// The length of data should be within max append size.
	// If the chunk size after appending the data will excceed the limit,
	// pad current chunk and ask the client to retry on the next chunk.
	std::tuple<GFSError, std::uint64_t /*offset*/>
		RPCAppendChunk(ChunkHandle handle, std::uint64_t dataID, std::vector<std::string> secondaries);

	// RPCApplyMutation is called by primary to apply mutations
	GFSError
		RPCApplyMutation(ChunkHandle handle, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset, std::uint64_t length);

	// RPCSendCopy is called by master, send the whole copy to given address
	GFSError
		RPCSendCopy(ChunkHandle handle, std::string addr);

	// RPCApplyCopy is called by another replica
	// rewrite the local version to given copy data
	GFSError
		RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo);

	// RPCGrantLease is called by master
	// mark the chunkserver as primary
	GFSError
		RPCGrantLease(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>> chunks);

	// RPCUpdateVersion is called by master
	// update the given chunks' version to 'newVersion'
	GFSError
		RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion);

	// RPCPushDataAndForward is called by client.
	// It saves client pushed data to memory buffer.
	// This should be replaced by a chain forwarding.
	GFSError
		RPCPushData(std::uint64_t dataID, std::string data);


};
MSGPACK_ADD_ENUM(ChunkServer::MutationType);

#endif
