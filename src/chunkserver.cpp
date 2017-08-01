#include "chunkserver.h"

template<typename Ret, typename ...Args>
void ChunkServer::bind(std::string rpcName, Ret(ChunkServer::*func)(Args...))
{
	srv.RPCBind(rpcName, std::function<Ret(Args...)>([this, func](Args ...args) -> Ret 
	{
		return (this->*func)(std::forward<Args>(args)...);
	}));
}

ChunkServer::ChunkServer(LightDS::Service &srv, const std::string &rootDir) : serve(srv),root(rootDir) 
{
	bind("RPCCreateChunk", &ChunkServer::RPCCreateChunk);
    bind("RPCReadChunk", &ChunkServer::RPCReadChunk);
    bind("RPCWriteChunk", &ChunkServer::RPCWriteChunk);
    bind("RPCAppendChunk", &ChunkServer::RPCAppendChunk);
    bind("RPCApplyMutation", &ChunkServer::RPCApplyMutation);
    bind("RPCSendCopy", &ChunkServer::RPCSendCopy);
    bind("RPCApplyCopy", &ChunkServer::RPCApplyCopy);
    bind("RPCGrantLease", &ChunkServer::RPCGrantLease);
    bind("RPCUpdateVersion", &ChunkServer::RPCUpdateVersion);
    bind("RPCPushData", &ChunkServer::RPCPushData);
}

void ChunkServer::Start()
{
	
	isrun = true;
}

void ChunkServer::Shutdown()
{
	isrun = false;
}

void ChunkServer::Heartbeat()
{
	std::vector<ChunkHandle> leaseExtensions;
	std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks;
	std::vector<ChunkHandle> failedChunks;
	for (int i = 0;i < chunkid.size();++i)
		if (iscrun[chunkid[i]]) 
		{
		    std::tuple<ChunkHandle, ChunkVersion> cv(chunkid[i],chunkversion[chunkid[i]]);
		    chunks.push_back(cv);
		}
		else failedChunks.push_back(chunkid[i]);	
	LightDS::RPCAddress ad = serve.ListService("master")[0];
	std::tuple<GFSError, std::vector<ChunkHandle>> tp = LightDS::RPCCall(ad,"RPCHeartbeat",isprimary,chunks,failedChunks);
	std::vector<ChunkHandle> garbage = tp.get<1>();
	for (int i = 0;i < garbage.size();++i)
	{
		std::string s = "";
	    for (int i = 0;i < root.size();++i)
	    {
		    s += root[i];
		    if (root[i] == '\\') s += '\\';
	    }
	    s = root +"\\"+ std::to_string(handle);
	    string ss = s + '@' + std::tostring(time(0)) + ".txt";
	    s = s + ".txt";
	    rename(s.c_str(), ss.c_str());
	}
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
GFSError
	ChunkServer::RPCCreateChunk(ChunkHandle handle)
{
	chunkid.push_back(handle);
	std::string s = "";
	for (int i = 0;i < root.size();++i)
	{
		s += root[i];
		if (root[i] == '\\') s += '\\';
	}
	char *fileName = s + "\\" + std::to_string(handle) + ".txt",*tag;  
    for(tag = fileName; *tag; ++tag)  
    {  
        if (*tag == '\\')  
        {  
            char buf[1000],path[1000];  
            strcpy(buf,fileName);  
            buf[strlen(fileName) - strlen(tag) + 1] = NULL;  
            strcpy(path,buf);  
            if (access(path,6) == -1)  
            {  
                mkdir(path);   
            }  
        }  
    }  
    freopen(fileName,"a",stdout);
    GFSError ge;
    return ge;
}

// RPCReadChunk is called by client, read chunk data and return
std::tuple<GFSError, std::string /*Data*/>
	ChunkServer::RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length)
{
	std::string s = "";
	for (int i = 0;i < root.size();++i)
	{
		s += root[i];
		if (root[i] == '\\') s += '\\';
	}
	s = root +"\\"+ std::to_string(handle) + ".txt";
	std::ifstream t(s);  
    std::stringstream buffer;  
    buffer << t.rdbuf();  
    std::string contents(buffer.str());
    std::string content = "";
    for (int i = 0;i < length;++i) content += contents[i + offset];
    GFSError ge;
    std::tuple<GFSError, std::string> tp(ge,content);
    return tp;
}
    	
// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
GFSError
	ChunkServer::RPCWriteChunk(ChunkHandle handle, std::uint64_t dataID, std::uint64_t offset, std::vector<std::string> secondaries)
{ 
    ++serial[handle];
	std::string s = "";
	for (int i = 0;i < root.size();++i)
	{
		s += root[i];
		if (root[i] == '\\') s += '\\';
	}
	string fileName = s + "\\" + std::to_string(handle) + ".txt";  
	FILE *stream;
	stream = fopen(fileName,"a");
	s = "";
	s += std::to_string(offset);
	s += 'L';
    fseek(stream,s,SEEK_SET);
    fprintf(stream,datamap[dataID]);
    //to do rpccall;
    ++serial[handle];
    for (int i = 0;i < secondaries.size();++i)
	{
		LightDS::RPCAddress ad = LightDS::from_string(secondaries[i]);
		LightDS::RPCCall(ad,"RPCApplyMutation",handle,serial[handle],MutationAppend,dataID,offset,datamap[dataID].size());
    }
    GFSError ge;
    return ge;
}
// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
std::tuple<GFSError, std::uint64_t /*offset*/>
	ChunkServer::RPCAppendChunk(ChunkHandle handle, std::uint64_t dataID, std::vector<std::string> secondaries)
{
	std::uint64_t off;
	std::string s = "";
	for (int i = 0;i < root.size();++i) 
	{
		s += root[i];
		if (root[i] == '\\') s += '\\';
	}
	string fileName = s + "\\" + std::to_string(handle) + ".txt";  
	FILE* file = fopen(fileName, "rb");
    if (file)
    {
        off = filelength(fileno(file));
        fclose(file);
    }
    freopen(fileName,"a",stdout);
    if (off + datamap[dataID].size() <= 64 * 1024 * 1024) 
    {
    	std::cout<<datamap[dataID];
    	//to do rpccall
    	++serial[handle];
    	for (int i = 0;i < secondaries.size();++i)
		{
			LightDS::RPCAddress ad = LightDS::from_string(secondaries[i]);
			LightDS::RPCCall(ad,"RPCApplyMutation",handle,serial[handle],MutationAppend,dataID,off,datamap[dataID].size());
    	}
    }
    else
    {
    	for (int j = 0;j < datamap[dataID].size() && j + off <= 64 * 1024 * 1024;++j) std::cout<<datamap[dataID][j];
    	++serial[handle];
    	for (int i = 0;i < secondaries.size();++i)
		{
			LightDS::RPCAddress ad = LightDS::from_string(secondaries[i]);
			LightDS::RPCCall(ad,"RPCApplyMutation",handle,serial[handle],MutationPad,dataID,off,datamap[dataID].size());
    	}
    }
    GFSError ge;
    ge.errCode = AppendPad;
    std::tuple<GFSError,std::uint64_t> tp(ge,off);
    return tp;
}

// RPCApplyMutation is called by primary to apply mutations
GFSError
	ChunkServer::RPCApplyMutation(ChunkHandle handle, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset, std::uint64_t length)
{
	GFSError ge;
	if (serialNo != serial[handle] + 1) return ge;
	++serial[handle];
	if (type == MutationWrite) 
	{
		std::string s = "";
	    for (int i = 0;i < root.size();++i)
    	{
			s += root[i];
			if (root[i] == '\\') s += '\\';
		}
		string fileName = s + "\\" + std::to_string(handle) + ".txt";  
		FILE *stream;
		stream = fopen(fileName,"a");
		s = "";
		s += std::to_string(offset);
		s += 'L';
   		fseek(stream,s,SEEK_SET);
  	 	fprintf(stream,datamap[dataID]);
  	}
  	if (type == MutationAppend)
  	{
  		std::string s = "";
		for (int i = 0;i < root.size();++i)
		{
			s += root[i];
			if (root[i] == '\\') s += '\\';
		}
		string fileName = s + "\\" + std::to_string(handle) + ".txt";  
		freopen(fileName,"a",stdout);
 	    std::cout<<datamap[dataID];
    }
    if (type == MutationPad)
  	{
  		std::string s = "";
		for (int i = 0;i < root.size();++i)
		{
			s += root[i];
			if (root[i] == '\\') s += '\\';
		}
		string fileName = s + "\\" + std::to_string(handle) + ".txt";  
		freopen(fileName,"a",stdout);
 	    for (int j = 0;j < datamap[dataID].size() && j + offset <= 64 * 1024 * 1024;++j) std::cout<<datamap[dataID][j];
    }
	GFEError ge;
	return ge;		
}
// RPCSendCopy is called by master, send the whole copy to given address
GFSError
	ChunkServer::RPCSendCopy(ChunkHandle handle, std::string addr)
{
	std::string s = "";
	for (int i = 0;i < root.size();++i)
	{
		s += root[i];
		if (root[i] == '\\') s += '\\';
	}
	s = root +"\\"+ std::to_string(handle) + ".txt";
	std::ifstream t(s);  
    std::stringstream buffer;  
    buffer << t.rdbuf();  
    std::string contents(buffer.str());
	freopen(addr,"a",stdout);
	std::cout<<contents;
	GFSError ge;
	return ge;
}
	
// RPCApplyCopy is called by another replica
// rewrite the local version to given copy data
GFSError
	ChunkServer::RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo)
{
	chunkversion[handle] = version;
	std::string s = "";
	for (int i = 0;i < root.size();++i)
	{
		s += root[i];
		if (root[i] == '\\') s += '\\';
	}
	s = root +"\\"+ std::to_string(handle) + ".txt";
	freopen(s,"w",stdout);
	std::cout<<data;
	serial[handle] = serialNo;
	GFSError ge;
	return ge;
}

// RPCGrantLease is called by master
// mark the chunkserver as primary
GFSError
	ChunkServer::RPCGrantLease(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>> chunks)
{
	for (int i = 0;i < chunks.size();++i)
	{
		ChunkHandle handle = chunks[i].get<0>();
		chunkversion[handle] = chunks[i].get<1>();
		primarytime[handle] = chunks[i].get<2>() + time(0);
		isprimary.push_back(handle);
	}		
}

// RPCUpdateVersion is called by master
// update the given chunks' version to 'newVersion'
GFSError
	ChunkServer::RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion)
{
	chunkversion[handle] = newVersion;
	GFSError ge;
	return ge;
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer.
// This should be replaced by a chain forwarding.
GFSError
	ChunkServer::RPCPushData(std::uint64_t dataID, std::string data)
{
	datamap[dataID] = data;
	GFSError ge;
	return ge;
}




