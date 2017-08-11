#include "chunkserver.h"
#include <fstream>


ChunkServer::ChunkServer(LightDS::Service &srv, const std::string &rootDir) : serve(srv),root(rootDir) 

{

	serve.RPCBind<GFSError(ChunkHandle)>("RPCCreateChunk", std::bind(&ChunkServer::RPCCreateChunk, this, std::placeholders::_1));

	serve.RPCBind<std::tuple<GFSError, std::string>(ChunkHandle, std::uint64_t , std::uint64_t )>("RPCReadChunk", std::bind(&ChunkServer::RPCReadChunk, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));

	serve.RPCBind<GFSError(ChunkHandle,std::uint64_t,std::uint64_t,std::vector<std::string>)>("RPCWriteChunk", std::bind(&ChunkServer::RPCWriteChunk, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3,std::placeholders::_4));

	serve.RPCBind<std::tuple<GFSError, std::uint64_t>(ChunkHandle , std::uint64_t , std::vector<std::string> )>("RPCAppendChunkChunk", std::bind(&ChunkServer::RPCAppendChunk, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));

	serve.RPCBind<GFSError(ChunkHandle, std::uint64_t, MutationType, std::uint64_t , std::uint64_t , std::uint64_t )>("RPCApplyMutation", std::bind(&ChunkServer::RPCApplyMutation, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3,std::placeholders::_4,std::placeholders::_5,std::placeholders::_6));

	serve.RPCBind<GFSError(ChunkHandle, std::string)>("RPCSendCopy", std::bind(&ChunkServer::RPCSendCopy, this, std::placeholders::_1,std::placeholders::_2));

	serve.RPCBind<GFSError(ChunkHandle, ChunkVersion, std::string , std::uint64_t )>("RPCApplyCopy", std::bind(&ChunkServer::RPCApplyCopy, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3,std::placeholders::_4));

	serve.RPCBind<GFSError(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>> )>("RPCGrantLease", std::bind(&ChunkServer::RPCGrantLease, this, std::placeholders::_1));

	serve.RPCBind<GFSError(ChunkHandle, ChunkVersion)>("RPCUpdateVersion", std::bind(&ChunkServer::RPCUpdateVersion, this, std::placeholders::_1,std::placeholders::_2));

	serve.RPCBind<GFSError(std::uint64_t dataID, std::string data)>("RPCPushData", std::bind(&ChunkServer::RPCPushData ,this, std::placeholders::_1,std::placeholders::_2));

}



void ChunkServer::Start()

{	

	isrun = true;

	chunkid.clear();

	serial.clear();

	chunkversion.clear();

	datamap.clear();

	primarytime.clear();

	isprimary.clear();

	std::string s = root;

	/*std::string ss = s + "/chunkid.txt";

	freopen(ss.c_str(),"r",stdin);

	ChunkHandle handle;

	while(std::cin>>handle) chunkid.push_back(handle);

	ss = s + "/serial.txt";

	freopen(ss.c_str(),"r",stdin);

	std::uint64_t x;

	for (int i = 0;i < chunkid.size();++i)

	{

		std::cin>>x;

		serial[chunkid[i]] = x;

	}

	ss = s + "/chunkversion.txt";

	freopen(ss.c_str(),"r",stdin);

	ChunkVersion y;

	for (int i = 0;i < chunkid.size();++i)

	{

		std::cin>>y;

		chunkversion[chunkid[i]] = y;

	}

	ss = s + "/dataID.txt";

	freopen(ss.c_str(),"r",stdin);

	std::vector<std::uint64_t> dataid;

	while (std::cin>>handle) dataid.push_back(handle);

	ss = s + "/datamap.txt";

	freopen(ss.c_str(),"r",stdin);

	for (int i = 0;i < dataid.size();++i) std::cin>>datamap[dataid[i]];

	ss = s + "/primarytime.txt";

	freopen(ss.c_str(),"r",stdin);

	for (int i = 0;i < chunkid.size();++i)

	{

		std::cin>>x;

		primarytime[chunkid[i]] = x;

	}

	ss = s + "/isprimary.txt";

	freopen(ss.c_str(),"r",stdin);

	for (int i = 0;i < chunkid.size();++i)

	{

		std::cin>>x;

		isprimary[chunkid[i]] = x;

	}*/

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

		if ( true ) 

		{

		    std::tuple<ChunkHandle, ChunkVersion> cv(chunkid[i],chunkversion[chunkid[i]]);

		    chunks.push_back(cv);

		}

		else failedChunks.push_back(chunkid[i]);	

	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];

	std::tuple<GFSError, std::vector<ChunkHandle>> tp = serve.RPCCall(ad,"RPCHeartbeat",primaries,chunks,failedChunks).get().as<std::tuple<GFSError, std::vector<ChunkHandle>>>();

	std::vector<ChunkHandle> garbage = std::get<1>(tp);

	for (int i = 0;i < garbage.size();++i)

	{

		std::string s = root;

	    s += '/'+ std::to_string(garbage[i]);

	    std::string ss = s + '@' + std::to_string(time(0)) + ".txt";

	    s = s + ".txt";

	    rename(s.c_str(), ss.c_str());

	}

}



// RPCCreateChunk is called by master to create a new chunk given the chunk handle.

GFSError

	ChunkServer::RPCCreateChunk(ChunkHandle handle)

{
        cout<<0<<endl;
	chunkid.push_back(handle);

	std::string s = root;

	std::string ts = s + '/' + std::to_string(handle) + ".txt";
cout<<1<<endl;
        const	char *fileName = ts.c_str(),*tag;  

    //      mkdir(root,S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);   
cout<<2<<fileName<<endl;
    std::ofstream outfile(fileName);
cout<<3<<endl;
    GFSError ge;

    ge.errCode = GFSErrorCode::OK;
cout<<4<<endl;
    std::string sav = s + "/chunkid.txt";

    std::ofstream outfile2(sav.c_str());
cout<<5<<endl;
    std::cout<<handle<<std::endl;

    sav = s + "/serial.txt";

    std::ofstream outfile3(sav.c_str());

    std::cout<<serial[handle]<<std::endl;

    sav = s + "/chunkversion.txt";

    std::ofstream outfile4(sav.c_str());

    std::cout<<chunkversion[handle]<<std::endl;

    return ge;

}



// RPCReadChunk is called by client, read chunk data and return

std::tuple<GFSError, std::string /*Data*/>

	ChunkServer::RPCReadChunk(ChunkHandle handle, std::uint64_t offset, std::uint64_t length)

{

	std::string s = root;

	s += '/'+ std::to_string(handle) + ".txt";

	std::ifstream t(s);  

    std::stringstream buffer;  

    buffer << t.rdbuf();  

    std::string contents(buffer.str());

    std::string content = "";

	if (length >= 0)

    for (int i = 0;i < length && i + offset < contents.size();++i) content += contents[i + offset];

    else content = contents;

    GFSError ge;

    ge.errCode = GFSErrorCode::OK;

    std::tuple<GFSError, std::string> tp(ge,content);

    return tp;

}

    	

// RPCWriteChunk is called by client

// applies chunk write to itself (primary) and asks secondaries to do the same.

GFSError

	ChunkServer::RPCWriteChunk(ChunkHandle handle, std::uint64_t dataID, std::uint64_t offset, std::vector<std::string> secondaries)

{ 
    
    if (primarytime[handle] < time(0) || isprimary[handle] == 0)

    {

    	GFSError ge;

    	ge.errCode = GFSErrorCode::AppendPad;

    	return ge;

    }

	std::string s = root;

	std::string ss = s;

	std::string fileName = s + '/' + std::to_string(handle) + ".txt";  

	FILE *stream;

	stream = fopen(fileName.c_str(),"a");

	s = "";

	s += std::to_string(offset);

	s += 'L';
cout<<"wtl"<<endl;

    fseek(stream,offset,SEEK_SET);

    fprintf(stream,datamap[dataID].c_str());

    //to do rpccall;

    ++serial[handle];

    for (int i = 0;i < secondaries.size();++i)

	{
cout<<"???"<<endl;

		LightDS::Service::RPCAddress ad = LightDS::Service::RPCAddress::from_string(secondaries[i]);

		serve.RPCCall(ad,"RPCApplyMutation",handle,serial[handle],MutationWrite,dataID,offset,datamap[dataID].size());

    }

    GFSError ge;

    ge.errCode = GFSErrorCode::OK;

    std::string sav = ss + "/serial.txt";

 FILE * fp1 = fopen(sav.c_str(),"w");

	for (int i = 0;i < chunkid.size();++i) fprintf(fp1,"%lld\n",serial[chunkid[i]]);
        fclose(fp1);

    return ge;

}



// RPCAppendChunk is called by client to apply atomic record append.

// The length of data should be within max append size.

// If the chunk size after appending the data will excceed the limit,

// pad current chunk and ask the client to retry on the next chunk.

std::tuple<GFSError, std::uint64_t /*offset*/>

	ChunkServer::RPCAppendChunk(ChunkHandle handle, std::uint64_t dataID, std::vector<std::string> secondaries)

{

	if (primarytime[handle] < time(0) || isprimary[handle] == 0)

    {

    	GFSError ge;

    	ge.errCode = GFSErrorCode::OK;

		std::tuple<GFSError, std::uint64_t> tp(ge, 0);

    	return tp;

    }

	std::uint64_t off;

	std::string s = root;

	std::string fileName = s + '/' + std::to_string(handle) + ".txt";  

	FILE* file = fopen(fileName.c_str(), "rb");

    if (file)

    {
 	fseek(file, 0L, SEEK_END);  
        off = ftell(file); 
    }

    freopen(fileName.c_str(),"a",stdout);

    if (off + datamap[dataID].size() <= 64 * 1024 * 1024) 

    {

    	std::cout<<datamap[dataID];

    	//to do rpccall

    	++serial[handle];

    	for (int i = 0;i < secondaries.size();++i)

		{

			LightDS::Service::RPCAddress ad = LightDS::Service::RPCAddress::from_string(secondaries[i]);

			serve.RPCCall(ad,"RPCApplyMutation",handle,serial[handle],MutationAppend,dataID,off,datamap[dataID].size());

    	}

    	GFSError ge;

        ge.errCode = GFSErrorCode::OK;

    	std::tuple<GFSError,std::uint64_t> tp(ge,off);

    	std::string sav = s + "/serial.txt";

    	freopen(sav.c_str(),"w",stdout);

    	for (int i = 0;i < chunkid.size();++i) std::cout<<serial[chunkid[i]]<<std::endl;

    	return tp;

    }

    else

    {

    	for (int j = 0;j < datamap[dataID].size() && j + off <= 64 * 1024 * 1024;++j) std::cout<<datamap[dataID][j];

    	++serial[handle];

    	for (int i = 0;i < secondaries.size();++i)

		{

			LightDS::Service::RPCAddress ad = LightDS::Service::RPCAddress::from_string(secondaries[i]);

			serve.RPCCall(ad,"RPCApplyMutation",handle,serial[handle],MutationPad,dataID,off,datamap[dataID].size());

    	}

    	GFSError ge;

    	ge.errCode = GFSErrorCode::NoLease;

   	 	std::tuple<GFSError,std::uint64_t> tp(ge,off);

   	    std::string sav = s + "/serial.txt";

   		freopen(sav.c_str(),"w",stdout);

    	for (int i = 0;i < chunkid.size();++i) std::cout<<serial[chunkid[i]]<<std::endl;

    	return tp;

    }

}



// RPCApplyMutation is called by primary to apply mutations

GFSError

	ChunkServer::RPCApplyMutation(ChunkHandle handle, std::uint64_t serialNo, MutationType type, std::uint64_t dataID, std::uint64_t offset, std::uint64_t length)

{

	GFSError ge;

	ge.errCode = GFSErrorCode::WrongSerial;

	if (serialNo != serial[handle] + 1) return ge;

	++serial[handle];

	std::string ss;

	if (type == MutationWrite) 

	{

		std::string s = root;

		ss = s;

		std::string fileName = s + '/' + std::to_string(handle) + ".txt";  

		FILE *stream;

		stream = fopen(fileName.c_str(),"a");

		s = "";

		s += std::to_string(offset);

		s += 'L';

   		fseek(stream,offset,SEEK_SET);

  	 	fprintf(stream,datamap[dataID].c_str());

  	}

  	if (type == MutationAppend)

  	{

  		std::string s = root;

		ss = s;

		std::string fileName = s + '/' + std::to_string(handle) + ".txt";  

		freopen(fileName.c_str(),"a",stdout);

 	    std::cout<<datamap[dataID];

    }

    if (type == MutationPad)

  	{

  		std::string s = root;

		ss = s;

		std::string fileName = s + '/' + std::to_string(handle) + ".txt";  

		freopen(fileName.c_str(),"a",stdout);

 	    for (int j = 0;j < datamap[dataID].size() && j + offset <= 64 * 1024 * 1024;++j) std::cout<<datamap[dataID][j];

    }

	ge.errCode = GFSErrorCode::OK;

	std::string sav = ss + "/serial.txt";

    freopen(sav.c_str(),"w",stdout);

    for (int i = 0;i < chunkid.size();++i) std::cout<<serial[chunkid[i]]<<std::endl;

	return ge;		

}



// RPCSendCopy is called by master, send the whole copy to given address

GFSError

	ChunkServer::RPCSendCopy(ChunkHandle handle, std::string addr)

{

	std::string s = root;

	s += '/'+ std::to_string(handle) + ".txt";

	std::ifstream t(s);  

    std::stringstream buffer;  

    buffer << t.rdbuf();  

    std::string contents(buffer.str());

	freopen(addr.c_str(),"w",stdout);

	std::cout<<contents;

	GFSError ge;

	ge.errCode = GFSErrorCode::OK;

	return ge;

}

	

// RPCApplyCopy is called by another replica

// rewrite the local version to given copy data

GFSError

	ChunkServer::RPCApplyCopy(ChunkHandle handle, ChunkVersion version, std::string data, std::uint64_t serialNo)

{

	std::string s = root;

	std::string ss = s;

	s += '/' + std::to_string(handle) + ".txt";

	freopen(s.c_str(),"w",stdout);

	std::cout<<data;

	serial[handle] = serialNo;

	chunkversion[handle] = version;

	GFSError ge;

	ge.errCode = GFSErrorCode::OK;

	std::string sav = s + "/serial.txt";

    freopen(sav.c_str(),"w",stdout);

    for (int i = 0;i < chunkid.size();++i) std::cout<<serial[chunkid[i]]<<std::endl;

    sav = ss + "/chunkversion.txt";

    freopen(sav.c_str(),"w",stdout);

    for (int i = 0;i < chunkid.size();++i) std::cout<<chunkversion[chunkid[i]]<<std::endl;

	return ge;

}



// RPCGrantLease is called by master

// mark the chunkserver as primary

GFSError

	ChunkServer::RPCGrantLease(std::vector<std::tuple<ChunkHandle /*handle*/, ChunkVersion /*newVersion*/, std::uint64_t /*expire timestamp*/>> chunks)

{
cout<<">"<<1<<endl;

	for (int i = 0;i < chunks.size();++i)

	{

		ChunkHandle handle = std::get<0>(chunks[i]);

		chunkversion[handle] = std::get<1>(chunks[i]);

		primarytime[handle] = std::get<2>(chunks[i]);

		primaries.push_back(handle);

		isprimary[handle] = 1;

	}

	std::string s = root;
cout<<">"<<2<<endl;
	std::string ss = s + "/primarytime.txt";

        FILE * fp1 = fopen(ss.c_str(),"w");
cout<<">"<<3<<endl;
	for (int i = 0;i < chunkid.size();++i) fprintf(fp1,"%lld\n",primarytime[chunkid[i]]);
        fclose(fp1);
	ss = s + "/isprimary.txt";

	
        FILE * fp2 = fopen(ss.c_str(),"w");
cout<<">"<<4<<endl;
	for (int i = 0;i < chunkid.size();++i) fprintf(fp2,"%lld\n",isprimary[chunkid[i]]);
 	fclose(fp2);
	GFSError ge;
cout<<">"<<5<<endl;
	ge.errCode = GFSErrorCode::OK;

	return ge;		

}



// RPCUpdateVersion is called by master

// update the given chunks' version to 'newVersion'

GFSError

	ChunkServer::RPCUpdateVersion(ChunkHandle handle, ChunkVersion newVersion)

{

	chunkversion[handle] = newVersion;

	GFSError ge;

	ge.errCode = GFSErrorCode::OK;

	std::string s = root;

	std::string sav = s + "/chunkversion.txt";

    freopen(sav.c_str(),"w",stdout);

    for (int i = 0;i < chunkid.size();++i) std::cout<<chunkversion[chunkid[i]]<<std::endl;

	return ge;

}



// RPCPushDataAndForward is called by client.

// It saves client pushed data to memory buffer.

// This should be replaced by a chain forwarding.

GFSError

	ChunkServer::RPCPushData(std::uint64_t dataID, std::string data)

{
cout<<1<<endl;
	datamap[dataID] = data;

	GFSError ge;

	ge.errCode = GFSErrorCode::OK;

	std::string s = root;
cout<<2<<endl;
	std::string sav = s + "/datamap.txt";


FILE * fp1 = fopen(sav.c_str(),"a");
 fprintf(fp1,"%s\n",data);
fclose(fp1);
    sav = s + "/dataID.txt";
cout<<3<<endl;
FILE * fp2 = fopen(sav.c_str(),"a");
 fprintf(fp2,"%lld\n",dataID);
fclose(fp2);
cout<<4<<endl;
	return ge;

}










