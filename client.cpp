#include "client.h"
#include<iostream>
using namespace std;
Client::Client(LightDS::User &srv) : serve(srv) {}

// Create creates a new file on the specific path on GFS.
GFSError 
	Client::Create(const std::string &path)
{
      cout<<"wtl  "<<0<<endl;
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
cout<<"wtl  "<<1<<endl;
	GFSError ge = serve.RPCCall(ad,"RPCCreateFile",path).get().as<GFSError>();
cout<<"wtl  "<<2<<endl;
	return ge;
}

// Mkdir creates a new directory on GFS.
GFSError 
	Client::Mkdir(const std::string &path)
{
cout<<"lc  "<<0<<endl;
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
cout<<"lc  "<<1<<endl;
	GFSError ge = serve.RPCCall(ad,"RPCMkdir",path).get().as<GFSError>();
cout<<"lc  "<<2<<endl;
	return ge;
}

// List lists files and directories in specific directory on GFS.
std::tuple<GFSError, std::vector<std::string> /*filenames*/>
	Client::List(const std::string &path)
{
cout<<"tt"<<endl;
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
cout<<"tt"<<endl;
	std::tuple<GFSError, std::vector<std::string>> tp = serve.RPCCall(ad,"RPCListFile",path).get().as<std::tuple<GFSError, std::vector<std::string>>>();
cout<<"tt"<<endl;
	return tp;
}

// Read reads the file at specific offset.
// It reads up to data.size() bytes form the File.
// It return the number of bytes, and an error if any.
std::tuple<GFSError, size_t /*byteOfRead*/>
	Client::Read(const std::string &path, std::uint64_t offset, std::vector<char> &data)
{
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
	std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/> tp = serve.RPCCall(ad,"RPCFileInfo",path).get().as<std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/>>();
	std::uint64_t c = offset / std::get<2>(tp);
	int t = data.size();
	data.clear();
	std::uint64_t off = offset % std::get<2>(tp);
	while (t + off > 64 * 1024 * 1024)
	{
	    t -= 64 * 1024 * 1024 - off;
	    std::tuple<GFSError, ChunkHandle> tpp = serve.RPCCall(ad,"RPCGetChunkHandle",path,c).get().as<std::tuple<GFSError, ChunkHandle>>();
	    std::tuple<GFSError, size_t /*byteOfRead*/> tpc = ReadChunk(std::get<1>(tpp),off,data);
	    off = 0;
	    ++c;
	}
	std::tuple<GFSError, ChunkHandle> tpp = serve.RPCCall(ad,"RPCGetChunkHandle",path,c).get().as<std::tuple<GFSError, ChunkHandle>>();
	std::tuple<GFSError, size_t /*byteOfRead*/> tpc = ReadChunk(std::get<1>(tpp),off,data);
	off = 0;
	GFSError ge;
	ge.errCode = GFSErrorCode::OK;
	std::tuple<GFSError, size_t /*byteOfRead*/> tp2(ge,0);
	return tp2;
}

// Write writes data to the file at specific offset.
GFSError 
	Client::Write(const std::string &path, std::uint64_t offset, const std::vector<char> &data)
{
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
	std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/> tp = serve.RPCCall(ad,"RPCFileInfo",path).get().as<std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/>>();
	std::uint64_t c = offset / std::get<2>(tp);
	int t = data.size();
	std::uint64_t off = offset % std::get<2>(tp);
	while (t + off > 64 * 1024 * 1024)
	{
		t -= 64 * 1024 * 1024 - off;
	    std::tuple<GFSError, ChunkHandle> tpp = serve.RPCCall(ad,"RPCGetChunkHandle",path,c).get().as<std::tuple<GFSError, ChunkHandle>>();
	    GFSError ge = WriteChunk(std::get<1>(tpp),off,data);
		off = 0;
		++c;
	}
	std::tuple<GFSError, ChunkHandle> tpp = serve.RPCCall(ad,"RPCGetChunkHandle",path,data).get().as<std::tuple<GFSError, ChunkHandle>>();
	GFSError ge = WriteChunk(std::get<1>(tpp),off,data);
	return ge;
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
std::tuple<GFSError, std::uint64_t /*offset*/>
	Client::Append(const std::string &path, const std::vector<char> &data)
{
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
	std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/> tp = serve.RPCCall(ad,"RPCFileInfo",path).get().as<std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/>>();

	std::uint64_t c = 0;
	int t = data.size();
	std::uint64_t off = 0;
	bool b = true;
	while (t + off > 64 * 1024 * 1024)
	{
		t -= 64 * 1024 * 1024 - off;
	    std::tuple<GFSError, ChunkHandle> tpp = serve.RPCCall(ad,"RPCGetChunkHandle",path,c).get().as<std::tuple<GFSError, ChunkHandle>>();
		if (b == true)
		{
			std::tuple<GFSError, ChunkHandle> tptp = tpp;
			b = false;
		}
	    std::tuple<GFSError, std::uint64_t /*offset*/> tpc = AppendChunk(std::get<1>(tpp),data);
		off = 0;
		++c;
	}
	std::tuple<GFSError, ChunkHandle> tpp = serve.RPCCall(ad,"RPCGetChunkHandle",path,c).get().as<std::tuple<GFSError, ChunkHandle>>();
	std::tuple<GFSError, std::uint64_t /*offset*/> tptp=AppendChunk(std::get<1>(tpp),data);
	return tptp;
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, create one.
std::tuple<GFSError, ChunkHandle>
	Client::GetChunkHandle(const std::string &path, std::uint64_t index)
{
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
//	std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/> tpp = serve.RPCCall(ad,"RPCFileInfo",path).get().as<std::tuple<GFSError, bool /*IsDir*/, ////std::uint64_t /*Length*/, std::uint64_t /*Chunks*/>>();
	std::tuple<GFSError, ChunkHandle> tp = serve.RPCCall(ad,"RPCGetChunkHandle",path,index).get().as<std::tuple<GFSError, ChunkHandle>>();
	return tp;
}

// ReadChunk reads data from the chunk at specific offset.
// data.size()+offset  should be within chunk size.
std::tuple<GFSError, size_t /*byteOfRead*/>
	Client::ReadChunk(ChunkHandle handle, std::uint64_t offset, std::vector<char> &data)
{
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
cout<<1<<endl;
	std::tuple<GFSError, std::vector<std::string>> tp = serve.RPCCall(ad,"RPCGetReplicas",handle).get().as<std::tuple<GFSError, std::vector<std::string>>>();
cout<<2<<endl;
	std::string s = std::get<1>(tp)[0];
cout<<3<<endl;
	LightDS::Service::RPCAddress ads = LightDS::Service::RPCAddress::from_string(s);
cout<<4<<endl;
	std::tuple<GFSError, std::string> tpc = serve.RPCCall(ads,"RPCReadChunk",handle,offset,-1).get().as<std::tuple<GFSError, std::string>>();
cout<<5<<endl;
	std::string da = std::get<1>(tpc);
	for (int i = 0;i < da.size();++i) data.push_back(da[i]);
	std::tuple<GFSError, size_t /*byteOfRead*/> tpp(std::get<0>(tpc),std::get<1>(tpc).size());
cout<<6<<endl;
	return tpp;
}

// WriteChunk writes data to the chunk at specific offset.
// data.size()+offset should be within chunk size.
GFSError
	Client::WriteChunk(ChunkHandle handle, std::uint64_t offset, const std::vector<char> &data)
{
        cout<<1<<endl;
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
cout<<2<<endl;
	std::tuple<GFSError, std::string /*Primary Address*/, std::vector<std::string> /*Secondary Addresses*/, std::uint64_t /*Expire Timestamp*/> tp;
cout<<3<<endl;
	tp = serve.RPCCall(ad,"RPCGetPrimaryAndSecondaries",handle).get().as<std::tuple<GFSError, std::string /*Primary Address*/, std::vector<std::string> /*Secondary Addresses*/, std::uint64_t /*Expire Timestamp*/>>();
cout<<4<<endl;
	std::string s = "";
	for (int i = 0;i < data.size();++i) s += data[i];
cout<<5<<endl;
	LightDS::Service::RPCAddress add = LightDS::Service::RPCAddress::from_string(std::get<1>(tp));
cout<<6<<endl;
	int t0 = time(0);
	std::uint64_t t = t0 << 32 + random() % t0;
cout<<7<<endl;
	serve.RPCCall(add,"RPCPushData",t,s);
cout<<8<<endl;
	GFSError ge = serve.RPCCall(add,"RPCWriteChunk",handle,t,offset,std::get<2>(tp)).get().as<GFSError>();
cout<<9<<endl;
	return ge;
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// data.size() should be within max append size.
std::tuple<GFSError, std::uint64_t /*offset*/>
	Client::AppendChunk(ChunkHandle handle, const std::vector<char> &data)
{
	srand(time(0));
	LightDS::Service::RPCAddress ad = serve.ListService("master")[0];
	std::tuple<GFSError, std::string /*Primary Address*/, std::vector<std::string> /*Secondary Addresses*/, std::uint64_t /*Expire Timestamp*/> tp;
	tp = serve.RPCCall(ad,"RPCGetPrimaryAndSecondaries",handle).get().as<std::tuple<GFSError, std::string /*Primary Address*/, std::vector<std::string> /*Secondary Addresses*/, std::uint64_t /*Expire Timestamp*/>>();
	std::string s = "";
	for (int i = 0;i < data.size();++i) s += data[i];
	LightDS::Service::RPCAddress add = LightDS::Service::RPCAddress::from_string(std::get<1>(tp));
	int t0 = time(0);
	int t = t0 << 32 + rand() % t0;
	serve.RPCCall(add,"RPCPushData",handle,t,s);
	std::tuple<GFSError, std::uint64_t /*offset*/> tpc = serve.RPCCall(add,"RPCAppendChunk",handle,t,std::get<2>(tp)).get().as<std::tuple<GFSError, std::uint64_t /*offset*/>>();
	return tpc;
}
		
		
		
