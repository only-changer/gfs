#include "master.h"
#include<iostream>
using namespace std;
Master::Master(LightDS::Service &serv, const std::string &rootdir) : srv(serv), rootDir(rootdir) {
	srv.RPCBind<std::tuple<GFSError, std::vector<ChunkHandle>>
		(std::vector<ChunkHandle>, std::vector<std::tuple<ChunkHandle, ChunkVersion>>, std::vector<ChunkHandle>)>
		("RPCHeartbeat", std::bind(&Master::RPCHeartbeat, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
	srv.RPCBind<std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t>
		(ChunkHandle)>("RPCGetPrimaryAndSecondaries", 
		std::bind(&Master::RPCGetPrimaryAndSecondaries, this, std::placeholders::_1));
	srv.RPCBind<std::tuple<GFSError, std::vector<std::string>>
		(ChunkHandle)>("RPCGetReplicas", 
		std::bind(&Master::RPCGetReplicas, this, std::placeholders::_1));
	srv.RPCBind<std::tuple<GFSError, bool, std::uint64_t, std::uint64_t>
		(std::string path)>("RPCGetFileInfo", 
		std::bind(&Master::RPCGetFileInfo, this, std::placeholders::_1));
	srv.RPCBind<GFSError(std::string)>("RPCCreateFile", 
		std::bind(&Master::RPCCreateFile, this, std::placeholders::_1));
	srv.RPCBind<GFSError(std::string)>("RPCDeleteFile", 
		std::bind(&Master::RPCDeleteFile, this, std::placeholders::_1));
	srv.RPCBind<GFSError(std::string)>("RPCMkdir", 
		std::bind(&Master::RPCMkdir, this, std::placeholders::_1));
	srv.RPCBind<std::tuple<GFSError, std::vector<std::string>>(std::string)>("RPCListFile", 
		std::bind(&Master::RPCListFile, this, std::placeholders::_1));
	srv.RPCBind<std::tuple<GFSError, ChunkHandle>(std::string, std::uint64_t)>("RPCGetChunkHandle", 
		std::bind(&Master::RPCGetChunkHandle, this, std::placeholders::_1, std::placeholders::_2));
}

void Master::Start() {
	/*	HandleToVersion.clear();
		ChunkCopy.clear();
		std::string s = rootDir + "\\HandleToVersion.txt";
		FILE * fp1 = fopen(s.c_str(), "r");
		long long m1, m2;
		while (fscanf(fp1 , "%lld%lld" , &m1, &m2) != EOF) {
			HandleToVersion[m1] = m2;
		}
//		fp1 -> close();
		s = rootDir + "\\ChunkCopy.txt";
		FILE * fp2 = fopen(s.c_str(), "r");
		std::string s1,s2,s3,s4;
		LightDS::Service::RPCAddress t1, t2, t3, t4;
		long long m;
		while (std::cin >>m) {
			std::vector<LightDS::Service::RPCAddress> tmp;
			std::cin >>s1;t1 = LightDS::Service::RPCAddress::from_string(s1);
			tmp.push_back(t1);
			std::cin >>s2;t2 = LightDS::Service::RPCAddress::from_string(s2);
			tmp.push_back(t2);
			std::cin >>s3;t3 = LightDS::Service::RPCAddress::from_string(s3);
			tmp.push_back(t3);
			std::cin >>s4;t4 = LightDS::Service::RPCAddress::from_string(s4);
			tmp.push_back(t4);
			ChunkCopy[m] = tmp;
		}*/
//		fp2 -> close();
	}

void Master::Shutdown()
{
}
std::tuple<GFSError, std::vector<ChunkHandle> /*Garbage Chunks*/>
		Master::RPCHeartbeat(std::vector<ChunkHandle> leaseExtensions, 
		std::vector<std::tuple<ChunkHandle, ChunkVersion>> chunks, 
		std::vector<ChunkHandle> failedChunks) {
		GFSError g;
		g.errCode = GFSErrorCode::OK;
		std::vector<ChunkHandle> Garbage;
		for (std::vector<std::tuple<ChunkHandle, ChunkVersion>>::iterator iter = chunks.begin(); iter != chunks.end(); iter++) {
			ChunkHandle Han = std::get<0>(*iter);
			if (HandleToVersion[Han] != std::get<1>(*iter)) {
				HandleToVersion[Han] =std::get<1>(*iter);
			}
		}
		std::string s = rootDir + "\\HandleToVersion.txt";
		std::ofstream fout(s.c_str());
		std::map<ChunkHandle, ChunkVersion>::iterator iter;
		for (iter = HandleToVersion.begin(); iter != HandleToVersion.end(); iter++) 
			fout <<iter -> first <<' ' <<iter -> second <<std::endl;
//		fout.close();
		
		
		std::vector<std::tuple<ChunkHandle , ChunkVersion , std::uint64_t >> temp;
		for (std::vector<ChunkHandle>::iterator it = leaseExtensions.begin(); it != leaseExtensions.begin(); it++) {
			std::tuple<ChunkHandle , ChunkVersion , std::uint64_t > tp(*it, HandleToVersion[*it], time(0) + 60);
			temp.push_back(tp);
		}
		srv.RPCCall(ChunkCopy[*leaseExtensions.begin()][0],"RPCGrantLease", temp);
		
		std::tuple<GFSError, std::vector<ChunkHandle> > tmp(g, Garbage);
		return tmp;
	} 
		
std::tuple<GFSError, std::string /*Primary Address*/,
			std::vector<std::string> /*Secondary Addresses*/, 
			std::uint64_t /*Expire Timestamp*/>
		Master::RPCGetPrimaryAndSecondaries(ChunkHandle handle) {
		GFSError g;
		g.errCode = GFSErrorCode::OK;
		std::tuple<ChunkHandle, ChunkVersion, std::uint64_t> lease(handle, HandleToVersion[handle], time(0) + 60);
		int r = rand() % 4;
cout<<"???   !!!   "<<0<<endl;
		std::vector<std::tuple<ChunkHandle, ChunkVersion, std::uint64_t>> v;
		v.push_back(lease);
		srv.RPCCall(ChunkCopy[handle][r],"RPCGrantLease", v);
cout<<"???   !!!   "<<1<<endl;
		std::vector<std::string> secondaries;
		if (r == 0) {
			secondaries.push_back(ChunkCopy[handle][1].to_string());
			secondaries.push_back(ChunkCopy[handle][2].to_string());
			secondaries.push_back(ChunkCopy[handle][3].to_string());
		}
		else if (r == 1) {
			secondaries.push_back(ChunkCopy[handle][0].to_string());
			secondaries.push_back(ChunkCopy[handle][2].to_string());
			secondaries.push_back(ChunkCopy[handle][3].to_string());
		}
		else if (r == 2) {
			secondaries.push_back(ChunkCopy[handle][0].to_string());
			secondaries.push_back(ChunkCopy[handle][1].to_string());
			secondaries.push_back(ChunkCopy[handle][3].to_string());
		}
		else if (r == 3) {
			secondaries.push_back(ChunkCopy[handle][0].to_string());
			secondaries.push_back(ChunkCopy[handle][1].to_string());
			secondaries.push_back(ChunkCopy[handle][2].to_string());
		}
		std::tuple<GFSError, std::string, std::vector<std::string>, std::uint64_t>
		temp(g, ChunkCopy[handle][r].to_string(), secondaries, time(0) + 60);
return temp;
	} 

std::tuple<GFSError, std::vector<std::string> /*Locations*/>
		Master::RPCGetReplicas(ChunkHandle handle) {
		if (ChunkCopy.count(handle) == 0) {
			GFSError g;
			g.errCode = GFSErrorCode::error;
			g.description = "No such handle";
			std::vector<std::string> tmp;
			std::tuple<GFSError, std::vector<std::string> > temp(g, tmp);
			return temp;
		}
		else {
			GFSError g;
			g.errCode = GFSErrorCode::OK;
			std::vector<std::string> tmp;
			tmp.push_back(ChunkCopy[handle][0].to_string());
			tmp.push_back(ChunkCopy[handle][0].to_string());
			tmp.push_back(ChunkCopy[handle][0].to_string());
			tmp.push_back(ChunkCopy[handle][0].to_string());
			std::tuple<GFSError, std::vector<std::string> > temp(g, tmp);
			return temp;
		}
	} 
		
std::tuple<GFSError, bool /*IsDir*/, std::uint64_t /*Length*/, std::uint64_t /*Chunks*/>
		Master::RPCGetFileInfo(std::string path) {
		bool IsDir;
		DIR *dirptr = NULL; 
		std::string s = rootDir + path;
		if(opendir(s.c_str()) == NULL) {  
			IsDir = false;
		}
		else {
			IsDir = true;
		}  
		if (!IsDir) {
			GFSError g;
			g.errCode = GFSErrorCode::OK;
			FILE * fp1 = fopen(path.c_str(), "r");
			fseek(fp1, 0, SEEK_END);
            std::uint64_t size = ftell(fp1); 
			std::vector<ChunkHandle> handle;
			long long m;
			while (fscanf(fp1 , "%lld" , &m) != EOF) {
				handle.push_back(m);
			}
			std::tuple<GFSError, bool , std::uint64_t , std::uint64_t > tmp(g, 1, size, handle.size());
			return tmp;
		}
		else {
			GFSError g;
			g.errCode = GFSErrorCode::error;
			g.description = "IsDir";
			std::tuple<GFSError, bool , std::uint64_t , std::uint64_t > tmp(g, 0, 0, 0);
			return tmp;
		}
		
	} 

GFSError
	Master::RPCCreateFile(std::string path) {
		std::string s = rootDir + path;
cout<<s<<endl;
		GFSError g;
		if (access(s.c_str(), F_OK) == 0) {
			g.errCode = GFSErrorCode::error;
			g.description = "Create file failed";
		}
		else {
			std::ofstream outfile(s.c_str());
			if (outfile) {
				g.errCode = GFSErrorCode::OK;
			}
			else {
				g.errCode = GFSErrorCode::error;
				g.description = "Create file failed";
			}
		}
		
		return g;
//		outfile.close();
return g;
	} 

GFSError
	Master::RPCDeleteFile(std::string path) {
		std::string s = rootDir + path;
		int b = remove(s.c_str());
		GFSError g;
		if (b) {
			g.errCode = GFSErrorCode::error;
            g.description = "Delete file failed";
		}
		else {
			g.errCode = GFSErrorCode::OK;
		}
		return g;
	}

GFSError
	Master::RPCMkdir(std::string path) {
cout<<000<<endl;
		std::string s = rootDir + path;	

cout<<001<<endl;

cout<<s<<endl;
		int isCreate = mkdir(s.c_str(),S_IRUSR | S_IWUSR | S_IXUSR | S_IRWXG | S_IRWXO);
cout<<002<<endl;
   		GFSError g;
		if(isCreate) {
cout<<003<<endl;
   			g.errCode = GFSErrorCode::error;
		}
   		else {
cout<<004<<endl;
   			g.errCode = GFSErrorCode::OK;
   		}
    	return g;
	} 

std::tuple<GFSError, std::vector<std::string> /*FileNames*/>
	Master::RPCListFile(std::string path) {

	std::string s = rootDir + path;	

	struct dirent * filename;

    	DIR * dir;

    	dir = opendir( s.c_str() );  

    	std::vector<std::string> List;

    	GFSError g;

    	g.errCode = GFSErrorCode::OK;

		while( ( filename = readdir(dir) ) != NULL ) {  

cout<<filename<<endl;
        	if( strcmp( filename->d_name , "." ) == 0 || strcmp( filename->d_name , "..") == 0)  
            	continue;  
        	List.push_back(filename ->d_name);
    	}  

std::tuple<GFSError, std::vector<std::string> /*FileNames*/> tp(g,List);
return tp;
	} 

std::tuple<GFSError, ChunkHandle>
	Master::RPCGetChunkHandle(std::string path, std::uint64_t chunkIndex) {
cout<<"000 "<<0<<endl;
		std::string s = rootDir + path;		
		FILE * fp1 = fopen(s.c_str(), "r");
		GFSError g;
		std::vector<ChunkHandle> handle;
		long long m;
cout<<"000 "<<1<<endl;
		while (fscanf(fp1 , "%lld" , &m) != EOF) {
			handle.push_back(m);
		}
		fclose(fp1);
cout<<"000 "<<2<<endl;
		if (handle.size() > chunkIndex) {
cout<<"000 "<<3<<endl;
		ChunkHandle tmp = handle[chunkIndex];
			g.errCode = GFSErrorCode::OK;
			std::tuple<GFSError, ChunkHandle> h(g, tmp);
			return h;
		}
		else if (handle.size() == chunkIndex) {
cout<<"000 "<<4<<endl;
			srand((unsigned)time(NULL));
			std::vector<LightDS::Service::RPCAddress> ad = srv.ListService("chunkserver");
			ChunkHandle chunkh = rand() % 2147483647 + time(0) << 32;
			while (ChunkCopy.count(chunkh) != 0) chunkh = rand() % 2147483647 + time(0) << 32;
			std::vector<LightDS::Service::RPCAddress> chundle;
			chundle.push_back(ad[rand() % ad.size()]);
			long long adv = rand() % ad.size();
			while (ad[adv].to_string() == chundle[0].to_string()) adv = rand() % ad.size();
			chundle.push_back(ad[adv]);
			while (ad[adv].to_string() == chundle[0].to_string() || ad[adv].to_string() == chundle[1].to_string()) adv = rand() % ad.size();
			chundle.push_back(ad[adv]);
			while (ad[adv].to_string() == chundle[0].to_string() || ad[adv].to_string() == chundle[1].to_string() || ad[adv].to_string() == chundle[2].to_string()) adv = rand() % ad.size();
			chundle.push_back(ad[adv]);
cout<<"000 "<<5<<' '<<chunkh<<endl;
			srv.RPCCall(chundle[0],"RPCCreateChunk",chunkh);
cout<<"000 "<<6<<endl;
			srv.RPCCall(chundle[1],"RPCCreateChunk",chunkh);
cout<<"000 "<<7<<endl;
			srv.RPCCall(chundle[2],"RPCCreateChunk",chunkh);
cout<<"000 "<<8<<endl;
			srv.RPCCall(chundle[3],"RPCCreateChunk",chunkh);
cout<<"000 "<<9<<endl;
			m = chunkh;			
			FILE * fp2 = fopen(s.c_str(),"a");
			fprintf(fp2, "%lld\n", m);
			fclose(fp2);
			ChunkCopy[chunkh] = chundle;
			g.errCode = GFSErrorCode::OK;
			std::tuple<GFSError, ChunkHandle> h(g, chunkh);
			return h;
		}
		else {
			g.errCode = GFSErrorCode::error;
			std::tuple<GFSError, ChunkHandle> h(g, 0);
			return h;
		}
//		fp1 -> close();
		 s = rootDir + "\\ChunkCopy.txt";
		std::ofstream fout(s.c_str());
		std::map<ChunkHandle, std::vector<LightDS::Service::RPCAddress> >::iterator iter;
		for (iter = ChunkCopy.begin(); iter != ChunkCopy.end(); iter++) {
			fout << iter -> first <<std::endl;
			fout << iter -> second[0].to_string() <<std::endl;
			fout << iter -> second[1].to_string() <<std::endl;
			fout << iter -> second[2].to_string() <<std::endl;
			fout << iter -> second[3].to_string() <<std::endl;
		}
	
//		fout.close();
	} 
	

