#include <iostream>
#include <string>
#include <cstring>
#include <cstdio>
#include <vector>
#include <curl/curl.h>

/*********************
 * 利用 libcurl 库实现 http 客户端，用于将最终结果发送给 web 服务器
 **********************/
struct Sender
{
    std::vector<char*> record;  // 记录需要发送的信息
    std::vector<char*> record_id;
    std::string response;  // 获得 web 服务器响应的结果
    std::string sha256;  // SQL语句的sha256值
    std::string url;
    CURL* curl;
    CURLcode ret;
public:
    // 初始化套接字，并与服务器建立连接
    Sender(int port=5000)
    {
        curl = nullptr;
        curl = curl_easy_init();
        url = std::string("http://127.0.0.1:") + std::to_string(port) + std::string("/arena_inner/");
    }

    ~Sender(){  // 释放内存
        for (std::size_t i=0;i<record.size();i++){
            delete [] record[i];
            delete [] record_id[i];
        }
    }

    // 回调函数，得到相应内容
	static int write_data(void *buffer, int size, int nmemb, void *userp)
	{
		std::string *str = dynamic_cast<std::string *>((std::string *) userp);
		str->append((char *) buffer, size * nmemb);
		return nmemb;
	}


	// 判断是否与服务器成功建立连接
    bool isStart()
    {
        return curl != nullptr;
    }


    // 关闭客户端，并真正的发送数据
    bool Close()
	{
		struct curl_httppost *post = NULL;
		struct curl_httppost *last = NULL;
		if (curl)
		{
			curl_easy_setopt(curl, CURLOPT_URL, (char *) url.c_str());	 //指定url

            curl_formadd( &post, &last, CURLFORM_PTRNAME, "hash", CURLFORM_PTRCONTENTS, sha256.c_str(), CURLFORM_END);  // SQL 的摘要
            for (std::size_t i=0;i<record.size();i++){
				curl_formadd(
					&post, &last, CURLFORM_PTRNAME, record_id[i],
					CURLFORM_PTRCONTENTS, record[i],
					CURLFORM_END);	//form-data key 和 value
			}

			curl_easy_setopt(curl, CURLOPT_HTTPPOST, (void *)post);	 //构造post参数
			curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, (void *)Sender::write_data);  //绑定相应
			curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) (&response));  //绑定响应内容的地址

			ret = curl_easy_perform(curl);	//执行请求
            curl_easy_cleanup(curl);
			if (ret == 0)
			{
				return response == std::string("OK");
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}

	 void send_result(std::string &message)
     {
        char * temp_id = new char[4];
        sprintf(temp_id, "%d", int(record.size()));
        record_id.push_back(temp_id);

        char * temp = new char[message.size()+1];
        for (std::size_t i=0;i<message.size();i++) {
            temp[i] = message[i];
        }
        temp[message.size()] = '\0';
        record.push_back(temp);
     }

};
