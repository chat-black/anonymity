#include <iostream>
#include <string>
#include <cstring>
#include <cstdio>
#include <vector>
#include <curl/curl.h>

/***************************************************
 * use the libcurl to achieve a http client and send the results to web server
 ***************************************************/
struct Sender
{
    std::vector<char*> record;  // record the data needed to send
    std::vector<char*> record_id;
    std::string response;  // http response
    std::string sha256;  // identifier of the SQL statement
    std::string url;
    CURL* curl;
    CURLcode ret;
public:
    // initialize the socket and connect to the server
    Sender(int port=5000)
    {
        curl = nullptr;
        curl = curl_easy_init();
        url = std::string("http://127.0.0.1:") + std::to_string(port) + std::string("/arena_inner/");
    }

    ~Sender(){  // release memory
        for (std::size_t i=0;i<record.size();i++){
            delete [] record[i];
            delete [] record_id[i];
        }
    }

	static int write_data(void *buffer, int size, int nmemb, void *userp)
	{
		std::string *str = dynamic_cast<std::string *>((std::string *) userp);
		str->append((char *) buffer, size * nmemb);
		return nmemb;
	}

    bool isStart()
    {
        return curl != nullptr;
    }

    // close http client and send the data
    bool Close()
	{
		struct curl_httppost *post = NULL;
		struct curl_httppost *last = NULL;
		if (curl)
		{
			curl_easy_setopt(curl, CURLOPT_URL, (char *) url.c_str());

            curl_formadd( &post, &last, CURLFORM_PTRNAME, "hash", CURLFORM_PTRCONTENTS, sha256.c_str(), CURLFORM_END);
            for (std::size_t i=0;i<record.size();i++){
				curl_formadd(
					&post, &last, CURLFORM_PTRNAME, record_id[i],
					CURLFORM_PTRCONTENTS, record[i],
					CURLFORM_END);
			}

			curl_easy_setopt(curl, CURLOPT_HTTPPOST, (void *)post);
			curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, (void *)Sender::write_data);
			curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) (&response));

			ret = curl_easy_perform(curl);
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
