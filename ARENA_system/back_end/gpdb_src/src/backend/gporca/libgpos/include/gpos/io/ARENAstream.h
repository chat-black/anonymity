#ifndef ARENA_IOSTREAM_H
#define ARENA_IOSTREAM_H

#include "gpos/io/IOstream.h"
#include <cstdio>
#include <sstream>
#include <cwchar>

/********************ARENA********************/
namespace gpos
{
    class ARENAIOstream :public IOstream
    {
    public:
        std::stringstream ss;

        // operator interface
        IOstream &operator<<(const CHAR * str)
        {
            ss << str;
            return *this;
        }

        IOstream &operator<<(const WCHAR w_str)
        {
            // WCHAR w_copy = w_str;
            // std::wstring temp(w_copy);
            // ss << std::string(temp.begin(), temp.end());
            // return *this;
            if (w_str)
            {
                ss << "I don't know";
                return *this;
            }
            return *this;
        }

        IOstream &operator<<(const CHAR c)
        {
            ss << c;
            return *this;
        }

        IOstream &operator<<(ULONG l)
        {
            ss << l;
            return *this;
        }

        IOstream &operator<<(ULLONG l)
        {
            ss << l;
            return *this;
        }

        IOstream &operator<<(INT i)
        {
            ss << i;
            return *this;
        }

        IOstream &operator<<(SINT i)
        {
            ss << i;
            return *this;
        }

        IOstream &operator<<(USINT i)
        {
            ss << i;
            return *this;
        }

        IOstream &operator<<(LINT i)
        {
            ss << i;
            return *this;
        }

        IOstream &operator<<(DOUBLE d)
        {
            ss << d;
            return *this;
        }

        IOstream &operator<<(const void * p)
        {
            ss << p;
            return *this;
        }

        IOstream &operator<<(WOSTREAM &(*) (WOSTREAM &) )
        {
            ss << "I don't know";
            return *this;
        }

        IOstream &operator<<(EStreamManipulator)
        {
            ss << "I don't know";
            return *this;
        }

	// needs to be implemented by subclass
        IOstream &operator<<(const WCHAR * w_str)
        {
            std::wstring temp(w_str);
            ss << std::string(temp.begin(), temp.end());
            return *this;
        }

        IOstream &operator<<(const CWStringConst *pwsconst) {
            return operator<<(pwsconst->GetBuffer());
        }

        IOstream &operator<<(BOOL b)
        {
            ss << b;
            return *this;
        }

        void reset()
        {
            ss.clear();
            ss.str("");
        }
    };
}

#endif