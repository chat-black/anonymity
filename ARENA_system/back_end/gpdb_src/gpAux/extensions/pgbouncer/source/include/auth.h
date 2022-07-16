/*-------------------------------------------------------------------------
 * auth.h
 *    Function and type definition for async authentication in pgbouncer
 *
 * Copyright (c) 2015 Pivotal Inc. All Rights Reserved
 *
 *-------------------------------------------------------------------------
 */
#ifndef _AUTH_ASYNC_H_
#define _AUTH_ASYNC_H_

#define AUTH_TIMEOUT 10
#define AUTH_MSGLEN  1024


struct AuthContext {
	PgSocket* client;
	struct event ev;
	const char* passwd;
	PgUser* user;
	const char* serveraddr;
	void* data;
	//AuthFun authfun;
};

struct AuthContext* CreateAuthContext(PgSocket* client, const char* passwd);
void DestroyAuthContext(struct AuthContext* auth);

bool auth_success(PgSocket *client);
bool auth_fail(PgSocket *client, char* msg);
bool need_external_auth(PgSocket *client) _MUSTCHECK;
bool async_auth_client(PgSocket *client, const char* passwd);

void start_authproxy(void);

#endif
