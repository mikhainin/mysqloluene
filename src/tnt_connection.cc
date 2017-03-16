#include "tnt_connection.h"

#include <stdint.h>
#include <iostream>

#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <tarantool/tnt_request.h>
#include <tarantool/tnt_object.h>

#include <msgpuck.h>

#include "tnt/iterator.h"
#include "tnt/tuple_builder.h"
#include "tnt/row.h"

TntConnection::TntConnection():
	tnt(nullptr),
	port(0)
{
/*
        const char * uri = "localhost:3301";
        struct tnt_stream * tnt = tnt_net(NULL); // Allocating stream
        tnt_set(tnt, TNT_OPT_URI, uri); // Setting URI
        tnt_set(tnt, TNT_OPT_SEND_BUF, 0); // Disable buffering for send
        tnt_set(tnt, TNT_OPT_RECV_BUF, 0); // Disable buffering for recv
        tnt_connect(tnt); // Initialize stream and connect to Tarantool
        tnt_ping(tnt); // Send ping request
        struct tnt_reply * reply = tnt_reply_init(NULL); // Initialize reply
        tnt->read_reply(tnt, reply); // Read reply from server
        tnt_reply_free(reply); // Free reply
        tnt_close(tnt); tnt_stream_free(tnt); // Close connection and free stream object
 */
}

TntConnection::~TntConnection()
{
	shutdownConnection();
}



void TntConnection::connect(const std::string &host_port)
{
	shutdownConnection(); // TODO: don't do this if we are/still connected

	tnt = tnt_net(NULL);
    tnt_set(tnt, TNT_OPT_URI, host_port.c_str()); // Setting URI
    tnt_set(tnt, TNT_OPT_SEND_BUF, 0); // Disable buffering for send
    tnt_set(tnt, TNT_OPT_RECV_BUF, 0); // Disable buffering for recv
    if (tnt_connect(tnt) != 0) {// Initialize stream and connect to Tarantool
    	// report error
    	std::cerr << tnt_strerror(tnt);
    	shutdownConnection();
    }
}

void TntConnection::shutdownConnection()
{
	if (tnt) {
		tnt_close(tnt);
		tnt_stream_free(tnt);
		tnt = nullptr;
	}
}

bool TntConnection::connected()
{
	return tnt != nullptr;
}

std::shared_ptr<tnt::Iterator> TntConnection::select(const std::string &space)
{
	int32_t sno = resolveSpace(space);
	if (sno == -1) {
		// TODO: set last error
		return std::shared_ptr<tnt::Iterator>();
	}

	struct tnt_stream *key = NULL;
	key = tnt_object(NULL);
	tnt_object_add_array(key, 0);
	tnt_select(tnt, sno, 0, UINT32_MAX, 0, 0, key); // box.space[sno]:select({}) // TODO: error check
	tnt_flush(tnt); // TODO: error check
	tnt_stream_free(key);

    auto result = tnt::Iterator::makeFromStream(tnt);
    tnt_stream_reqid(tnt, 0);

	return result;
}

bool TntConnection::insert(const std::string &space, const tnt::TupleBuilder &builder)
{
	int32_t sno = resolveSpace(space);
	if (sno == -1) {
		// TODO: set last error
		return false;
	}

	struct tnt_stream *val = tnt_object_as(NULL, const_cast<char*>(builder.ptr()), builder.size());
	auto result = tnt_insert(tnt, sno, val);
	tnt_flush(tnt); // TODO: error check
	tnt_stream_free(val);

	struct tnt_reply reply;
	tnt_reply_init(&reply);
	if (tnt->read_reply(tnt, &reply) == -1) {
		throw new std::runtime_error("Failed to read reply"); // TODO: add moar info into error message
	}
	tnt_reply_free(&reply);

	return true;
}

bool TntConnection::del(const std::string &space, const tnt::TupleBuilder &builder)
{
	int32_t sno = resolveSpace(space);
	if (sno == -1) {
		// TODO: set last error
		return false;
	}

	struct tnt_stream *val = tnt_object_as(NULL, const_cast<char*>(builder.ptr()), builder.size());
	auto result = tnt_delete(tnt, sno, 0, val);
	tnt_flush(tnt); // TODO: error check
	tnt_stream_free(val);

	struct tnt_reply reply;
	tnt_reply_init(&reply);
	if (tnt->read_reply(tnt, &reply) == -1) {
		throw new std::runtime_error("Failed to read reply"); // TODO: add moar info into error message
	}
	tnt_reply_free(&reply);

	return true;
}

bool TntConnection::update(const std::string &space, const tnt::TupleBuilder &builder)
{
	/*
	int32_t sno = resolveSpace(space);
	if (sno == -1) {
		// TODO: set last error
		return false;
	}

	struct tnt_stream *val = tnt_object_as(NULL, const_cast<char*>(builder.ptr()), builder.size());
	auto result = tnt_update(tnt, sno, val);
	tnt_flush(tnt); // TODO: error check
	tnt_stream_free(val);

	struct tnt_reply reply;
	tnt_reply_init(&reply);
	if (tnt->read_reply(tnt, &reply) == -1) {
		throw new std::runtime_error("Failed to read reply"); // TODO: add moar info into error message
	}
	tnt_reply_free(&reply);

	return true;*/
	return false; // TODO: implement me
}

int TntConnection::resolveSpace(const std::string &space)
{
	tnt_reload_schema(tnt); // TODO: error check if connected, if not loaded yet
	int32_t sno = tnt_get_spaceno(tnt, space.c_str(), space.size());
	return sno;
}
