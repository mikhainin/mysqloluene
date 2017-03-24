#include "connection.h"

#include <stdint.h>
#include <iostream>

#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <tarantool/tnt_request.h>
#include <tarantool/tnt_object.h>

#include <msgpuck.h>

#include "iterator.h"
#include "tuple_builder.h"
#include "row.h"

namespace tnt {

Connection::Connection():
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

Connection::~Connection()
{
	shutdownConnection();
}



void Connection::connect(const std::string &host_port)
{
	last_error.clear();

	shutdownConnection(); // TODO: don't do this if we are/still connected

	tnt = tnt_net(NULL);
    tnt_set(tnt, TNT_OPT_URI, host_port.c_str()); // Setting URI
    tnt_set(tnt, TNT_OPT_SEND_BUF, 0); // Disable buffering for send
    tnt_set(tnt, TNT_OPT_RECV_BUF, 0); // Disable buffering for recv
    if (tnt_connect(tnt) != 0) {// Initialize stream and connect to Tarantool
    	// report error
    	last_error = tnt_strerror(tnt);
    	shutdownConnection();
    }
}

void Connection::shutdownConnection()
{
	if (tnt) {
		tnt_close(tnt);
		tnt_stream_free(tnt);
		tnt = nullptr;
	}
}

bool Connection::connected()
{
	return tnt != nullptr;
}

std::shared_ptr<tnt::Iterator> Connection::select(const std::string &space, const tnt::TupleBuilder &builder)
{
	last_error.clear();

	int32_t sno = resolveSpace(space);
	if (sno == -1) {
		last_error = "Can't resolve space '" + space + "'";
		return std::shared_ptr<tnt::Iterator>();
	}

	std::unique_ptr<struct tnt_stream, void(*)(struct tnt_stream*)> key (
			tnt_object_as(NULL, const_cast<char*>(builder.ptr()), builder.size()),
			Connection::deleteStream
		);

	if (tnt_select(tnt, sno, 0, UINT32_MAX, 0, 0, key.get()) == -1) {// box.space[sno]:select({}) {
		last_error = tnt_strerror(tnt);
		return std::shared_ptr<tnt::Iterator>();
	}
	if (tnt_flush(tnt) == -1) {
		last_error = tnt_strerror(tnt);
		return std::shared_ptr<tnt::Iterator>();
	}

    auto result = tnt::Iterator::makeFromStream(tnt);

	return result;
}

bool Connection::insert(const std::string &space, const tnt::TupleBuilder &builder)
{
	last_error.clear();

	int32_t sno = resolveSpace(space);
	if (sno == -1) {
		// TODO: set last error
		return false;
	}

	struct tnt_stream *val = tnt_object_as(NULL, const_cast<char*>(builder.ptr()), builder.size());
	auto result = tnt_insert(tnt, sno, val);
	if (result == -1) {
		tnt_stream_free(val);
		last_error = tnt_strerror(tnt);
		return false;
	}
	tnt_stream_free(val);
	if (tnt_flush(tnt) == -1) {
		last_error = tnt_strerror(tnt);
		return false;
	}

	struct tnt_reply reply;
	tnt_reply_init(&reply);
	if (tnt->read_reply(tnt, &reply) == -1) {
		throw new std::runtime_error("Failed to read reply"); // TODO: add moar info into error message
	}
	tnt_reply_free(&reply);

	return true;
}

bool Connection::del(const std::string &space, const tnt::TupleBuilder &builder)
{
	last_error.clear();

	int32_t sno = resolveSpace(space);
	if (sno == -1) {
		last_error = "Can't resolve space '" + space + "'";
		return false;
	}

	std::unique_ptr<struct tnt_stream, void(*)(struct tnt_stream*)> key (
			tnt_object_as(NULL, const_cast<char*>(builder.ptr()), builder.size()),
			Connection::deleteStream
		);
	if (tnt_delete(tnt, sno, 0, key.get()) == -1) {
		last_error = tnt_strerror(tnt);
		return false;
	}
	if (tnt_flush(tnt) == -1) {
		last_error = tnt_strerror(tnt);
		return false;
	}

	struct tnt_reply reply;
	tnt_reply_init(&reply);
	if (tnt->read_reply(tnt, &reply) == -1) {
		tnt_reply_free(&reply);
		if (reply.error) {
			last_error.assign(reply.error, reply.error_end);
		} else {
			last_error = "Unknown reply error"; // TODO: add error code
		}
		return false;
	}
	tnt_reply_free(&reply);

	return true;
}

bool Connection::replace(const std::string &space, const tnt::TupleBuilder &builder)
{
	last_error.clear();

	int32_t sno = resolveSpace(space);
	if (sno == -1) {
		// TODO: set last error
		last_error = "Space not found. ";
		last_error.append(tnt_strerror(tnt));
		return false;
	}

	std::unique_ptr<struct tnt_stream, void(*)(struct tnt_stream*)> val(
			tnt_object_as(NULL, const_cast<char*>(builder.ptr()), builder.size()),
			Connection::deleteStream
		);
	auto result = tnt_replace(tnt, sno, val.get());
	int64_t reqid = tnt->reqid;
	if (tnt_flush(tnt) == -1) {
		last_error = tnt_strerror(tnt);
		return false;
	}

	struct tnt_reply reply;
	tnt_reply_init(&reply);
	if (tnt->read_reply(tnt, &reply) == -1) {
		last_error = tnt_strerror(tnt);
		tnt_reply_free(&reply);
		return false; // TODO: add moar info into error message
	}
	if (reqid != reply.sync) {
		tnt_reply_free(&reply);
		last_error = "sync mismatch";
		shutdownConnection();
		return false;
	}
	if (reply.code != 0) {
		tnt_reply_free(&reply);
		if (reply.error) {
			last_error.assign(reply.error, reply.error_end);
		} else {
			last_error = "Unknown reply error"; // TODO: add error code
		}
		return false;
	}
	tnt_reply_free(&reply);

	return true;
}

int Connection::resolveSpace(const std::string &space)
{
	if (spaces.find(space) == spaces.end()) {
		tnt_reload_schema(tnt); // TODO: error check if connected, if not loaded yet
		int32_t sno = tnt_get_spaceno(tnt, space.c_str(), space.size());
		if (sno == -1) {
			return -1;
		}
		spaces[space] = sno;
	}
	return spaces[space];
}

void Connection::deleteStream(struct tnt_stream *stream)
{
	tnt_stream_free(stream);
}

}
