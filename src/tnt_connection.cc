#include "tnt_connection.h"

#include <stdint.h>
#include <iostream>

#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <tarantool/tnt_request.h>

#include <msgpuck.h>

#include "tnt_row.h"

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

std::shared_ptr<TntRow> TntConnection::select(const std::string &space)
{
/*
int tnt_get_spaceno(struct tnt_stream *s, const char *space,
s		    size_t space_len)
{
	struct tnt_schema *sch = (TNT_SNET_CAST(s))->schema;
	return tnt_schema_stosid(sch, space, space_len);
}
 */

	struct tnt_stream *key = NULL;
	key = tnt_object(NULL);
	tnt_object_add_array(key, 0);
	tnt_select(tnt, 513, 0, UINT32_MAX, 0, 0, key); // box.space[513]:select({})
	tnt_flush(tnt);
	tnt_stream_free(key);

	struct tnt_reply reply;
	tnt_reply_init(&reply);
	tnt->read_reply(tnt, &reply);

	const char *data = reply.data;
	std::size_t data_len = reply.data_end - reply.data;
	// uint32_t vsz = mp_decode_array(&data);
    // uint32_t arrsz = vsz;
    // uint32_t str_len = 0;

    auto result = std::shared_ptr<TntRow>(new TntRow(data, data_len));
    tnt_reply_free(&reply);
    tnt_stream_reqid(tnt, 0);

	tnt_reply_free(&reply);

	return result;
}
