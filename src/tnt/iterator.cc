#include "iterator.h"

#include <msgpuck.h>

#include <tarantool/tarantool.h>
#include <tarantool/tnt_net.h>
#include <tarantool/tnt_opt.h>
#include <tarantool/tnt_reply.h>
#include <tarantool/tnt_stream.h>
#include <tarantool/tnt_request.h>

#include <cassert>
#include <stdexcept>

#include "../tnt_row.h"


namespace tnt {

Iterator::Iterator():
	reply(nullptr),
	rowsNumber(0)
{
}

std::shared_ptr<Iterator> Iterator::makeFromStream(struct tnt_stream *tnt)
{
	std::shared_ptr<Iterator> iter = std::shared_ptr<Iterator>(new Iterator);

	iter->reply_holder.reset(
			new struct tnt_reply,
			&Iterator::deleteReply
		);
	iter->reply = iter->reply_holder.get();

	tnt_reply_init(iter->reply);
	if (tnt->read_reply(tnt, iter->reply) == -1) {
		throw new std::runtime_error("Failed to read reply"); // TODO: add moar info into error message
	}
	iter->tuples_data = iter->reply->data;
	assert(mp_typeof(*iter->tuples_data) == MP_ARRAY);
	iter->rowsNumber = mp_decode_array(&iter->tuples_data);

	return iter;
}

void Iterator::deleteReply(struct tnt_reply *reply)
{
	if (reply) {
		tnt_reply_free(reply);
		delete reply;
	}
}

std::shared_ptr<TntRow> Iterator::nextRow()
{
	if (!reply->data) {
		// error
		return std::shared_ptr<TntRow>();
	} else if (tuples_data == reply->data_end) {
		// data is over
		return std::shared_ptr<TntRow>();
	} else {
		auto row = TntRow::eatData(tuples_data); // (new TntRow(reply->data, reply->data - reply->data_end));
		return row;
	}
}

Iterator::operator bool() const
{
	return tuples_data != reply->data_end;
}

}
