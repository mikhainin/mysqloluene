#pragma once

#include <memory>
#include "../tnt_row.h"

struct tnt_reply;
struct tnt_stream;

namespace tnt {

class Iterator
{
	Iterator();
public:
	static std::shared_ptr<Iterator> makeFromStream(struct tnt_stream *tnt);
	std::shared_ptr<TntRow> nextRow();
	operator bool() const;
private:
	std::shared_ptr<struct tnt_reply> reply_holder;
	struct tnt_reply *reply;
	const char *tuples_data = nullptr;
	int rowsNumber;

	static void deleteReply(struct tnt_reply *reply);
};

}
