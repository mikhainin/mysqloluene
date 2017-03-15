#pragma once

#include <vector>

#include "row.h"

namespace tnt {
class TupleBuilder
{
	using field_content_t = Row::field_content_t;
public:
	TupleBuilder(std::size_t size);
	void push(const int64_t &value);
	void push(const unsigned &value);
	void push(const std::string &value);
	void push(const char *str, std::size_t length);
	void push(const bool &b);
	void pushNull();

	std::size_t size() const;
	const char *ptr() const;
private:
	char data[1024]; // TODO: grow dynamically
	char *p;
};
}
