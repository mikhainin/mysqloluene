#include "tuple_builder.h"

#include <msgpuck.h>

namespace tnt {

TupleBuilder::TupleBuilder(std::size_t size)
	: p(nullptr)
{
	memset(&data[0], 0, sizeof data);
	// p = &data[0];
	p = data;
	p = mp_encode_array(p, size);
}

void TupleBuilder::push(const int64_t &value)
{
	if (value < 0) {
		p = mp_encode_int(p, value);
	} else {
		p = mp_encode_uint(p, value);
	}
}

void TupleBuilder::push(const unsigned &value)
{
	p = mp_encode_uint(p, value);
}

void TupleBuilder::push(const std::string &value)
{
	push(value.c_str(), value.size());
}

void TupleBuilder::push(const char *str, std::size_t length)
{
	p = mp_encode_str(p, str, length);
}

void TupleBuilder::push(const bool &value)
{
	p = mp_encode_bool(p, value);
}

void TupleBuilder::pushNull()
{
	p = mp_encode_nil(p);
}

std::size_t TupleBuilder::size() const
{
	return p - &data[0];
}

const char *TupleBuilder::ptr() const
{
	return &data[0];
}

}
