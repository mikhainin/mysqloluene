#include "tnt_row.h"

#include <cstdint>
#include <string>
#include <iostream>

#include <msgpuck.h>

TntRow::TntRow(const char *_data, size_t sz):
	data(nullptr),
	size(sz),
	p(nullptr)
{
	data = new char[size];
	memcpy(data, _data, size);
	p = &data[0];

	assert(mp_typeof(*p) == MP_ARRAY);
	assert(mp_decode_array(&p) == 1);
	assert(mp_decode_array(&p) == 2);
}

TntRow::~TntRow()
{
	delete [] data;
	data = nullptr;
	size = 0;
}

int TntRow::getInt()
{
	assert(mp_typeof(*p) == MP_UINT);
	return mp_decode_uint(&p);
}

std::string TntRow::getString()
{
	// assert(mp_typeof(*p) == MP_STR);
	// u_int i = mp_decode_uint(&p); // WTF?
	assert(mp_typeof(*p) == MP_STR);

	uint32_t str_len = 0;
	const char * str = mp_decode_str(&p, &str_len);

	return std::string(str, str_len);
}
