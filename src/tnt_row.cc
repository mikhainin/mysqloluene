#include "tnt_row.h"

#include <cstdint>
#include <string>
#include <iostream>

#include <msgpuck.h>

TntRow::TntRow()
{
}

TntRow::~TntRow()
{
}

std::shared_ptr<TntRow> TntRow::eatData(const char *(&p))
{
	std::shared_ptr<TntRow> row = std::make_shared<TntRow>();

	p = row->eatDataInternal(p);
	return row;
}

const char * TntRow::eatDataInternal(const char *p)
{
	assert(mp_typeof(*p) == MP_ARRAY);
	uint32_t elementsNumber = mp_decode_array(&p);
	assert(elementsNumber != -1);

	fields.reserve(elementsNumber);

	for(uint32_t i = 0; i < elementsNumber; ++i) {
		enum mp_type type = mp_typeof(*p);
		field_content_t f;
		f.type = type;
		switch(type) {
		case MP_UINT:
			f.u = mp_decode_uint(&p);
			break;
		case MP_INT:
			f.i = mp_decode_int(&p);
			break;
		case MP_BIN: // passthrough
		case MP_STR:
			f.str.data = mp_decode_str(&p, &f.str.len);
			break;
		case MP_BOOL:
			f.b = mp_decode_bool(&p);
			break;
		case MP_FLOAT:
			f.f = mp_decode_float(&p);
			break;
		case MP_DOUBLE:
			f.d = mp_decode_double(&p);
			break;
		case MP_NIL:
			mp_decode_nil(&p);
			break;
		case MP_ARRAY:
			throw std::runtime_error("Array is not supported");
		case MP_EXT:
			throw std::runtime_error("Have no idea what it is but MP_EXT is not supported either");
		default:
			throw std::runtime_error("Unknown unsupported MSGPACK type");
		}
		fields.push_back(f);
	}
	return p;
}

int64_t TntRow::getInt(int i) const
{
	assert(fields.size() > i);
	assert(fields[i].type == MP_UINT || fields[i].type == MP_INT);
	if (fields[i].type == MP_UINT) {
		return fields[i].u;
	} else if (fields[i].type == MP_INT) {
		return fields[i].i;
	} else {
		assert(false && "we should not have reached here!");
		return -1;
	}
}

std::string TntRow::getString(int i) const
{
	assert(fields.size() > i);
	assert(fields[i].type == MP_STR || fields[i].type == MP_BIN);

	return std::string(fields[i].str.data, fields[i].str.len);
}

bool TntRow::getBool(int i) const
{
	assert(fields.size() > i);
	assert(fields[i].type == MP_BOOL);
	return fields[i].b;
}

double TntRow::getDouble(int i) const
{
	assert(fields.size() > i);
	if (fields[i].type == MP_FLOAT) {
		return fields[i].f;
	} else if (fields[i].type == MP_DOUBLE) {
		return fields[i].d;
	} else {
		assert(false && "we should not have reached here!");
		return -1;
	}
}


int TntRow::getFieldNum() const
{
	return fields.size();
}

bool TntRow::isInt(int i) const
{
	return fields[i].type == MP_INT || fields[i].type == MP_UINT;
}

bool TntRow::isNull(int i) const
{
	return fields[i].type == MP_NIL;
}

bool TntRow::isString(int i) const
{
	return fields[i].type == MP_STR || fields[i].type == MP_BIN;
}

bool TntRow::isBool(int i) const
{
	return fields[i].type == MP_BOOL;
}

bool TntRow::isFloatingPoint(int i) const
{
	return fields[i].type == MP_FLOAT || fields[i].type == MP_DOUBLE;
}
