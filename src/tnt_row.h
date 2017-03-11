#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

class TntRow
{
	struct field_content_t {
		char type;
		union {
			int64_t i;
			uint64_t u;
			struct {
				uint32_t len;
				const char *data;
			} str;
			bool b;
			float f;
			double d;
		};
	};

	TntRow(const TntRow &) = delete;
	TntRow& operator = (const TntRow &) = delete;
public:
	// TntRow(const char *data, size_t sz);
	// TntRow(const char *data, size_t sz);
	TntRow();
	~TntRow();

	static std::shared_ptr<TntRow> eatData(const char *(&p));

	int64_t getInt(int i) const;
	std::string getString(int i) const;
	bool getBool(int i) const;
	double getDouble(int i) const;
	int getFieldNum() const;

	bool isInt(int i) const;
	bool isNull(int i) const;
	bool isString(int i) const;
	bool isBool(int i) const;
	bool isFloatingPoint(int i) const;
private:
	// std::size_t size;
	std::vector<field_content_t> fields;
	// char *data;
//	/ const char *p;
	const char * eatDataInternal(const char *p);
};
